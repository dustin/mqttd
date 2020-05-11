{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent       (myThreadId, throwTo)
import           Control.Concurrent.STM   (check, modifyTVar', newTChanIO, orElse, readTBQueue, readTChan, readTVar,
                                           registerDelay, writeTChan, writeTVar)
import           Control.Lens
import           Control.Monad            (forever, unless, void, when)
import qualified Control.Monad.Catch      as E
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.Logger     (logDebugN, logInfoN, runStderrLoggingT)
import           Control.Monad.Trans      (lift)
import qualified Data.ByteString.Char8    as BCS
import qualified Data.ByteString.Lazy     as BL
import           Data.Conduit             (ConduitT, Void, await, runConduit, yield, (.|))
import           Data.Conduit.Attoparsec  (conduitParser, sinkParser)
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network     (AppData, appSink, appSource, runTCPServer, serverSettings)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import qualified Data.Map.Strict          as Map
import qualified Data.UUID                as UUID
import qualified Network.WebSockets       as WS
import           System.Random            (randomIO)
import           UnliftIO                 (MonadUnliftIO (..), async, atomically, waitAnyCancel)

import qualified Network.MQTT.Lens        as T
import qualified Network.MQTT.Types       as T

import           MQTTD
import           MQTTD.Util

dispatch :: PublishConstraint m => Session -> T.MQTTPkt -> MQTTD m ()

dispatch Session{..} T.PingPkt = void $ sendPacketIO _sessionChan T.PongPkt

-- QoS 1 ACK (receiving client got our publish message)
dispatch sess pkt@(T.PubACKPkt ack) = gotResponse sess (ack ^. T.pktID) pkt

-- QoS 2 ACK (receiving client received our message)
dispatch sess pkt@(T.PubRECPkt ack) = gotResponse sess (ack ^. T.pktID) pkt

-- QoS 2 REL (publishing client says we can ship the message)
dispatch Session{..} (T.PubRELPkt rel) = do
  pkt <- atomically $ do
    (r, m) <- Map.updateLookupWithKey (const.const $ Nothing) (rel ^. T.pktID) <$> readTVar _sessionQP
    writeTVar _sessionQP m
    _ <- sendPacket _sessionChan (T.PubCOMPPkt (T.PubCOMP (rel ^. T.pktID) (maybe 0x92 (const 0) r) mempty))
    pure r
  justM (broadcast (Just _sessionID)) pkt

-- QoS 2 COMPlete (publishing client says publish is complete)
dispatch sess pkt@(T.PubCOMPPkt ack) = gotResponse sess (ack ^. T.pktID) pkt

dispatch sess@Session{..} (T.SubscribePkt req@(T.SubscribeRequest pid subs props)) = do
  subscribe sess req
  void $ sendPacketIO _sessionChan (T.SubACKPkt (T.SubscribeResponse pid (map (const (Right T.QoS0)) subs) props))

dispatch sess@Session{..} (T.UnsubscribePkt (T.UnsubscribeRequest pid subs props)) = do
  uns <- unsubscribe sess subs
  void $ sendPacketIO _sessionChan (T.UnsubACKPkt (T.UnsubscribeResponse pid props uns))

dispatch sess@Session{..} (T.PublishPkt req) = do
  r@T.PublishRequest{..} <- resolveAliasIn sess req
  satisfyQoS _pubQoS r
    where
      satisfyQoS T.QoS0 r = broadcast (Just _sessionID) r
      satisfyQoS T.QoS1 r@T.PublishRequest{..} = do
        sendPacketIO_ _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0 mempty))
        broadcast (Just _sessionID) r
      satisfyQoS T.QoS2 r@T.PublishRequest{..} = atomically $ do
        void $ sendPacket _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0 mempty))
        modifyTVar' _sessionQP (Map.insert _pubPktID r)

dispatch sess (T.DisconnectPkt (T.DisconnectRequest T.DiscoNormalDisconnection _props)) = do
  let Just sid = sess ^? sessionClient . _Just . clientConnReq . T.connID
  modifySession sid (Just . set sessionWill Nothing)

-- TODO: other disconnection types.

dispatch _ x = fail ("unhandled: " <> show x)

type MQTTConduit m = (ConduitT () BCS.ByteString (MQTTD m) (), ConduitT BCS.ByteString Void (MQTTD m) ())

handleConnection :: PublishConstraint m => AppData -> MQTTD m ()
handleConnection ad = runMQTTDConduit (appSource ad, appSink ad)

runMQTTDConduit :: forall m. PublishConstraint m => MQTTConduit m -> MQTTD m ()
runMQTTDConduit (src,sink) = runConduit $ do
  (cpkt@(T.ConnPkt _ pl), genedID) <- ensureID =<< src .| sinkParser T.parseConnect
  cid <- lift nextID

  lift $ run pl cid cpkt genedID

  where
    run :: T.ProtocolLevel -> ClientID -> T.MQTTPkt -> Maybe BL.ByteString -> MQTTD m ()
    run pl cid (T.ConnPkt req@T.ConnectRequest{..} _) nid = do
      logInfoN ("A connection is made " <> tshow req)
      -- Register and accept the connection
      tid <- liftIO myThreadId
      (sess@Session{_sessionID, _sessionChan}, existing) <- registerClient req cid tid
      let cprops = [ T.PropTopicAliasMaximum 100 ] <> [ T.PropAssignedClientIdentifier i | Just i <- [nid] ]
      sendPacketIO _sessionChan (T.ConnACKPkt $ T.ConnACKFlags existing T.ConnAccepted cprops)

      wdch <- liftIO newTChanIO
      w <- async $ watchdog (3 * seconds (fromIntegral _keepAlive)) wdch _sessionID tid
      o <- async $ processOut pl _sessionChan
      i <- async $ E.finally (runIn wdch sess pl) (teardown cid req)
      void $ waitAnyCancel [i, o, w]

    runIn wdch sess pl = runConduit $ src
        .| conduitParser (T.parsePacket pl)
        .| C.mapM (\i@(_,x) -> logDebugN ("<< " <> tshow x) >> pure i)
        .| C.mapM_ (\(_,x) -> feed wdch >> dispatch sess x)

    processOut pl ch = runConduit $
      C.repeatM (atomically $ readTBQueue ch)
      .| C.mapM (\x -> logDebugN (">> " <> tshow x) >> pure x)
      .| C.map (BL.toStrict . T.toByteString pl)
      .| sink

    teardown :: ClientID -> T.ConnectRequest -> MQTTD m ()
    teardown cid c@T.ConnectRequest{..} = do
      logDebugN ("Tearing down ... " <> tshow c)
      unregisterClient _connID cid

    ensureID (T.ConnPkt c@T.ConnectRequest{_connID=""} pl) = do
      nid <- BL.fromStrict . UUID.toASCIIBytes <$> liftIO randomIO
      logDebugN ("Generating ID for anonymous client: " <> tshow nid)
      pure (T.ConnPkt c{T._connID=nid} pl, Just nid)
    ensureID x = pure (x, Nothing)

    feed wdch = atomically (writeTChan wdch True)

    watchdog pp wdch sid t = forever $ do
      toch <- liftIO $ registerDelay pp
      timedOut <- atomically $ ((check =<< readTVar toch) >> pure True) `orElse` (readTChan wdch >> pure False)
      when timedOut $ do
        logInfoN ("Client with session " <> tshow sid <> " timed out")
        liftIO $ throwTo t MQTTPingTimeout

handleWS :: PublishConstraint m => WS.PendingConnection -> MQTTD m ()
handleWS pc = do
  conn <- liftIO $ WS.acceptRequest pc
  runMQTTDConduit (wsSource conn, wsSink conn)

  where
    wsSource ws = forever $ do
      bs <- liftIO $ WS.receiveData ws
      unless (BCS.null bs) $ yield bs

    wsSink ws = justM (\bs -> liftIO (WS.sendBinaryData ws bs) >> wsSink ws) =<< await

main :: IO ()
main = do
  e <- newEnv
  runStderrLoggingT . runIO e $ do
    sc <- async sessionCleanup
    pc <- async persistenceCleanup
    -- Plaintext server
    serv <- async (withRunInIO $ \unl -> runTCPServer (serverSettings 1883 "*") (unl . handleConnection))
    -- TLS Server
    let cfile = "certificate.pem"
        kfile = "key.pem"
    sserv <- async (withRunInIO $ \unl -> runGeneralTCPServerTLS (tlsConfig "*" 8883 cfile kfile) (unl . handleConnection))
    -- Websockets server
    ws <- async (withRunInIO $ \unl -> WS.runServer "0.0.0.0" 8080 (unl . handleWS))

    void $ waitAnyCancel [sc, pc, serv, sserv, ws]
