{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent       (myThreadId, throwTo)
import           Control.Concurrent.STM   (check, newTChanIO, orElse, readTBQueue, readTChan, readTVar, registerDelay,
                                           writeTChan)
import           Control.Lens
import           Control.Monad            (forever, void, when)
import qualified Control.Monad.Catch      as E
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.Logger     (MonadLogger (..), logDebugN, logInfoN, runStderrLoggingT)
import           Control.Monad.Trans      (lift)
import qualified Data.ByteString.Lazy     as BL
import           Data.Conduit             (runConduit, (.|))
import           Data.Conduit.Attoparsec  (conduitParser, sinkParser)
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network     (AppData, appSink, appSource, runTCPServer, serverSettings)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import qualified Data.UUID                as UUID
import           System.Random            (randomIO)
import           UnliftIO                 (MonadUnliftIO (..), async, waitAnyCancel)

import qualified Network.MQTT.Lens        as T
import qualified Network.MQTT.Types       as T

import           MQTTD
import           MQTTD.Util

dispatch :: (MonadLogger m, MonadFail m, MonadIO m) => Session -> T.MQTTPkt -> MQTTD m ()

dispatch Session{..} T.PingPkt = void $ sendPacketIO _sessionChan T.PongPkt

dispatch _ (T.PubACKPkt _) = pure ()

dispatch sess@Session{..} (T.SubscribePkt req@(T.SubscribeRequest pid subs props)) = do
  subscribe sess req
  void $ sendPacketIO _sessionChan (T.SubACKPkt (T.SubscribeResponse pid (map (const (Right T.QoS0)) subs) props))

dispatch sess@Session{..} (T.UnsubscribePkt (T.UnsubscribeRequest pid subs props)) = do
  uns <- unsubscribe sess subs
  void $ sendPacketIO _sessionChan (T.UnsubACKPkt (T.UnsubscribeResponse pid props uns))

dispatch sess@Session{..} (T.PublishPkt req) = do
  r@T.PublishRequest{..} <- resolveAliasIn sess req
  satisfyQoS _pubQoS r
  broadcast (Just _sessionID) r
    where
      satisfyQoS T.QoS0 _ = pure ()
      satisfyQoS T.QoS1 T.PublishRequest{..} =
        void $ sendPacketIO _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0 _pubProps))
      satisfyQoS T.QoS2 T.PublishRequest{..} =
        void $ sendPacketIO _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0x80 _pubProps))

-- TODO:  QoS 2 (and maybe even 1?)

dispatch sess (T.DisconnectPkt (T.DisconnectRequest T.DiscoNormalDisconnection _props)) = do
  let Just sid = sess ^? sessionClient . _Just . clientConnReq . T.connID
  modifySession sid (Just . set sessionWill Nothing)

-- TODO: other disconnection types.

dispatch _ x = fail ("unhandled: " <> show x)

handleConnection :: forall m. (MonadLogger m, MonadUnliftIO m, MonadFail m, E.MonadMask m, E.MonadThrow m) => AppData -> MQTTD m ()
handleConnection ad = runConduit $ do
  (cpkt@(T.ConnPkt _ pl), genedID) <- ensureID =<< appSource ad .| sinkParser T.parseConnect
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

    runIn wdch sess pl = runConduit $ appSource ad
        .| conduitParser (T.parsePacket pl)
        .| C.mapM (\i@(_,x) -> logDebugN ("<< " <> tshow x) >> pure i)
        .| C.mapM_ (\(_,x) -> feed wdch >> dispatch sess x)

    processOut pl ch = runConduit $
      C.repeatM (liftSTM $ readTBQueue ch)
      .| C.mapM (\x -> logDebugN (">> " <> tshow x) >> pure x)
      .| C.map (BL.toStrict . T.toByteString pl)
      .| appSink ad

    teardown :: ClientID -> T.ConnectRequest -> MQTTD m ()
    teardown cid c@T.ConnectRequest{..} = do
      logDebugN ("Tearing down ... " <> tshow c)
      unregisterClient _connID cid

    ensureID (T.ConnPkt c@T.ConnectRequest{_connID=""} pl) = do
      nid <- BL.fromStrict . UUID.toASCIIBytes <$> liftIO randomIO
      logDebugN ("Generating ID for anonymous client: " <> tshow nid)
      pure (T.ConnPkt c{T._connID=nid} pl, Just nid)
    ensureID x = pure (x, Nothing)

    feed wdch = liftSTM (writeTChan wdch True)

    watchdog pp wdch sid t = forever $ do
      toch <- liftIO $ registerDelay pp
      timedOut <- liftSTM $ ((check =<< readTVar toch) >> pure True) `orElse` (readTChan wdch >> pure False)
      when timedOut $ do
        logInfoN ("Client with session " <> tshow sid <> " timed out")
        liftIO $ throwTo t MQTTPingTimeout

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
    -- TODO:  websockets

    void $ waitAnyCancel [sc, pc, serv, sserv]
