{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module MQTTD.Conduit where

import           Control.Concurrent       (myThreadId, throwTo)
import           Control.Concurrent.STM   (check, newTChanIO, orElse, readTBQueue, readTChan, readTVar, registerDelay,
                                           writeTChan)
import           Control.Monad            (forever, unless, void, when)
import qualified Control.Monad.Catch      as E
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.Logger     (logDebugN, logInfoN)
import           Control.Monad.Trans      (lift)
import qualified Data.ByteString.Char8    as BCS
import qualified Data.ByteString.Lazy     as BL
import           Data.Conduit             (ConduitT, Void, await, runConduit, yield, (.|))
import           Data.Conduit.Attoparsec  (conduitParser, sinkParser)
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network     (AppData, appSink, appSource)
import qualified Data.Map.Strict          as Map
import qualified Data.UUID                as UUID
import qualified Network.MQTT.Types       as T
import qualified Network.WebSockets       as WS
import           System.Random            (randomIO)
import           UnliftIO                 (async, atomically, waitAnyCancel)

import           MQTTD
import           MQTTD.Util

type MQTTConduit m = (ConduitT () BCS.ByteString (MQTTD m) (), ConduitT BCS.ByteString Void (MQTTD m) ())

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
      deliverConnACK pl existing cprops
      retransmit sess

      wdch <- liftIO newTChanIO
      w <- async $ watchdog (3 * seconds (fromIntegral _keepAlive)) wdch _sessionID tid
      o <- async $ processOut pl _sessionChan
      i <- async $ E.finally (runIn wdch sess pl) (teardown cid req)
      void $ waitAnyCancel [i, o, w]

    run _ _ pkt _ = fail ("Unhandled start packet: " <> show pkt)

    runIn wdch sess pl = runConduit $ src
        .| conduitParser (T.parsePacket pl)
        .| C.mapM (\i@(_,x) -> logDebugN ("<< " <> tshow x) >> pure i)
        .| C.mapM_ (\(_,x) -> feed wdch >> dispatch sess x)


    commonOut pl = C.mapM (\x -> logDebugN (">> " <> tshow x) >> pure x)
                   .| C.map (BL.toStrict . T.toByteString pl)
                   .| sink

    deliverConnACK pl existing cprops = runConduit $
      yield (T.ConnACKPkt $ T.ConnACKFlags existing T.ConnAccepted cprops) .| commonOut pl

    processOut pl ch = runConduit $
      C.repeatM (atomically $ readTBQueue ch) .| commonOut pl

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

    retransmit Session{..} = atomically $ do
      mapM_ rt . Map.elems =<< readTVar _sessionQP
        where rt p = sendPacket _sessionChan (T.PublishPkt p{T._pubDup=True})

webSocketsApp :: PublishConstraint m => WS.PendingConnection -> MQTTD m ()
webSocketsApp pc = do
  conn <- liftIO $ WS.acceptRequest pc
  runMQTTDConduit (wsSource conn, wsSink conn)

  where
    wsSource ws = forever $ do
      bs <- liftIO $ WS.receiveData ws
      unless (BCS.null bs) $ yield bs

    wsSink ws = justM (\bs -> liftIO (WS.sendBinaryData ws bs) >> wsSink ws) =<< await

tcpApp :: PublishConstraint m => AppData -> MQTTD m ()
tcpApp ad = runMQTTDConduit (appSource ad, appSink ad)
