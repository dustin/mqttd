{-# LANGUAGE FlexibleContexts    #-}
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
import           Control.Monad.Reader     (asks)
import           Control.Monad.Trans      (lift)
import qualified Data.ByteString.Char8    as BCS
import qualified Data.ByteString.Lazy     as BL
import           Data.Conduit             (ConduitT, Void, await, runConduit, yield, (.|))
import           Data.Conduit.Attoparsec  (conduitParser, sinkParser)
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network     (AppData, appSink, appSource)
import qualified Data.Map.Strict          as Map
import           Data.String              (IsString (..))
import qualified Data.UUID                as UUID
import qualified Network.MQTT.Types       as T
import qualified Network.WebSockets       as WS
import           System.Random            (randomIO)
import           UnliftIO                 (async, atomically, waitAnyCancel)

import           MQTTD
import           MQTTD.Config
import           MQTTD.Stats
import           MQTTD.Types
import           MQTTD.Util

type MQTTConduit m = (ConduitT () BCS.ByteString (MQTTD m) (), ConduitT BCS.ByteString Void (MQTTD m) ())

authorize :: (MonadFail m, Monad m) => T.ConnectRequest -> MQTTD m (Either String ())
authorize T.ConnectRequest{..} = do
  Authorizer{..} <- asks authorizer
  pure . unless _authAnon $ do
    uname <- maybe (Left "anonymous clients are not allowed") Right _username
    (User _ want _) <- maybe (Left "invalid username or password") Right (Map.lookup uname _authUsers)
    pass <- maybe (Right "") Right _password
    when (pass /= want) $ Left "invalid username or password"

runMQTTDConduit :: forall m. PublishConstraint m => MQTTConduit m -> MQTTD m ()
runMQTTDConduit (src, sink) = runConduit $ do
  (cpkt@(T.ConnPkt _ pl), genedID) <- ensureID =<< commonIn .| sinkParser T.parseConnect
  cid <- lift nextID

  lift $ run pl cid cpkt genedID

  where
    count s x = asks statStore >>= incrementStat s (fromIntegral $ BCS.length x) >> pure x

    run :: T.ProtocolLevel -> ClientID -> T.MQTTPkt -> Maybe SessionID -> MQTTD m ()
    run pl cid (T.ConnPkt req@T.ConnectRequest{..} _) nid = do
      r <- authorize req
      case r of
        Left x  -> notAuthorized pl req x
        Right _ -> authorized pl cid req nid

    run _ _ pkt _ = fail ("Unhandled start packet: " <> show pkt)

    notAuthorized pl req s = do
      logInfoN ("Unauthorized connection: " <> tshow req)
      runConduit $ do
        yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession (noauth pl) []) .| commonOut pl
        yield (T.DisconnectPkt $ T.DisconnectRequest T.DiscoNotAuthorized [
                  T.PropReasonString (fromString s)
                  ]) .| commonOut pl

          where noauth T.Protocol311 = T.NotAuthorized
                noauth T.Protocol50 = T.ConnNotAuthorized

    authorized pl cid req@T.ConnectRequest{..} nid = do
      logInfoN ("A connection is made " <> tshow req)
      -- Register and accept the connection
      tid <- liftIO myThreadId
      (sess@Session{_sessionID, _sessionChan}, existing) <- registerClient req cid tid
      let cprops = [ T.PropTopicAliasMaximum 100 ] <> [ T.PropAssignedClientIdentifier i | Just i <- [nid] ]
      deliverConnACK pl existing cprops

      wdch <- liftIO newTChanIO
      w <- async $ watchdog (3 * seconds (fromIntegral _keepAlive)) wdch _sessionID tid
      o <- async $ processOut pl _sessionChan
      i <- async $ E.finally (processIn wdch sess pl) (teardown cid req)
      retransmit sess
      void $ waitAnyCancel [i, o, w]

    processIn wdch sess pl = runConduit $ commonIn
        .| conduitParser (T.parsePacket pl)
        .| C.mapM (\i@(_,x) -> logDebugN ("<< " <> tshow x) >> pure i)
        .| C.mapM_ (\(_,x) -> feed wdch >> dispatch sess x)


    commonIn = src .| C.mapM (count StatBytesRcvd)

    commonOut pl = C.mapM (\x -> logDebugN (">> " <> tshow x) >> pure x)
                   .| C.map (BL.toStrict . T.toByteString pl)
                   .| C.mapM (count StatBytesSent)
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

    retransmit Session{..} = atomically $ mapM_ rt . Map.elems =<< readTVar _sessionQP
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
