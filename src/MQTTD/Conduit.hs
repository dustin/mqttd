{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module MQTTD.Conduit where

import           Cleff
import           Cleff.Fail
import           Control.Concurrent       (myThreadId, throwTo)
import           Control.Concurrent.STM   (check, modifyTVar', newTChanIO, orElse, readTBQueue, readTChan, readTVar,
                                           registerDelay, writeTBQueue, writeTChan)
import           Control.Lens
import           Control.Monad            (forever, guard, unless, void, when)
import qualified Control.Monad.Catch      as E
import           Control.Monad.Trans      (lift)
import qualified Data.ByteString.Char8    as BCS
import qualified Data.ByteString.Lazy     as BL
import           Data.Conduit             (ConduitT, Void, await, runConduit, yield, (.|))
import           Data.Conduit.Attoparsec  (conduitParser, sinkParser)
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network     (AppData, appSink, appSockAddr, appSource)
import qualified Data.Map.Strict          as Map
import           Data.String              (IsString (..))
import           Data.Text                (Text, intercalate, pack, unpack)
import           Data.Time.Clock          (getCurrentTime)
import qualified Data.UUID                as UUID
import qualified Network.MQTT.Lens        as T
import qualified Network.MQTT.Types       as T
import qualified Network.WebSockets       as WS
import           System.Random            (randomIO)
import           UnliftIO                 (async, atomically, waitAnyCancel)

import           MQTTD
import           MQTTD.Authorizer
import           MQTTD.DB                 (DB (..))
import           MQTTD.Logging
import           MQTTD.ScheduleFX
import           MQTTD.Stats
import           MQTTD.Types
import           MQTTD.Util

type MQTTConduit es = (ConduitT () BCS.ByteString (Eff es) (), ConduitT BCS.ByteString Void (Eff es) (), Text)

runMQTTDConduit :: forall es. [IOE, ScheduleFX SessionID, AuthFX, Fail, LogFX, DB, Stats, MQTTD] :>> es => MQTTConduit es -> Eff es ()
runMQTTDConduit (src, sink, addr) = runConduit $ do
  (cpkt@(T.ConnPkt _ pl), genedID) <- lift . ensureID =<< commonIn .| sinkParser T.parseConnect
  cid <- lift nextID

  lift $ run pl cid cpkt genedID

  where
    count s x = incrementStat s (fromIntegral $ BCS.length x) >> pure x

    run :: [IOE, ScheduleFX SessionID, AuthFX, Fail, DB, Stats, MQTTD] :>> es => T.ProtocolLevel -> ClientID -> T.MQTTPkt -> Maybe SessionID -> Eff es ()
    run pl cid (T.ConnPkt req _) nid = do
      r <- authorize req
      case r of
        Left x  -> notAuthorized pl req x
        Right _ -> authorized pl cid req nid

    run _ _ pkt _ = fail ("Unhandled start packet from " <> unpack addr <> ": " <> show pkt)

    notAuthorized pl req s = do
      logInfoL ["Unauthorized connection from ", addr, ": ", tshow req]
      runConduit $ do
        yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession (noauth pl) []) .| commonOut pl
        yield (T.DisconnectPkt $ T.DisconnectRequest T.DiscoNotAuthorized [
                  T.PropReasonString (fromString s)
                  ]) .| commonOut pl

          where noauth T.Protocol311 = T.NotAuthorized
                noauth T.Protocol50  = T.ConnNotAuthorized

    authorized pl cid req@T.ConnectRequest{..} nid = do
      logConn "connection" req
      -- Register and accept the connection
      tid <- liftIO myThreadId
      (sess@Session{_sessionID, _sessionChan}, existing) <- registerClient req cid tid
      let ka = \case
            0 -> 1800
            x -> fromIntegral x
      let cprops = [ T.PropTopicAliasMaximum 100 ] <> [ T.PropAssignedClientIdentifier i | Just i <- [nid] ]
                   <> [ T.PropServerKeepAlive (fromIntegral (ka _keepAlive)) | _keepAlive == 0]
      deliverConnACK pl existing cprops

      wdch <- liftIO newTChanIO
      w <- async $ watchdog (3 * seconds (ka _keepAlive)) wdch _sessionID tid
      o <- async $ processOut pl _sessionChan
      i <- async $ E.finally (processIn wdch sess pl `E.onException` logConn "process exception" req) (teardown cid req)
      retransmit sess
      void $ waitAnyCancel [i, o, w]

    logConn h T.ConnectRequest{..} =
      logInfoL [h,  " from ",  addr,  lu,
                " s=",  tshow _connID,
                lw _lastWill,
                " c=",  tf _cleanSession,
                " ka=",  tshow _keepAlive,
                sp _connProperties]
      where
        tf True  = "t"
        tf False = "f"
        sp [] = ""
        sp xs = " p=[" <> intercalate " " (map (pack . drop 4 . show) xs) <> "]"
        lu = maybe "" (tshow . (" u=" <>)) _username
        lw Nothing = ""
        lw (Just T.LastWill{..}) = mconcat [
          " w={t=", tshow _willTopic,
          ", r=", tf _willRetain,
          ", q=", tshow (fromEnum _willQoS),
          sp _willProps,
          "}"
          ]

    processIn wdch sess pl = runConduit $ commonIn
        .| conduitParser (T.parsePacket pl)
        .| C.mapM (\i@(_,x) -> logDbgL ["<< ", tshow x] >> pure i)
        .| C.mapM_ (\(_,x) -> feed wdch >> dispatch sess x)


    commonIn = src .| C.mapM (count StatBytesRcvd)

    commonOut pl = C.mapM (\x -> logDbgL [">> ", tshow x] >> pure x)
                   .| C.map (BL.toStrict . T.toByteString pl)
                   .| C.mapM (count StatBytesSent)
                   .| sink

    deliverConnACK pl existing cprops = runConduit $
      yield (T.ConnACKPkt $ T.ConnACKFlags existing T.ConnAccepted cprops) .| commonOut pl

    processOut pl ch = runConduit $
      C.repeatM (atomically $ readTBQueue ch) .| commonOut pl

    teardown :: [IOE, MQTTD] :>> es => ClientID -> T.ConnectRequest -> Eff es ()
    teardown cid c@T.ConnectRequest{..} = do
      logConn "disconnection" c
      unregisterClient _connID cid

    ensureID (T.ConnPkt c@T.ConnectRequest{_connID=""} pl) = do
      nid <- BL.fromStrict . UUID.toASCIIBytes <$> liftIO randomIO
      logDbgL ["Generating ID for anonymous client: ", tshow nid]
      pure (T.ConnPkt c{T._connID=nid} pl, Just nid)
    ensureID x = pure (x, Nothing)

    feed wdch = atomically (writeTChan wdch True)

    watchdog pp wdch sid t = forever $ do
      toch <- liftIO $ registerDelay pp
      timedOut <- atomically $ ((check =<< readTVar toch) >> pure True) `orElse` (readTChan wdch >> pure False)
      when timedOut $ do
        logInfoL ["Client with session ", tshow sid, " timed out"]
        liftIO $ throwTo t MQTTPingTimeout

    retransmit Session{..} = do
      -- We can atomically find the partially transmitted messages and
      -- drop them into an outbound queue that does not exceed the
      -- maximum amount allowable by either the client or our own
      -- queue size.  The rest is returned for async processing.
      now <- liftIO getCurrentTime
      bl <- atomically do
        tokens <- readTVar _sessionFlight
        modifyTVar' _sessionQP (Map.filter (stillValid now))
        (t, bl) <- splitAt (min defaultQueueSize (fromIntegral tokens)) . Map.elems <$> readTVar _sessionQP
        modifyTVar' _sessionFlight (subtract . fromIntegral . length $ t)
        mapM_ (sendPacket _sessionChan . T.PublishPkt . set T.pubDup True . uncurry (adjExpiry now)) t
        pure bl
      -- The backlog is processed as space frees up in its queue.
      allSessions <- asks sessions
      mapM_ (atomically . art now allSessions) bl
        where
          art now allSessions (_, p) = do
              guard =<< isClientConnected _sessionID allSessions
              writeTBQueue _sessionBacklog (deadline now p, p{T._pubDup=True})

          stillValid now (Just t, _) = now < t
          stillValid _ _             = True

          adjExpiry _ Nothing p    = p
          adjExpiry now (Just t) p = p & T.properties . traversed . T._PropMessageExpiryInterval .~ relExp now t

webSocketsApp :: [IOE, ScheduleFX SessionID, AuthFX, Fail, MQTTD, Stats, LogFX, DB] :>> es => WS.PendingConnection -> Eff es ()
webSocketsApp pc = do
  conn <- liftIO $ WS.acceptRequest pc
  runMQTTDConduit (wsSource conn, wsSink conn, "<unknown>")

  where
    wsSource ws = forever $ do
      bs <- liftIO $ WS.receiveData ws
      unless (BCS.null bs) $ yield bs

    wsSink ws = justM (\bs -> liftIO (WS.sendBinaryData ws bs) >> wsSink ws) =<< await

tcpApp :: [IOE, Fail, ScheduleFX SessionID, AuthFX, MQTTD, DB, Stats, LogFX] :>> es => AppData -> Eff es ()
tcpApp ad = runMQTTDConduit (appSource ad, appSink ad, tshow (appSockAddr ad))
