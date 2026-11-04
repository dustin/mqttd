{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module MQTTD.Conduit where

import           Control.Applicative      ((<|>))
import           Control.Concurrent       (myThreadId, throwTo)
import           Control.Concurrent.STM   (check, flushTBQueue, modifyTVar', newTChanIO, orElse, readTBQueue, readTChan, readTVar,
                                           registerDelay, writeTBQueue, writeTChan)
import           Control.Lens
import           Control.Monad            (forever, guard, unless, void, when)
import qualified Control.Monad.Catch      as E
import           Control.Monad.Trans      (lift)
import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString.Char8    as BCS
import qualified Data.ByteString.Lazy     as BL
import           Data.Conduit             (ConduitT, Void, await, runConduit, yield, (.|))
import           Data.Conduit.Attoparsec  (Position (..), PositionRange (..), conduitParser, sinkParser)
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network     (AppData, appSink, appSockAddr, appSource)
import           Data.Functor             (($>))
import qualified Data.Map.Strict          as Map
import           Data.Maybe               (isNothing, maybeToList)
import qualified Data.Set                 as Set
import           Data.String              (IsString (..))
import           Data.Text                (Text, intercalate, pack, unpack)
import           Data.Time.Clock          (getCurrentTime)
import           Data.Word                (Word8)
import qualified Data.UUID                as UUID
import           Effectful
import           Effectful.Fail
import qualified Network.MQTT.Lens        as T
import           Network.MQTT.Topic       (mkTopic)
import qualified Network.MQTT.Types       as T
import qualified Network.WebSockets       as WS
import           System.Random            (randomIO)
import           UnliftIO                 (async, atomically, waitAnyCancel)

import           MQTTD
import           MQTTD.Authorizer
import           MQTTD.Config             (Limits (..), Redirect (..))
import           MQTTD.DB                 (DB (..))
import           MQTTD.Logging
import           MQTTD.ScheduleFX
import           MQTTD.Stats
import           MQTTD.Types
import           MQTTD.Util

-- | The protocol level of a CONNECT for a version we don't support
-- (e.g. 3 for MQTT 3.1).
unsupportedLevel :: A.Parser Word8
unsupportedLevel = do
  _ <- A.word8 0x10
  _ <- T.parseHdrLen
  n <- (\hi lo -> fromIntegral hi * 256 + fromIntegral lo) <$> A.anyWord8 <*> A.anyWord8
  name <- A.take n
  guard (name `elem` ["MQTT", "MQIsdp"])
  lvl <- A.anyWord8
  guard (lvl `notElem` [4, 5])
  pure lvl

type MQTTConduit es = (ConduitT () BCS.ByteString (Eff es) (), ConduitT BCS.ByteString Void (Eff es) (), Text)

runMQTTDConduit :: forall es. (IOE :> es, ScheduleFX SessionID :> es, AuthFX :> es, Fail :> es, LogFX :> es, DB :> es, Stats :> es, MQTTD :> es) => MQTTConduit es -> Eff es ()
runMQTTDConduit (src, sink, addr) = runConduit $ do
  start <- commonIn .| sinkParser ((Right <$> T.parseConnect) <|> (Left <$> unsupportedLevel))
  case start of
    Left lvl -> lift $ refuseLevel lvl
    Right connPkt -> do
      (cpkt@(T.ConnPkt _ pl), genedID) <- lift (ensureID connPkt)
      cid <- lift nextID
      lift $ run pl cid cpkt genedID

  where
    -- Say why, in a CONNACK the client can read, then close
    -- [3.1.1 MQTT-3.1.2-2], [5.0 §3.1.2.2].
    refuseLevel lvl = do
      logInfoL ["Unsupported protocol level ", tshow lvl, " from ", addr]
      let (pl, rc) = if lvl < 4 then (T.Protocol311, T.UnacceptableProtocol)
                     else (T.Protocol50, T.ConnUnsupportedProtocolVersion)
      runConduit $ yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession rc []) .| commonOut pl

    count s x = incrementStat s (fromIntegral $ BCS.length x) >> pure x

    run :: (IOE :> es, ScheduleFX SessionID :> es, AuthFX :> es, Fail :> es, DB :> es, Stats :> es, MQTTD :> es) => T.ProtocolLevel -> ClientID -> T.MQTTPkt -> Maybe SessionID -> Eff es ()
    -- With a redirect configured, every client is sent elsewhere: a 5.0
    -- client with a Server Reference [5.0 §4.11], a 3.1.1 one (which can't
    -- be redirected) as server unavailable.  Returning closes the connection.
    run pl cid pkt nid = asks redirect >>= \case
      Nothing -> run' pl cid pkt nid
      Just r -> do
        logInfoL ["Redirecting a connection from ", addr, " (", tshow r, ")"]
        let (rc, ref) = case (pl, r) of
              (T.Protocol50, UseAnotherServer s) -> (T.ConnUseAnotherServer, [T.PropServerReference s])
              (T.Protocol50, ServerMoved s)      -> (T.ConnServerMoved, [T.PropServerReference s])
              _                                  -> (T.ServerUnavailable, [])
        runConduit $ yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession rc ref) .| commonOut pl

    run' :: (IOE :> es, ScheduleFX SessionID :> es, AuthFX :> es, Fail :> es, DB :> es, Stats :> es, MQTTD :> es) => T.ProtocolLevel -> ClientID -> T.MQTTPkt -> Maybe SessionID -> Eff es ()
    -- In 3.1.1, an empty client ID (one we generated) requires a clean
    -- session [MQTT-3.1.3-8].  Returning closes the connection.
    run' T.Protocol311 _ (T.ConnPkt T.ConnectRequest{T._cleanSession=False} _) (Just _) = do
      logInfoL ["Rejecting empty client ID without a clean session from ", addr]
      runConduit $ yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession T.IdentifierRejected []) .| commonOut T.Protocol311

    -- No authentication methods are supported [5.0 §4.12], [MQTT-4.12.0-1].
    run' T.Protocol50 _ (T.ConnPkt T.ConnectRequest{T._connProperties=props} _) _
      | any (has T._PropAuthenticationMethod) props = do
          logInfoL ["Rejecting unsupported authentication method from ", addr]
          runConduit $ yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession T.ConnBadAuthenticationMethod []) .| commonOut T.Protocol50

    -- A will topic is a topic name: not empty, and no wildcards
    -- [MQTT-4.7.3-1], [MQTT-3.1.3-11].  3.1.1 has no CONNACK code for
    -- it, so the connection is just closed.  (5.0's 0x90 is for a will
    -- topic that's valid but not accepted.)
    run' pl _ (T.ConnPkt T.ConnectRequest{T._lastWill=Just T.LastWill{T._willTopic}} _) _
      | isNothing (mkTopic (blToText _willTopic)) = do
          logInfoL ["Rejecting invalid will topic ", tshow _willTopic, " from ", addr]
          when (pl == T.Protocol50) $
            runConduit $ yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession T.ConnProtocolError []) .| commonOut pl

    run' pl cid (T.ConnPkt req _) nid = either (notAuthorized pl req) (const $ authorized pl cid req nid) =<< auth
      where
        -- The will is published as the client, so it needs permission
        -- to publish to its topic.
        auth = authorize req >>= either (pure . Left) (const $ willAuth <$> getACL (T._username req))
        willAuth acl = maybe (Right ()) (authWill acl) (T._lastWill req)

    run' _ _ pkt _ = fail ("Unhandled start packet from " <> unpack addr <> ": " <> show pkt)

    notAuthorized pl req s = do
      logInfoL ["Unauthorized connection from ", addr, ": ", tshow req]
      -- Just the CONNACK; returning closes the connection.  There's no
      -- DISCONNECT after a refusing CONNACK [3.1.1 MQTT-3.2.2-5],
      -- [5.0 MQTT-3.2.2-7, MQTT-3.14.0-1].
      runConduit $
        yield (T.ConnACKPkt $ T.ConnACKFlags T.NewSession (noauth pl) [
                  T.PropReasonString (fromString s) | pl == T.Protocol50
                  ]) .| commonOut pl

          where noauth T.Protocol311 = T.NotAuthorized
                noauth T.Protocol50  = T.ConnNotAuthorized

    authorized pl cid req@T.ConnectRequest{..} nid = do
      logConn "connection" req
      -- Register and accept the connection
      tid <- liftIO myThreadId
      (sess@Session{_sessionID, _sessionChan}, existing) <- registerClient pl req cid tid
      lims <- asks limits
      rprefix <- asks responsePrefix
      let cprops = [ T.PropTopicAliasMaximum topicAliasMaximum ] <> [ T.PropAssignedClientIdentifier i | Just i <- [nid] ]
                   -- Not supported [5.0 §3.2.2.3.12].
                   <> [ T.PropSubscriptionIdentifierAvailable 0 | pl == T.Protocol50 ]
                   <> [ T.PropReceiveMaximum receiveMaximum | pl == T.Protocol50 ]
                   <> [ T.PropMaximumPacketSize n | Just n <- [_limitPacketSize lims], pl == T.Protocol50 ]
                   -- Only for a client that asked [MQTT-3.1.2-28].
                   <> [ T.PropResponseInformation (p <> _connID <> "/")
                      | Just p <- [rprefix], pl == T.Protocol50,
                        T.PropRequestResponseInformation 1 `elem` _connProperties ]
                   <> [ T.PropServerKeepAlive ka | let ka = effectiveKeepAlive (_limitKeepAlive lims) pl _keepAlive,
                                                   ka /= _keepAlive, pl == T.Protocol50 ]
      deliverConnACK pl existing cprops

      wdch <- liftIO newTChanIO
      w <- traverse (\to -> async $ watchdog to wdch pl sess tid) (keepAliveTimeout (_limitKeepAlive lims) pl _keepAlive)
      o <- async $ processOut pl sess
      i <- async $ E.finally (processIn wdch sess pl `E.onException` logConn "process exception" req) (teardown pl cid req)
      retransmit sess
      void (waitAnyCancel ([i, o] <> maybeToList w)) `E.finally` flushDisconnect pl _sessionChan

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
        .| dispatchUntilDisconnect
      where
        -- A packet bigger than the Maximum Packet Size is a protocol
        -- error (DISCONNECT 0x95 for 5.0) [5.0 §3.2.2.3.6].
        tooBig maxSize (PositionRange start end) = maybe False (\m -> posOffset end - posOffset start > fromIntegral m) maxSize
        rejectBig = do
          when (pl == T.Protocol50) $
            sendPacketIO_ (_sessionChan sess) (T.DisconnectPkt (T.DisconnectRequest T.DiscoPacketTooLarge []))
          fail "packet bigger than the maximum packet size"

        -- Nothing follows a client's DISCONNECT, and returning closes the
        -- connection [3.1.1 §3.14.4], [5.0 §3.14.4].
        dispatchUntilDisconnect = await >>= mapM_ \(pos,x) -> do
          maxSize <- lift (asks (_limitPacketSize . limits))
          when (tooBig maxSize pos) $ lift rejectBig
          lift (feed wdch >> dispatch sess x)
          case x of
            T.DisconnectPkt{} -> pure ()
            _                 -> dispatchUntilDisconnect


    commonIn = src .| C.mapM (count StatBytesRcvd)

    commonOut pl = C.mapM (\x -> logDbgL [">> ", tshow x] >> pure x)
                   .| C.map (BL.toStrict . T.toByteString pl)
                   .| C.mapM (count StatBytesSent)
                   .| sink

    deliverConnACK pl existing cprops = runConduit $
      yield (T.ConnACKPkt $ T.ConnACKFlags existing T.ConnAccepted cprops) .| commonOut pl

    -- Each packet is taken off the queue and written with async
    -- exceptions masked, so one isn't lost by the connection being
    -- killed between the two.
    processOut pl Session{..} = forever . E.mask_ $ do
      p <- atomically $ readTBQueue _sessionChan >>= sending
      runConduit $ yield p .| commonOut pl
      where
        -- Once a PUBLISH goes out, sending it again is a resend, so the
        -- copy kept for retransmission gets DUP=1 [MQTT-3.3.1-1].
        sending p@(T.PublishPkt T.PublishRequest{_pubQoS, _pubPktID})
          | _pubQoS > T.QoS0 = p <$ modifyTVar' _sessionQP (Map.adjust (_2 . T.pubDup .~ True) _pubPktID)
        sending p = pure p

    teardown :: (IOE :> es, MQTTD :> es) => T.ProtocolLevel -> ClientID -> T.ConnectRequest -> Eff es ()
    teardown pl cid c@T.ConnectRequest{..} = do
      logConn "disconnection" c
      unregisterClient pl _connID cid

    ensureID (T.ConnPkt c@T.ConnectRequest{_connID=""} pl) = do
      nid <- BL.fromStrict . UUID.toASCIIBytes <$> liftIO randomIO
      logDbgL ["Generating ID for anonymous client: ", tshow nid]
      pure (T.ConnPkt c{T._connID=nid} pl, Just nid)
    ensureID x = pure (x, Nothing)

    feed wdch = atomically (writeTChan wdch True)

    -- A DISCONNECT the connection was ending with (a protocol error, a
    -- takeover or a keep alive timeout) may still be queued when it's
    -- killed.  It's sent before the connection closes [5.0 §4.13].
    flushDisconnect pl ch = when (pl == T.Protocol50) $ do
      pending <- atomically $ flushTBQueue ch
      E.handle (\(_ :: E.SomeException) -> pure ()) . runConduit $
        C.yieldMany [p | p@T.DisconnectPkt{} <- pending] .| commonOut pl

    watchdog pp wdch pl Session{_sessionID, _sessionChan} t = forever $ do
      toch <- liftIO $ registerDelay pp
      timedOut <- atomically $ ((check =<< readTVar toch) $> True) `orElse` (readTChan wdch $> False)
      when timedOut $ do
        logInfoL ["Client with session ", tshow _sessionID, " timed out"]
        when (pl == T.Protocol50) $
          sendPacketIO_ _sessionChan (T.DisconnectPkt (T.DisconnectRequest T.DiscoKeepAliveTimeout []))
        liftIO $ throwTo t MQTTPingTimeout

    retransmit Session{..} = do
      -- We can atomically find the partially transmitted messages and
      -- drop them into an outbound queue that does not exceed the
      -- maximum amount allowable by either the client or our own
      -- queue size.  The rest is returned for async processing.
      now <- liftIO getCurrentTime
      bl <- atomically do
        -- PUBRELs waiting for PUBCOMP are resent first.  They're still in
        -- flight, so each takes a slot [5.0 MQTT-4.4.0-1], [3.1.1 §4.4].
        rel <- Set.toList <$> readTVar _sessionRel
        mapM_ (\p -> sendPacket _sessionChan (T.PubRELPkt (T.PubREL p 0 mempty))) rel
        modifyTVar' _sessionFlight (\x -> x - min x (fromIntegral (length rel)))
        tokens <- readTVar _sessionFlight
        -- Expired messages are dropped, and so are ones too big for this
        -- connection, as if they'd been delivered [MQTT-3.1.2-25].
        modifyTVar' _sessionQP (Map.filter (\e -> stillValid now e && not (tooBig e)))
        (t, bl) <- splitAt (min defaultQueueSize (fromIntegral tokens)) . Map.elems <$> readTVar _sessionQP
        modifyTVar' _sessionFlight (subtract . fromIntegral . length $ t)
        -- DUP is already set on the ones that were sent before.
        mapM_ (sendPacket _sessionChan . T.PublishPkt . uncurry (adjExpiry now)) t
        pure bl
      -- The backlog is processed as space frees up in its queue.
      allSessions <- asks sessions
      mapM_ (atomically . art allSessions) bl
        where
          -- It keeps its original deadline [MQTT-3.3.2-5].
          art allSessions dp = do
              guard =<< isClientConnected _sessionID allSessions
              writeTBQueue _sessionBacklog dp

          tooBig (_, p) = maybe False (`tooLargeFor` p) _sessionClient

          stillValid now (Just t, _) = now < t
          stillValid _ _             = True


webSocketsApp :: (IOE :> es, ScheduleFX SessionID :> es, AuthFX :> es, Fail :> es, MQTTD :> es, Stats :> es, LogFX :> es, DB :> es) => WS.PendingConnection -> Eff es ()
webSocketsApp pc = do
  conn <- liftIO $ WS.acceptRequest pc
  runMQTTDConduit (wsSource conn, wsSink conn, "<unknown>")

  where
    wsSource ws = forever $ do
      bs <- liftIO $ WS.receiveData ws
      unless (BCS.null bs) $ yield bs

    wsSink ws = justM (\bs -> liftIO (WS.sendBinaryData ws bs) >> wsSink ws) =<< await

tcpApp :: (IOE :> es, Fail :> es, ScheduleFX SessionID :> es, AuthFX :> es, MQTTD :> es, DB :> es, Stats :> es, LogFX :> es) => AppData -> Eff es ()
tcpApp ad = runMQTTDConduit (appSource ad, appSink ad, tshow (appSockAddr ad))
