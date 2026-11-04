{-# LANGUAGE ScopedTypeVariables #-}

module Integration where

import           Test.Tasty.HUnit

import           Control.Concurrent     (threadDelay)
import           Control.Concurrent.STM (TChan, check, modifyTVar', newTChanIO,
                                         newTVarIO, orElse, readTChan, readTVar,
                                         readTVarIO, registerDelay, retry,
                                         writeTChan, writeTVar)
import           Control.Exception      (catch)
import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString        as BS
import           Data.Conduit           (await, runConduit, yield, (.|))
import           Data.Conduit.Network   (appSink, appSource, clientSettings,
                                         runTCPClient)
import           Data.IORef             (newIORef, readIORef, writeIORef)
import           Control.Monad          (replicateM)
import qualified Data.ByteString.Lazy   as BL
import           Data.List              (sort)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromJust, fromMaybe)
import           Data.Password.Bcrypt   (PasswordHash (..))
import           Effectful
import           Network.MQTT.Client    as MC
import qualified Network.MQTT.Types     as T
import           Network.Socket.Free    (getFreePort)
import           Network.URI
import           UnliftIO               (Concurrently (..), atomically, bracket,
                                         cancel, concurrently, mapConcurrently_,
                                         runConcurrently)

import           MQTTD
import           MQTTD.Config
import           MQTTD.Logging
import           MQTTD.Main
import           MQTTD.Stats
import           MQTTD.Util             (runFailIO)

type TestServer = URI

testConfig :: Config
testConfig = Config {
  _confDebug = False,
  _confUsers = mempty,
  _confListeners = [], -- filled in by withTestServiceConfig
  _confDefaults = ListenerOptions { _optAllowAnonymous = Just True },
  _confPersist = PersistenceConfig ":memory:"
  }

withTestServiceConfig :: Config -> (TestServer -> Assertion) -> Assertion
withTestServiceConfig conf f = do
  port <- getFreePort
  let uri = fromJust $ parseURI $ "mqtt://127.0.0.1:" <> show port <> "/"
      cf = conf{_confListeners = [MQTTListener "127.0.0.1" port mempty]}
  bracket (runEff . withUnliftStrategy (ConcUnlift Persistent Unlimited) . runFailIO . runNoLogFX . runNewStats $ runServerLogging cf) (mapM_ cancel) (const $ f uri)

withTestService :: (TestServer -> Assertion) -> Assertion
withTestService = withTestServiceConfig testConfig

sleep :: Int -> IO ()
sleep = threadDelay . seconds

unit_basicPubSub :: Assertion
unit_basicPubSub = withTestService $ \u -> do
  mv <- newTVarIO mempty
  (pubber, subber) <- concurrently
                      (MC.connectURI MC.mqttConfig u)
                      (MC.connectURI MC.mqttConfig{_msgCB=MC.SimpleCallback (\_ t v _ -> atomically $ modifyTVar' mv (Map.insert t v)),
                                                   _protocol=MC.Protocol50} u)

  MC.publishq pubber "test/retained0" "future message0" True MC.QoS0 []
  MC.publishq pubber "test/retained1" "future message1" True MC.QoS1 []
  MC.publishq pubber "test/retained2" "future message2" True MC.QoS2 []
  MC.publishq pubber "test2/dontcare" "another retained" True MC.QoS1 []

  -- Subscriber client
  _ <- MC.subscribe subber [("test/#", MC.subOptions),
                            ("test2/+", MC.subOptions{_retainHandling=T.DoNotSendOnSubscribe})] []

  -- Publish a few things
  MC.publishq pubber "nope/nosub" "no subscribers here" False MC.QoS0 []
  MC.publishq pubber "test/tv0" "test message 0" False MC.QoS0 []
  MC.publishq pubber "test/tv1" "test message 1" False MC.QoS1 []
  MC.publishq pubber "test/tv2" "test message 2" False MC.QoS2 []

  -- Wait for results.
  m <- atomically $ do
    m <- readTVar mv
    check (length m >= 6)
    pure m
  assertEqual "Got the messages" (Map.fromList [("test/tv0", "test message 0"),
                                                ("test/tv1", "test message 1"),
                                                ("test/tv2", "test message 2"),
                                                ("test/retained0", "future message0"),
                                                ("test/retained1", "future message1"),
                                                ("test/retained2", "future message2")])
    m

unit_unsub :: Assertion
unit_unsub = withTestService $ \u -> do
  mv <- newTVarIO mempty
  (pubber, subber) <- concurrently
                      (MC.connectURI MC.mqttConfig u)
                      (MC.connectURI MC.mqttConfig{_msgCB=MC.SimpleCallback (\_ t v _ -> atomically $ modifyTVar' mv (Map.insert t v)),
                                                   _protocol=MC.Protocol50} u)

  MC.publishq pubber "test/retained0" "future message0" True MC.QoS0 []
  MC.publishq pubber "test/retained1" "future message1" True MC.QoS1 []
  MC.publishq pubber "test/retained2" "future message2" True MC.QoS2 []
  MC.publishq pubber "test2/dontcare" "another retained" True MC.QoS1 []

  -- Subscriber client
  _ <- MC.subscribe subber [("test/#", MC.subOptions),
                            ("test2/+", MC.subOptions{_retainHandling=T.DoNotSendOnSubscribe})] []

  _ <- MC.unsubscribe subber ["test2/+"] []

  -- Publish a few things
  MC.publishq pubber "nope/nosub" "no subscribers here" False MC.QoS0 []
  MC.publishq pubber "test/tv0" "test message 0" False MC.QoS0 []
  MC.publishq pubber "test2/noreceipt" "test message 0" False MC.QoS0 []

  -- Wait for results.
  m <- atomically $ do
    m <- readTVar mv
    check (length m >= 4)
    pure m
  assertEqual "Got the messages" (Map.fromList [("test/tv0", "test message 0"),
                                                ("test/retained0", "future message0"),
                                                ("test/retained1", "future message1"),
                                                ("test/retained2", "future message2")])
    m

-- This is similar to the above, but guarantees the order of messages.
unit_orderedPubSub :: Assertion
unit_orderedPubSub = withTestService $ \u -> do
  mv <- newTVarIO mempty
  -- We have some processing delay that speeds up as we process,
  -- causing things to basically happen out of order.
  tdv <- newTVarIO 9
  (pubber, subber) <- concurrently
                      (MC.connectURI MC.mqttConfig u)
                      (MC.connectURI MC.mqttConfig{_msgCB=MC.OrderedCallback (
                                                      \_ t v _ -> do
                                                        threadDelay . (* 1000) =<< readTVarIO tdv
                                                        atomically $ do
                                                          modifyTVar' tdv pred
                                                          modifyTVar' mv ((t, v):)),
                                                   _protocol=MC.Protocol50} u)

  MC.publishq pubber "test/retained0" "future message0" True MC.QoS0 []
  MC.publishq pubber "test/retained1" "future message1" True MC.QoS1 []
  MC.publishq pubber "test/retained2" "future message2" True MC.QoS2 []
  MC.publishq pubber "test2/dontcare" "another retained" True MC.QoS1 []

  -- Subscriber client
  _ <- MC.subscribe subber [("test/#", MC.subOptions),
                            ("test2/+", MC.subOptions{_retainHandling=T.DoNotSendOnSubscribe})] []

  -- Publish a few things
  MC.publishq pubber "nope/nosub" "no subscribers here" False MC.QoS0 []
  MC.publishq pubber "test/tv0" "test message 0" False MC.QoS0 []
  MC.publishq pubber "test/tv1" "test message 1" False MC.QoS1 []
  MC.publishq pubber "test/tv2" "test message 2" False MC.QoS2 []

  -- Wait for results.
  m <- atomically $ do
    m <- readTVar mv
    check (length m >= 6)
    pure m

  assertEqual "Got the messages" [
                                     ("test/tv2", "test message 2"),
                                     ("test/tv1", "test message 1"),
                                     ("test/tv0", "test message 0"),
                                     ("test/retained2", "future message2"),
                                     ("test/retained1", "future message1"),
                                     ("test/retained0", "future message0")
                                  ]
    m


unit_retainAsPublished :: Assertion
unit_retainAsPublished = withTestService $ \u -> do
  mv <- newTVarIO mempty
  (pubber, subber) <- concurrently
                      (MC.connectURI MC.mqttConfig u)
                      (MC.connectURI MC.mqttConfig{_msgCB=MC.LowLevelCallback (\_ r@T.PublishRequest{..}
                                                                                -> atomically $ modifyTVar' mv (Map.insert _pubTopic r)),
                                                   _protocol=MC.Protocol50} u)
  MC.publishq pubber "t" "future message0" True MC.QoS0 []

  _ <- MC.subscribe subber [("t", MC.subOptions{_retainAsPublished=True})] []

  r@T.PublishRequest{..} <- atomically $ do
    m <- readTVar mv
    check (length m == 1)
    pure (m Map.! "t")

  assertBool ("message reported retained: " <> show r) _pubRetain

chCallback :: TChan (Topic, BL.ByteString) -> MessageCallback
chCallback ch = MC.SimpleCallback (\_ t v _ -> atomically $ writeTChan ch (t,v))

unit_aliases :: Assertion
unit_aliases = withTestService $ \u -> do
  -- Publisher client
  mv <- newTChanIO
  (pubber, subber) <- concurrently
                      (MC.connectURI MC.mqttConfig{_protocol=MC.Protocol50} u)
                      (MC.connectURI MC.mqttConfig{_msgCB=chCallback mv,
                                                    _protocol=MC.Protocol50,
                                                    _connProps=[T.PropTopicAliasMaximum 5]} u)

  _ <- MC.subscribe subber [("test/#", MC.subOptions)] []

  MC.pubAliased pubber "test/a" "alpha" True MC.QoS0 []
  MC.pubAliased pubber "test/a" "bravo" True MC.QoS2 []
  MC.pubAliased pubber "test/a" "charlie" True MC.QoS1 []
  mapConcurrently_ (\v -> MC.pubAliased pubber "test/a" v True MC.QoS1 []) ["delta", "echo", "foxtrot"]

  -- Wait for results.
  m <- atomically $ replicateM 6 (readTChan mv)
  assertEqual "Got the messages" [("test/a", v) | v <- ["alpha", "bravo", "charlie",
                                                        "delta", "echo", "foxtrot"]]
    (sort m)

unit_shared :: (String -> IO ()) -> Assertion
unit_shared step = withTestService $ \u -> do
  mv <- newTVarIO mempty
  let baseConfig = MC.mqttConfig{_protocol=MC.Protocol50}
      vals = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"]
  (pubber, sub1, sub2, sub3, sub21, sub22) <-
    runConcurrently $ (,,,,,)
    <$> Concurrently (MC.connectURI baseConfig u)
    <*> Concurrently (MC.connectURI baseConfig{_msgCB=mkcb mv (1::Int)} u)
    <*> Concurrently (MC.connectURI baseConfig{_msgCB=mkcb mv 2} u)
    <*> Concurrently (MC.connectURI baseConfig{_msgCB=mkcb mv 3} u)
    <*> Concurrently (MC.connectURI baseConfig{_msgCB=mkcb mv 21} u)
    <*> Concurrently (MC.connectURI baseConfig{_msgCB=mkcb mv 22} u)

  s1 <- traverse (\c -> MC.subscribe c [("$share/a/t/#", MC.subOptions)] []) [sub1, sub2, sub3]
  step ("first subs: " <> show s1)
  s2 <- traverse (\c -> MC.subscribe c [("$share/b/t/#", MC.subOptions)] []) [sub21, sub22]
  step ("second subs: " <> show s2)

  mapM_ (\v -> MC.pubAliased pubber "t/a" v False MC.QoS0 []) vals

  -- Wait for results.
  timedOut <- registerDelay 5000000
  mm <- atomically $ ((check =<< readTVar timedOut) >> (Left <$> readTVar mv)) `orElse` do
    m <- readTVar mv
    check ((sum . fmap length) m >= 12)
    pure (Right m)

  m <- either (\x -> assertFailure ("timed out waiting for result " <> show x)) pure mm

  step (show . Map.assocs $ m)

  assertEqual "Got the messages" [(v, 2) | v <- ["alpha", "bravo", "charlie",
                                                 "delta", "echo", "foxtrot"]]
    (Map.assocs . fmap length $ m)

  where
    mkcb mv i = MC.SimpleCallback (\_ _ v _ -> atomically $ modifyTVar' mv (Map.insertWith (<>) v [i]))

assertFails :: String -> IO a -> Assertion
assertFails msg a = catch (a >> assertFailure msg) (\(_ :: MC.MQTTException) -> pure ())

unit_AAA :: (String -> IO ()) -> Assertion
unit_AAA step = let conf = testConfig{
                     _confUsers = Map.fromList [
                         ("all", User "all" (Plaintext "allpass") []),
                         ("test", User "test" (Plaintext "testpass") [
                             Allow ACLPubSub "test/#", Allow ACLSub "ro/#", Deny "#"]),
                         ("test2", User "test" (HashedPass (PasswordHash
                                                            "$2b$10$RDhbUlKDkvTZsgfhiBlu9./SDwgQDv9UXkD6ZgxMhZ/67D0R7aW5m")) [
                             Allow ACLPubSub "test/#", Allow ACLSub "ro/#", Deny "#"])
                         ],
                     _confDefaults = ListenerOptions (Just False)
                     }
                    baseConfig = MC.mqttConfig{_protocol=MC.Protocol50} in
  withTestServiceConfig conf $ \u -> do
  step "anonymous"
  assertFails "anonymous connection" (MC.connectURI baseConfig u)
  step "bad user"
  assertFails "bad user" (MC.connectURI baseConfig (withCreds u "someuser" "x"))
  step "bad pass"
  assertFails "bad user" (MC.connectURI baseConfig (withCreds u "all" "x"))

  step "good user"
  allUser <- MC.connectURI baseConfig (withCreds u "all" "allpass")
  alls1 <- MC.subscribe allUser [("x", T.subOptions),
                                 ("test/a", T.subOptions),
                                 ("$share/x/test/a", T.subOptions),
                                 ("$share/x/misc/a", T.subOptions)] []
  assertEqual "all sub response" ([Right T.QoS0, Right T.QoS0, Right T.QoS0, Right T.QoS0], []) alls1
  assertFails "all user publish to share" (MC.pubAliased allUser "$share/blah/test/a" "x" True MC.QoS1 [])
  MC.pubAliased allUser "test/a" "x" True MC.QoS1 []

  step "test user"
  testUser <- MC.connectURI baseConfig (withCreds u "test" "testpass")
  tests1 <- MC.subscribe testUser [("x", T.subOptions),
                                   ("test/a", T.subOptions),
                                   ("ro/a", T.subOptions),
                                   ("$share/x/test/a", T.subOptions),
                                   ("$share/x/misc/a", T.subOptions)] []
  assertEqual "test sub response" ([Left T.SubErrNotAuthorized,
                                    Right T.QoS0,
                                    Right T.QoS0,
                                    Right T.QoS0,
                                    Left T.SubErrNotAuthorized], []) tests1
  MC.pubAliased testUser "test/a" "x" True MC.QoS1 []
  assertFails "test user publish to share" (MC.pubAliased testUser "$share/blah/test/a" "x" True MC.QoS1 [])
  assertFails "publish from bad user" (MC.pubAliased testUser "fail/a" "x" True MC.QoS1 [])
  assertFails "publish to ro topic" (MC.pubAliased testUser "ro/a" "x" True MC.QoS1 [])

  step "test user 2"
  assertFails "bad password for test2" $ MC.connectURI baseConfig (withCreds u "test2" "testpass")
  _ <- MC.connectURI baseConfig (withCreds u "test2" "testpass2")
  pure ()

  where
    withCreds url u p = let a = fromMaybe nullURIAuth (uriAuthority url) in url{uriAuthority=Just a{uriUserInfo = u <> ":" <> p <> "@"}}

unit_retainedOnSubscribe :: Assertion
unit_retainedOnSubscribe = withTestService $ \u -> do
    got <- newTVarIO Nothing
    c <- MC.connectURI MC.mqttConfig{_msgCB=MC.LowLevelCallback (\_ r -> atomically $ writeTVar got (Just r))} u
    MC.publishq c "t" "x" True MC.QoS0 []
    _ <- MC.subscribe c [("t", MC.subOptions)] []
    req@T.PublishRequest{..} <- atomically $ readTVar got >>= maybe retry pure
    assertBool ("retained message delivered with RETAIN=0: " <> show req) _pubRetain

-- H1: Connecting with CleanSession=1 / Clean Start must discard any
-- existing session, including its subscriptions [MQTT-3.1.2-6] (3.1.1),
-- [MQTT-3.1.2-4] (5.0).
unit_cleanSessionDropsOldSubs :: Assertion
unit_cleanSessionDropsOldSubs = withTestService $ \u -> do
  -- net-mqtt's connectURI takes the client ID from the URI fragment.
  let cu = u{uriFragment="#h1-clean-session"}
  -- Persistent session with a QoS1 subscription.
  old <- MC.connectURI MC.mqttConfig{_cleanSession=False} cu
  _ <- MC.subscribe old [("h1/old", MC.subOptions{_subQoS=MC.QoS1})] []

  -- Take over the session with the same client ID, but a clean session.
  -- (Taking over rather than disconnecting first, since mqttd doesn't
  -- close the connection on DISCONNECT and normalDisconnect waits 10s.)
  got <- newTVarIO []
  new <- MC.connectURI MC.mqttConfig{_cleanSession=True,
                                     _msgCB=MC.OrderedCallback (\_ t _ _ -> atomically $ modifyTVar' got (t:))} cu
  _ <- MC.subscribe new [("h1/canary", MC.subOptions{_subQoS=MC.QoS1})] []

  pubber <- MC.connectURI MC.mqttConfig u
  MC.publishq pubber "h1/old" "should not arrive" False MC.QoS1 []
  MC.publishq pubber "h1/canary" "should arrive" False MC.QoS1 []

  timedOut <- registerDelay 5000000
  r <- atomically $ ((check =<< readTVar timedOut) >> (Left <$> readTVar got)) `orElse` do
    ts <- readTVar got
    check ("h1/canary" `elem` ts)
    pure (Right ts)
  ts <- either (\x -> assertFailure ("timed out waiting for canary, got " <> show x)) pure r
  assertEqual "only the clean session's subscription delivered" ["h1/canary"] (reverse ts)

-- * Will tests

-- | A raw MQTT connection, for tests that need a client to misbehave
-- (e.g. drop the connection without sending DISCONNECT), which
-- net-mqtt's client won't do.
data RawConn = RawConn {
  rawSend      :: T.MQTTPkt -> IO (),
  rawSendBytes :: BS.ByteString -> IO (),
  rawRecv      :: IO T.MQTTPkt
  }

-- | Connect, wait for a successful CONNACK and run the action.  The
-- connection is closed (without DISCONNECT) when the action returns.
withRawConn :: TestServer -> T.ProtocolLevel -> T.ConnectRequest -> (RawConn -> IO a) -> IO a
withRawConn u pl req f = runTCPClient (clientSettings port "127.0.0.1") $ \ad -> do
  buf <- newIORef BS.empty
  let sendBytes b = runConduit $ yield b .| appSink ad
      send = sendBytes . BL.toStrict . T.toByteString pl
      recv = readIORef buf >>= go . A.parse (T.parsePacket pl)
        where
          go (A.Done rest p) = writeIORef buf rest >> pure p
          go (A.Fail _ _ e)  = assertFailure ("parse error: " <> e)
          go (A.Partial k)   = runConduit (appSource ad .| await) >>=
                               maybe (assertFailure "connection closed") (go . k)
      rc = RawConn send sendBytes recv
  send (T.ConnPkt req pl)
  recv >>= \case
    T.ConnACKPkt (T.ConnACKFlags _ T.ConnAccepted _) -> f rc
    x -> assertFailure ("unexpected connect response: " <> show x)
  where
    port = read . drop 1 . uriPort . fromJust . uriAuthority $ u

-- | Subscribe to a QoS1 topic on a raw connection so the session
-- survives disconnection (sessions with only QoS0 subs don't; see H3).
rawKeepSession :: RawConn -> IO ()
rawKeepSession rc = do
  rawSend rc (T.SubscribePkt (T.SubscribeRequest 1 [("keep/session", T.subOptions{T._subQoS=T.QoS1})] []))
  rawRecv rc >>= \case
    T.SubACKPkt _ -> pure ()
    x -> assertFailure ("unexpected subscribe response: " <> show x)

willReq :: BL.ByteString -> Bool -> [T.Property] -> [T.Property] -> T.ConnectRequest
willReq cid clean cprops wprops = T.connectRequest{
  T._connID=cid,
  T._cleanSession=clean,
  T._connProperties=cprops,
  T._lastWill=Just T.LastWill{T._willRetain=False, T._willQoS=T.QoS0,
                              T._willTopic="will/" <> cid, T._willMsg="gone",
                              T._willProps=wprops}
  }

-- | Watch for will messages (and anything else on will/#).
willWatcher :: TestServer -> IO (TChan T.PublishRequest)
willWatcher u = do
  ch <- newTChanIO
  c <- MC.connectURI MC.mqttConfig{_protocol=MC.Protocol50,
                                   _msgCB=MC.LowLevelCallback (\_ r -> atomically $ writeTChan ch r)} u
  _ <- MC.subscribe c [("will/#", MC.subOptions)] []
  pure ch

-- | Wait up to the given number of microseconds for a message.
nextWithin :: Int -> TChan a -> IO (Maybe a)
nextWithin us ch = do
  to <- registerDelay us
  atomically $ (Just <$> readTChan ch) `orElse` (readTVar to >>= check >> pure Nothing)

expectWill :: Int -> TChan T.PublishRequest -> IO T.PublishRequest
expectWill us ch = nextWithin us ch >>= maybe (assertFailure "expected a will message, but none arrived") pure

expectNoWill :: Int -> TChan T.PublishRequest -> Assertion
expectNoWill us ch = nextWithin us ch >>= mapM_ (\r -> assertFailure ("unexpected will: " <> show r))

-- Baseline: a dropped connection publishes its will [MQTT-3.1.2-8].
unit_willOnDrop311 :: Assertion
unit_willOnDrop311 = withTestService $ \u -> do
  ch <- willWatcher u
  withRawConn u T.Protocol311 (willReq "drop311" True [] []) (const $ pure ())
  T.PublishRequest{..} <- expectWill 2000000 ch
  assertEqual "will topic" "will/drop311" _pubTopic

-- H2: The will is published when the connection drops, not when a
-- persistent session eventually expires [MQTT-3.1.2-8].
unit_willOnDropPersistent311 :: Assertion
unit_willOnDropPersistent311 = withTestService $ \u -> do
  ch <- willWatcher u
  withRawConn u T.Protocol311 (willReq "persist311" False [] []) rawKeepSession
  T.PublishRequest{..} <- expectWill 2000000 ch
  assertEqual "will topic" "will/persist311" _pubTopic

-- H2: The will is delayed by the Will Delay Interval when the session
-- outlives it [5.0 §3.1.3.2.2].
unit_willDelay50 :: Assertion
unit_willDelay50 = withTestService $ \u -> do
  ch <- willWatcher u
  withRawConn u T.Protocol50 (willReq "delay50" False [T.PropSessionExpiryInterval 60]
                                                      [T.PropWillDelayInterval 2]) rawKeepSession
  expectNoWill 1000000 ch
  T.PublishRequest{..} <- expectWill 3000000 ch
  assertEqual "will topic" "will/delay50" _pubTopic

-- H2: Reconnecting before the Will Delay Interval passes cancels the
-- will [MQTT-3.1.3-9].
unit_willDelayCancelledByReconnect50 :: Assertion
unit_willDelayCancelledByReconnect50 = withTestService $ \u -> do
  ch <- willWatcher u
  let req = willReq "cancel50" False [T.PropSessionExpiryInterval 60] [T.PropWillDelayInterval 2]
  withRawConn u T.Protocol50 req rawKeepSession
  withRawConn u T.Protocol50 req{T._lastWill=Nothing} $ \_ -> expectNoWill 3000000 ch

-- H2: When a new connection takes over a session, the old connection's
-- will is published [MQTT-3.1.4-3] (5.0; Will Delay 0), and 3.1.1
-- likewise closes the old connection without a DISCONNECT.
willOnTakeover :: T.ProtocolLevel -> BL.ByteString -> Assertion
willOnTakeover pl cid = withTestService $ \u -> do
  ch <- willWatcher u
  let req = willReq cid False [T.PropSessionExpiryInterval 60 | pl == T.Protocol50] []
  withRawConn u pl req $ \rc -> do
    rawKeepSession rc
    withRawConn u pl req{T._lastWill=Nothing} $ \_ -> do
      T.PublishRequest{..} <- expectWill 2000000 ch
      assertEqual "will topic" ("will/" <> cid) _pubTopic

unit_willOnTakeover311 :: Assertion
unit_willOnTakeover311 = willOnTakeover T.Protocol311 "takeover311"

unit_willOnTakeover50 :: Assertion
unit_willOnTakeover50 = willOnTakeover T.Protocol50 "takeover50"

-- T2: A normal DISCONNECT discards the will [MQTT-3.1.2-10] (3.1.1),
-- [MQTT-3.14.4-3] (5.0).
noWillOnDisconnect :: T.ProtocolLevel -> BL.ByteString -> Assertion
noWillOnDisconnect pl cid = withTestService $ \u -> do
  ch <- willWatcher u
  withRawConn u pl (willReq cid True [] []) $ \rc ->
    rawSend rc (T.DisconnectPkt (T.DisconnectRequest T.DiscoNormalDisconnection []))
  expectNoWill 1000000 ch

unit_noWillOnDisconnect311 :: Assertion
unit_noWillOnDisconnect311 = noWillOnDisconnect T.Protocol311 "disco311"

unit_noWillOnDisconnect50 :: Assertion
unit_noWillOnDisconnect50 = noWillOnDisconnect T.Protocol50 "disco50"

-- T2: In 5.0, a DISCONNECT with Remaining Length 0 means reason code
-- 0x00 (Normal disconnection) [5.0 §3.14.2.1], and that's what most
-- clients send.  It must discard the will just like the long form.
unit_noWillOnShortDisconnect50 :: Assertion
unit_noWillOnShortDisconnect50 = withTestService $ \u -> do
  ch <- willWatcher u
  withRawConn u T.Protocol50 (willReq "shortdisco50" True [] []) $ \rc ->
    rawSendBytes rc (BS.pack [0xe0, 0x00])
  expectNoWill 1000000 ch

-- T1: Will Delay Interval isn't a PUBLISH property, so it must not be
-- forwarded, but the will's other properties must be [5.0 §3.3.2.3].
unit_willPropsOnPublish50 :: Assertion
unit_willPropsOnPublish50 = withTestService $ \u -> do
  ch <- willWatcher u
  withRawConn u T.Protocol50 (willReq "props50" True [] [T.PropWillDelayInterval 0,
                                                         T.PropContentType "text/plain",
                                                         T.PropUserProperty "k" "v"]) (const $ pure ())
  T.PublishRequest{..} <- expectWill 2000000 ch
  assertBool ("will delay forwarded: " <> show _pubProps) (T.PropWillDelayInterval 0 `notElem` _pubProps)
  assertBool ("content type missing: " <> show _pubProps) (T.PropContentType "text/plain" `elem` _pubProps)
  assertBool ("user property missing: " <> show _pubProps) (T.PropUserProperty "k" "v" `elem` _pubProps)
