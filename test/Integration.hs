{-# LANGUAGE ScopedTypeVariables #-}

module Integration where

import           Test.Tasty.HUnit

import           Control.Concurrent     (threadDelay)
import           Control.Concurrent.STM (TChan, TVar, check, modifyTVar', newTChanIO,
                                         newTVarIO, orElse, readTChan, readTVar,
                                         readTVarIO, registerDelay, retry,
                                         writeTChan, writeTVar)
import           Control.Exception      (catch, finally)
import qualified Control.Exception      as E
import qualified Data.UUID              as UUID
import qualified Database.SQLite.Simple as SQL
import           System.Directory       (getTemporaryDirectory, removeFile)
import           System.Random          (randomIO)
import           System.Timeout         (timeout)
import qualified Data.Attoparsec.ByteString as A
import qualified Data.ByteString        as BS
import           Data.Conduit           (await, runConduit, yield, (.|))
import           Data.Conduit.Network   (appSink, appSource, clientSettings,
                                         runTCPClient)
import           Data.IORef             (newIORef, readIORef, writeIORef)
import           Control.Monad          (replicateM, unless)
import qualified Data.ByteString.Lazy   as BL
import           Data.List              (sort)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromJust, fromMaybe)
import           Data.Password.Bcrypt   (PasswordHash (..))
import           Data.Time.Clock        (NominalDiffTime, diffUTCTime,
                                         getCurrentTime)
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
  rawSend           :: T.MQTTPkt -> IO (),
  rawSendBytes      :: BS.ByteString -> IO (),
  rawRecv           :: IO T.MQTTPkt,
  -- | Receive a packet, or Nothing if the server closed the connection.
  rawRecvMaybe      :: IO (Maybe T.MQTTPkt),
  rawSessionPresent :: T.SessionReuse,
  rawConnAckProps   :: [T.Property]
  }

-- | Connect, wait for a successful CONNACK and run the action.  The
-- connection is closed (without DISCONNECT) when the action returns.
withRawConn :: TestServer -> T.ProtocolLevel -> T.ConnectRequest -> (RawConn -> IO a) -> IO a
withRawConn u pl req f = runTCPClient (clientSettings port "127.0.0.1") $ \ad -> do
  buf <- newIORef BS.empty
  let sendBytes b = runConduit $ yield b .| appSink ad
      send = sendBytes . BL.toStrict . T.toByteString pl
      recvMaybe = readIORef buf >>= go . A.parse (T.parsePacket pl)
        where
          go (A.Done rest p) = writeIORef buf rest >> pure (Just p)
          go (A.Fail _ _ e)  = assertFailure ("parse error: " <> e)
          go (A.Partial k)   = runConduit (appSource ad .| await) >>= maybe (pure Nothing) (go . k)
      recv = recvMaybe >>= maybe (assertFailure "connection closed") pure
  send (T.ConnPkt req pl)
  recv >>= \case
    T.ConnACKPkt (T.ConnACKFlags sp T.ConnAccepted props) -> f (RawConn send sendBytes recv recvMaybe sp props)
    x -> assertFailure ("unexpected connect response: " <> show x)
  where
    port = read . drop 1 . uriPort . fromJust . uriAuthority $ u

rawSubscribe :: RawConn -> BL.ByteString -> T.QoS -> IO ()
rawSubscribe rc f q = do
  rawSend rc (T.SubscribePkt (T.SubscribeRequest 1 [(f, T.subOptions{T._subQoS=q})] []))
  rawRecv rc >>= \case
    T.SubACKPkt _ -> pure ()
    x -> assertFailure ("unexpected subscribe response: " <> show x)

-- | Give a raw connection's session a QoS1 subscription.
rawKeepSession :: RawConn -> IO ()
rawKeepSession rc = rawSubscribe rc "keep/session" T.QoS1

-- | Receive a PUBLISH on a raw connection (skipping other packets).
rawExpectPublish :: RawConn -> IO T.PublishRequest
rawExpectPublish rc = timeout 2000000 go >>= maybe (assertFailure "no PUBLISH received") pure
  where go = rawRecv rc >>= \case
          T.PublishPkt p -> pure p
          _              -> go

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

-- * Session persistence

-- H3: A session outlives its connection until it expires, whatever its
-- subscriptions' QoS, and resumes with Session Present=1
-- [MQTT-3.1.2-4] (3.1.1), [5.0 §3.1.2.11.2].
sessionReq :: T.ProtocolLevel -> BL.ByteString -> T.ConnectRequest
sessionReq pl cid = T.connectRequest{T._connID=cid, T._cleanSession=False,
                                     T._connProperties=[T.PropSessionExpiryInterval 60 | pl == T.Protocol50]}

qos0SessionSurvivesDisconnect :: T.ProtocolLevel -> BL.ByteString -> Assertion
qos0SessionSurvivesDisconnect pl cid = withTestService $ \u -> do
  withRawConn u pl (sessionReq pl cid) $ \rc -> rawSubscribe rc "h3/t" T.QoS0
  withRawConn u pl (sessionReq pl cid) $ \rc -> do
    assertEqual "session present" T.ExistingSession (rawSessionPresent rc)
    pubber <- MC.connectURI MC.mqttConfig u
    MC.publishq pubber "h3/t" "hello" False MC.QoS0 []
    T.PublishRequest{..} <- rawExpectPublish rc
    assertEqual "delivered on the resumed subscription" "h3/t" _pubTopic

unit_qos0SessionSurvivesDisconnect311 :: Assertion
unit_qos0SessionSurvivesDisconnect311 = qos0SessionSurvivesDisconnect T.Protocol311 "qos0-311"

unit_qos0SessionSurvivesDisconnect50 :: Assertion
unit_qos0SessionSurvivesDisconnect50 = qos0SessionSurvivesDisconnect T.Protocol50 "qos0-50"

-- H3: ...including a session with no subscriptions at all.
emptySessionSurvivesDisconnect :: T.ProtocolLevel -> BL.ByteString -> Assertion
emptySessionSurvivesDisconnect pl cid = withTestService $ \u -> do
  withRawConn u pl (sessionReq pl cid) (const $ pure ())
  withRawConn u pl (sessionReq pl cid) $ \rc ->
    assertEqual "session present" T.ExistingSession (rawSessionPresent rc)

unit_emptySessionSurvivesDisconnect311 :: Assertion
unit_emptySessionSurvivesDisconnect311 = emptySessionSurvivesDisconnect T.Protocol311 "empty-311"

unit_emptySessionSurvivesDisconnect50 :: Assertion
unit_emptySessionSurvivesDisconnect50 = emptySessionSurvivesDisconnect T.Protocol50 "empty-50"

withTempDB :: (FilePath -> IO a) -> IO a
withTempDB f = do
  dir <- getTemporaryDirectory
  name <- UUID.toString <$> randomIO
  let path = dir <> "/mqttd-test-" <> name <> ".db"
  f path `finally` (removeFile path `catch` (\(_ :: IOError) -> pure ()))

-- | Wait for a session's subscriptions to be written to the DB.
waitForStoredSubs :: FilePath -> BL.ByteString -> Assertion
waitForStoredSubs path cid = timeout 2000000 loop >>= maybe (assertFailure "subscription never stored") pure
  where
    loop = do
      n <- count `catch` \e -> case SQL.sqlError e of
                                SQL.ErrorBusy -> pure 0 -- the server is writing
                                _             -> E.throwIO e
      unless (n > 0) $ threadDelay 10000 >> loop
    count = SQL.withConnection path $ \db -> do
      [SQL.Only (n :: Int)] <- SQL.query db "select count(*) from session_subs where session_id = ?" (SQL.Only cid)
      pure n

-- H3: ...and across a restart.
unit_qos0SessionSurvivesRestart311 :: Assertion
unit_qos0SessionSurvivesRestart311 = withTempDB $ \path -> do
  let conf = testConfig{_confPersist = PersistenceConfig path}
      req = sessionReq T.Protocol311 "restart-311"
  withTestServiceConfig conf $ \u -> withRawConn u T.Protocol311 req $ \rc -> do
    rawSubscribe rc "h3/t" T.QoS0
    waitForStoredSubs path "restart-311"
  withTestServiceConfig conf $ \u -> withRawConn u T.Protocol311 req $ \rc -> do
    assertEqual "session present" T.ExistingSession (rawSessionPresent rc)
    pubber <- MC.connectURI MC.mqttConfig u
    MC.publishq pubber "h3/t" "hello" False MC.QoS0 []
    T.PublishRequest{..} <- rawExpectPublish rc
    assertEqual "delivered on the restored subscription" "h3/t" _pubTopic

-- * Packet IDs

-- H5: Each in-flight PUBLISH to a session needs its own packet ID, even
-- when overlapping subscriptions deliver the same message twice
-- [MQTT-2.2.1-3].
unit_overlappingSubsDistinctPktIDs :: Assertion
unit_overlappingSubsDistinctPktIDs = withTestService $ \u ->
  withRawConn u T.Protocol50 T.connectRequest{T._connID="h5-overlap"} $ \rc -> do
    rawSubscribe rc "h5/#" T.QoS1
    rawSubscribe rc "h5/t" T.QoS1
    pubber <- MC.connectURI MC.mqttConfig u
    MC.publishq pubber "h5/t" "x" False MC.QoS1 []
    -- Neither is acknowledged, so both are in flight.
    a <- rawExpectPublish rc
    b <- rawExpectPublish rc
    assertBool ("duplicate in-flight packet ID: " <> show (a, b)) (T._pubPktID a /= T._pubPktID b)

-- * QoS 2 delivery

-- | Receive the next packet on a raw connection, failing if none arrives.
rawRecvWithin :: RawConn -> IO T.MQTTPkt
rawRecvWithin rc = timeout 2000000 (rawRecv rc) >>= maybe (assertFailure "no packet received") pure

rawExpectPubREL :: RawConn -> IO T.PktID
rawExpectPubREL rc = rawRecvWithin rc >>= \case
  T.PubRELPkt (T.PubREL p _ _) -> pure p
  x -> assertFailure ("expected PUBREL, got " <> show x)

-- | Receive a QoS2 PUBLISH, PUBREC it, and receive the PUBREL.
rawPubRecvToRel :: RawConn -> IO T.PktID
rawPubRecvToRel rc = do
  T.PublishRequest{_pubPktID} <- rawExpectPublish rc
  rawSend rc (T.PubRECPkt (T.PubREC _pubPktID 0 []))
  p <- rawExpectPubREL rc
  assertEqual "PUBREL for our PUBREC" _pubPktID p
  pure p

-- H6: After PUBREC, the server must keep the PUBREL state and resend
-- the PUBREL when the session resumes [5.0 MQTT-4.4.0-1], [3.1.1 §4.4].
pubrelResentAfterReconnect :: T.ProtocolLevel -> BL.ByteString -> Assertion
pubrelResentAfterReconnect pl cid = withTestService $ \u -> do
  pid <- withRawConn u pl (sessionReq pl cid) $ \rc -> do
    rawSubscribe rc "h6/t" T.QoS2
    pubber <- MC.connectURI MC.mqttConfig u
    MC.publishq pubber "h6/t" "x" False MC.QoS2 []
    rawPubRecvToRel rc
  -- Dropped before PUBCOMP.
  withRawConn u pl (sessionReq pl cid) $ \rc -> do
    assertEqual "session present" T.ExistingSession (rawSessionPresent rc)
    p <- rawExpectPubREL rc
    assertEqual "resent PUBREL" pid p

unit_pubrelResentAfterReconnect311 :: Assertion
unit_pubrelResentAfterReconnect311 = pubrelResentAfterReconnect T.Protocol311 "h6-311"

unit_pubrelResentAfterReconnect50 :: Assertion
unit_pubrelResentAfterReconnect50 = pubrelResentAfterReconnect T.Protocol50 "h6-50"

-- H6: ...but once PUBCOMP arrives, the exchange is done and nothing is
-- resent.
unit_nothingResentAfterPubcomp :: Assertion
unit_nothingResentAfterPubcomp = withTestService $ \u -> do
  let req = sessionReq T.Protocol311 "h6-done"
  withRawConn u T.Protocol311 req $ \rc -> do
    rawSubscribe rc "h6/t" T.QoS2
    pubber <- MC.connectURI MC.mqttConfig u
    MC.publishq pubber "h6/t" "x" False MC.QoS2 []
    p <- rawPubRecvToRel rc
    rawSend rc (T.PubCOMPPkt (T.PubCOMP p 0 []))
    -- A PINGRESP shows the PUBCOMP has been processed.
    rawSend rc T.PingPkt
    rawRecvWithin rc >>= assertEqual "ping response" T.PongPkt
  withRawConn u T.Protocol311 req $ \rc -> do
    assertEqual "session present" T.ExistingSession (rawSessionPresent rc)
    timeout 500000 (rawRecv rc) >>= mapM_ (\x -> assertFailure ("unexpected resend: " <> show x))

-- * Shared subscriptions

-- | Count messages received, by topic.
countingClient :: TestServer -> IO (MC.MQTTClient, TVar (Map.Map Topic Int))
countingClient u = do
  mv <- newTVarIO mempty
  c <- MC.connectURI MC.mqttConfig{_protocol=MC.Protocol50,
                                   _msgCB=MC.OrderedCallback (\_ t _ _ -> atomically $ modifyTVar' mv (Map.insertWith (+) t 1))} u
  pure (c, mv)

awaitCount :: TVar (Map.Map Topic Int) -> Topic -> Int -> Assertion
awaitCount mv t n = do
  to <- registerDelay 2000000
  r <- atomically $ ((readTVar to >>= check) >> (Left <$> readTVar mv)) `orElse` do
    m <- readTVar mv
    check (Map.findWithDefault 0 t m >= n)
    pure (Right ())
  either (\m -> assertFailure ("timed out waiting for " <> show n <> " of " <> show t <> ": " <> show m)) pure r

-- K1: Unsubscribing from a shared subscription removes only that
-- session; the other members keep getting messages [MQTT-3.10.4-1].
unit_unsubSharedKeepsOthers :: Assertion
unit_unsubSharedKeepsOthers = withTestService $ \u -> do
  (c1, _) <- countingClient u
  (c2, mv2) <- countingClient u
  mapM_ (\c -> MC.subscribe c [("$share/g/k1/t", MC.subOptions)] []) [c1, c2]
  _ <- MC.unsubscribe c1 ["$share/g/k1/t"] []
  pubber <- MC.connectURI MC.mqttConfig u
  mapM_ (\i -> MC.publishq pubber "k1/t" (BL.pack [i]) False MC.QoS0 []) [1..10]
  awaitCount mv2 "k1/t" 10

-- K1: ...and only from the named group.
unit_unsubSharedOnlyThatGroup :: Assertion
unit_unsubSharedOnlyThatGroup = withTestService $ \u -> do
  (c, mv) <- countingClient u
  _ <- MC.subscribe c [("$share/a/k1/x", MC.subOptions), ("$share/b/k1/x", MC.subOptions),
                       ("k1/canary", MC.subOptions)] []
  _ <- MC.unsubscribe c ["$share/a/k1/x"] []
  pubber <- MC.connectURI MC.mqttConfig u
  MC.publishq pubber "k1/x" "x" False MC.QoS0 []
  MC.publishq pubber "k1/canary" "c" False MC.QoS0 []
  awaitCount mv "k1/canary" 1
  m <- readTVarIO mv
  assertEqual "delivered once, via group b" 1 (Map.findWithDefault 0 "k1/x" m)

-- K1: UNSUBACK reports what was actually removed.
unit_unsubSharedUnsuback :: Assertion
unit_unsubSharedUnsuback = withTestService $ \u -> do
  (c, _) <- countingClient u
  _ <- MC.subscribe c [("$share/g/k1/t", MC.subOptions)] []
  r <- MC.unsubscribe c ["$share/g/k1/t", "$share/g/k1/nope"] []
  assertEqual "unsuback" ([T.UnsubSuccess, T.UnsubNoSubscriptionExisted], []) r

-- * Backlog

-- | A raw connection with Receive Maximum 1, subscribed to k4/# at QoS1,
-- holding the first message ("first") in flight so later ones are
-- backlogged.  Returns the in-flight message's packet ID.
withBacklog :: TestServer -> BL.ByteString -> (RawConn -> MC.MQTTClient -> T.PktID -> IO a) -> IO a
withBacklog u cid f =
  withRawConn u T.Protocol50 T.connectRequest{T._connID=cid, T._connProperties=[T.PropReceiveMaximum 1]} $ \rc -> do
    rawSubscribe rc "k4/#" T.QoS1
    pubber <- MC.connectURI MC.mqttConfig{_protocol=MC.Protocol50} u
    MC.publishq pubber "k4/first" "first" False MC.QoS1 []
    T.PublishRequest{_pubTopic, _pubPktID} <- rawExpectPublish rc
    assertEqual "first message" "k4/first" _pubTopic
    f rc pubber _pubPktID

-- K4: A backlogged message that hasn't expired is delivered when a slot
-- frees up.
unit_backlogDeliversUnexpired :: Assertion
unit_backlogDeliversUnexpired = withTestService $ \u -> withBacklog u "k4-live" $ \rc pubber pid -> do
  MC.publishq pubber "k4/live" "live" False MC.QoS1 [T.PropMessageExpiryInterval 60]
  rawSend rc (T.PubACKPkt (T.PubACK pid 0 []))
  T.PublishRequest{_pubTopic} <- rawExpectPublish rc
  assertEqual "backlogged message delivered" "k4/live" _pubTopic

-- K4: ...but one that expired while waiting is discarded
-- [MQTT-3.3.2-5].
unit_backlogSkipsExpired :: Assertion
unit_backlogSkipsExpired = withTestService $ \u -> withBacklog u "k4-expired" $ \rc pubber pid -> do
  MC.publishq pubber "k4/expired" "expired" False MC.QoS1 [T.PropMessageExpiryInterval 1]
  MC.publishq pubber "k4/live" "live" False MC.QoS1 []
  threadDelay 1500000
  rawSend rc (T.PubACKPkt (T.PubACK pid 0 []))
  T.PublishRequest{_pubTopic} <- rawExpectPublish rc
  assertEqual "expired message skipped" "k4/live" _pubTopic

-- * Keep alive

-- | Wait for the server to close a raw connection, returning how long it
-- took (ignoring any packets it sends first).
rawTimeToClose :: RawConn -> IO NominalDiffTime
rawTimeToClose rc = do
  start <- getCurrentTime
  let loop = rawRecvMaybe rc >>= maybe (pure ()) (const loop)
  timeout 10000000 loop >>= maybe (assertFailure "connection never closed") pure
  (`diffUTCTime` start) <$> getCurrentTime

-- M4: A client that sends nothing is disconnected after one and a half
-- times its Keep Alive [3.1.1 MQTT-3.1.2-24], [5.0 MQTT-3.1.2-22].
unit_keepAliveTimeout :: Assertion
unit_keepAliveTimeout = withTestService $ \u ->
  withRawConn u T.Protocol50 T.connectRequest{T._connID="m4", T._keepAlive=2} $ \rc -> do
    t <- rawTimeToClose rc
    assertBool ("closed after " <> show t <> ", expected about 3s") (t >= 2.5 && t <= 3.5)

-- * Maximum Packet Size

-- M8: A PUBLISH bigger than the client's Maximum Packet Size isn't sent,
-- and is treated as if it had been delivered [MQTT-3.1.2-24],
-- [MQTT-3.1.2-25].  (With Receive Maximum 1, a discarded message still
-- in flight would block the next one.)
unit_maximumPacketSize :: Assertion
unit_maximumPacketSize = withTestService $ \u ->
  withRawConn u T.Protocol50 T.connectRequest{T._connID="m8",
                                              T._connProperties=[T.PropMaximumPacketSize 100,
                                                                 T.PropReceiveMaximum 1]} $ \rc -> do
    rawSubscribe rc "m8/#" T.QoS1
    pubber <- MC.connectURI MC.mqttConfig{_protocol=MC.Protocol50} u
    MC.publishq pubber "m8/big" (BL.replicate 200 0x78) False MC.QoS1 []
    MC.publishq pubber "m8/small" "small" False MC.QoS1 []
    T.PublishRequest{_pubTopic} <- rawExpectPublish rc
    assertEqual "oversize message discarded" "m8/small" _pubTopic

-- * Subscription Identifiers

-- M9: mqttd doesn't support Subscription Identifiers, so it must say so
-- in CONNACK [5.0 §3.2.2.3.12]...
unit_subIdsNotAvailable :: Assertion
unit_subIdsNotAvailable = withTestService $ \u ->
  withRawConn u T.Protocol50 T.connectRequest{T._connID="m9-connack"} $ \rc ->
    assertBool ("CONNACK properties: " <> show (rawConnAckProps rc))
      (T.PropSubscriptionIdentifierAvailable 0 `elem` rawConnAckProps rc)

-- M9: ...and a SUBSCRIBE with one anyway is a protocol error, answered
-- with DISCONNECT 0xA1 [5.0 §3.8.2.1.2].
unit_subIdRejected :: Assertion
unit_subIdRejected = withTestService $ \u ->
  withRawConn u T.Protocol50 T.connectRequest{T._connID="m9-sub"} $ \rc -> do
    rawSend rc (T.SubscribePkt (T.SubscribeRequest 1 [("m9/t", T.subOptions)] [T.PropSubscriptionIdentifier 7]))
    timeout 2000000 (rawRecvMaybe rc) >>= \case
      Nothing -> assertFailure "connection wasn't closed"
      Just Nothing -> pure () -- closed (the DISCONNECT is best effort)
      Just (Just (T.DisconnectPkt (T.DisconnectRequest T.DiscoSubscriptionIdentifiersNotSupported _))) -> pure ()
      Just (Just x) -> assertFailure ("expected DISCONNECT 0xA1, got " <> show x)
