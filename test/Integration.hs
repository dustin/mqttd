{-# LANGUAGE ScopedTypeVariables #-}

module Integration where

import           Test.Tasty
import           Test.Tasty.HUnit

import           Control.Concurrent     (newChan, threadDelay)
import           Control.Concurrent.STM (TChan, check, modifyTVar', newTChanIO, newTVarIO, orElse, readTChan, readTVar,
                                         registerDelay, writeTChan)
import           Control.Exception      (catch)
import           Control.Monad          (replicateM, when)
import           Control.Monad.Logger   (runChanLoggingT)
import qualified Data.ByteString.Lazy   as BL
import           Data.Either            (isLeft)
import           Data.List              (sort)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromJust)
import           Data.Password.Bcrypt   (PasswordHash (..))
import           Network.MQTT.Client    as MC
import qualified Network.MQTT.Types     as T
import           Network.Socket.Free    (getFreePort)
import           Network.URI
import           UnliftIO               (Concurrently (..), atomically, bracket, cancel, concurrently, mapConcurrently_,
                                         runConcurrently)

import           MQTTD
import           MQTTD.Config
import           MQTTD.Main

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
  ch <- newChan
  -- async (mapM_ (\(_,_,_,x) -> print x) =<< getChanContents ch)
  bracket (runChanLoggingT ch $ runServerLogging cf) (mapM_ cancel) (const $ f uri)

withTestService :: (TestServer -> Assertion) -> Assertion
withTestService = withTestServiceConfig testConfig

sleep :: Int -> IO ()
sleep = threadDelay . seconds

testBasicPubSub :: Assertion
testBasicPubSub = withTestService $ \u -> do
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

testUnsub :: Assertion
testUnsub = withTestService $ \u -> do
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

testRetainAsPublished :: Assertion
testRetainAsPublished = withTestService $ \u -> do
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

testAliases :: Assertion
testAliases = withTestService $ \u -> do
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

testShared :: (String -> IO ()) -> Assertion
testShared step = withTestService $ \u -> do
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

  when (isLeft mm) $ assertFailure ("timed out waiting for result: " <> show mm)

  let Right m = mm

  step (show . Map.assocs $ m)

  assertEqual "Got the messages" [(v, 2) | v <- ["alpha", "bravo", "charlie",
                                                 "delta", "echo", "foxtrot"]]
    (Map.assocs . fmap length $ m)

  where
    mkcb mv i = MC.SimpleCallback (\_ _ v _ -> atomically $ modifyTVar' mv (Map.insertWith (<>) v [i]))

assertFails :: String -> IO a -> Assertion
assertFails msg a = catch (a >> assertFailure msg) (\(_ :: MC.MQTTException) -> pure ())

testAAA :: (String -> IO ()) -> Assertion
testAAA step = let conf = testConfig{
                     _confUsers = Map.fromList [
                         ("all", User "all" (Plaintext "allpass") []),
                         ("test", User "test" (Plaintext "testpass") [
                             Allow ACLPubSub "test/#", Allow ACLSub "ro/#", Deny "#"]),
                         ("test2", User "test" (HashedPass (PasswordHash
                                                            "$2b$10$RDhbUlKDkvTZsgfhiBlu9./SDwgQDv9UXkD6ZgxMhZ/67D0R7aW5m")) [
                             Allow ACLPubSub "test/#", Allow ACLSub "ro/#", Deny "#"])
                         ],
                     _confDefaults = (ListenerOptions (Just False))
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
    withCreds url u p = let Just a = uriAuthority url in url{uriAuthority=Just a{uriUserInfo = u <> ":" <> p <> "@"}}

tests :: [TestTree]
tests = [
  testCase "Basic" testBasicPubSub,
  testCase "Unsub" testUnsub,
  testCase "Retain as Published" testRetainAsPublished,
  testCase "Aliases" testAliases,
  testCaseSteps "Shared" testShared,
  testCaseSteps "AAA" testAAA
  ]
