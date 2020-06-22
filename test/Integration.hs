module Integration where

import           Test.Tasty
import           Test.Tasty.HUnit

import           Control.Concurrent     (newChan, threadDelay)
import           Control.Concurrent.STM (TChan, check, modifyTVar', newTChanIO, newTVarIO, orElse, readTChan, readTVar,
                                         registerDelay, writeTChan)
import           Control.Monad          (replicateM, when)
import           Control.Monad.Logger   (runChanLoggingT)
import qualified Data.ByteString.Lazy   as BL
import           Data.Either            (isLeft)
import           Data.List              (sort)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromJust)
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

withTestService :: (TestServer -> Assertion) -> Assertion
withTestService f = do
  port <- getFreePort
  let conf = Config {
        _confDebug = False,
        _confUsers = mempty,
        _confListeners = [MQTTListener "127.0.0.1" port mempty],
        _confDefaults = ListenerOptions { _optAllowAnonymous = Just True },
        _confPersist = PersistenceConfig ":memory:"
        }

  let uri = fromJust $ parseURI $ "mqtt://127.0.0.1:" <> show port <> "/"

  ch <- newChan
  bracket (runChanLoggingT ch $ runServerLogging conf) (mapM_ cancel) (const $ f uri)

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

tests :: [TestTree]
tests = [
    testCase "Basic" testBasicPubSub,
    testCase "Aliases" testAliases,
    testCaseSteps "Shared" testShared
    ]
