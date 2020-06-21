module Integration where

import           Test.Tasty
import           Test.Tasty.HUnit

import           Control.Concurrent     (newChan, threadDelay)
import           Control.Concurrent.STM (check, modifyTVar', newTChanIO, newTVarIO, readTChan, readTVar, writeTChan)
import           Control.Monad          (replicateM)
import           Control.Monad.Logger   (runChanLoggingT)
import           Data.List              (sort)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromJust)
import           Network.MQTT.Client    as MC
import qualified Network.MQTT.Types     as T
import           Network.Socket.Free    (getFreePort)
import           Network.URI
import           UnliftIO               (atomically, bracket, cancel, concurrently, mapConcurrently_)

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

testAliases :: Assertion
testAliases = withTestService $ \u -> do
  -- Publisher client
  mv <- newTChanIO
  (pubber, subber) <- concurrently
                      (MC.connectURI MC.mqttConfig{_protocol=MC.Protocol50} u)
                      (MC.connectURI MC.mqttConfig{_msgCB=MC.SimpleCallback (\_ t v _ -> atomically $ writeTChan mv (t,v)),
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

tests :: [TestTree]
tests = [
  testCase "Basic" testBasicPubSub,
  testCase "Aliases" testAliases
  ]
