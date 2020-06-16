module Integration where

import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           Control.Applicative      (liftA2)
import           Control.Concurrent       (Chan (..), newChan, threadDelay)
import           Control.Concurrent.STM   (TVar, check, modifyTVar', newTChanIO, newTVarIO, readTChan, readTVar, retry,
                                           writeTChan)
import           Control.Monad            (forever, replicateM, void, when)
import           Control.Monad.Logger     (LogLevel (..), MonadLogger (..), filterLogger, logInfoN, runChanLoggingT)
import           Data.Conduit             (ConduitT, Void, await, runConduit, yield, (.|))
import           Data.Either              (isLeft, isRight)
import           Data.List                (intercalate, sort)
import qualified Data.Map.Strict          as Map
import           Data.Maybe               (fromJust)
import           Data.Monoid              (Sum (..))
import           Data.Set                 (Set)
import           Data.Text                (Text)
import qualified Data.Text                as Text
import           Database.SQLite.Simple   hiding (bind, close)
import           Network.MQTT.Client      as MC
import qualified Network.MQTT.Lens        as T
import qualified Network.MQTT.Types       as T
import           Network.Socket.Free      (getFreePort)
import           Network.URI
import           UnliftIO                 (async, atomically, bracket, cancel, mapConcurrently_, waitAnyCancel)

import           MQTTD
import           MQTTD.Conduit
import           MQTTD.Config
import           MQTTD.DB
import           MQTTD.Main
import           MQTTD.Types
import           MQTTD.Util

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
  bracket (runChanLoggingT ch $ runServerLogging conf) (mapM_ cancel) (const $ waitForConn uri >> f uri)

  where
    waitForConn _ = sleep 1 -- TODO:  Some positive signal that listeners are ready.

saveCB mv _ t v _ = atomically $ modifyTVar' mv (Map.insert t v)

sleep = threadDelay . seconds

testBasicPubSub :: Assertion
testBasicPubSub = withTestService $ \u -> do
  -- Publisher client
  pubber <- MC.connectURI MC.mqttConfig u
  MC.publishq pubber "test/retained0" "future message0" True MC.QoS0 []
  MC.publishq pubber "test/retained1" "future message1" True MC.QoS1 []
  MC.publishq pubber "test/retained2" "future message2" True MC.QoS2 []
  MC.publishq pubber "test2/dontcare" "another retained" True MC.QoS0 []

  -- Subscriber client
  mv <- newTVarIO mempty
  subber <- MC.connectURI MC.mqttConfig{_msgCB=MC.SimpleCallback (saveCB mv), _protocol=MC.Protocol50} u
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
  pubber <- MC.connectURI MC.mqttConfig{_protocol=MC.Protocol50} u
  mv <- newTChanIO
  subber <- MC.connectURI MC.mqttConfig{_msgCB=MC.SimpleCallback (\_ t v _ -> atomically $ writeTChan mv (t,v)),
                                        _protocol=MC.Protocol50,
                                        _connProps=[T.PropTopicAliasMaximum 5]} u
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
