module Integration where

import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           Control.Applicative      (liftA2)
import           Control.Concurrent       (threadDelay)
import           Control.Concurrent.STM   (TVar, check, modifyTVar', newTVarIO, readTVar, retry)
import           Control.Monad            (forever, void, when)
import           Control.Monad.Logger     (LogLevel (..), MonadLogger (..), filterLogger, logInfoN, runStderrLoggingT)
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
import           UnliftIO                 (async, atomically, bracket, cancel, waitAnyCancel)

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

  bracket (runServer conf) (mapM_ cancel) (const $ waitForConn uri >> f uri)

  where
    waitForConn _ = sleep 1 -- TODO:  Some positive signal that listeners are ready.

saveCB mv _ t v _ = atomically $ modifyTVar' mv (Map.insert t v)

sleep = threadDelay . seconds

testBasicPubSub :: Assertion
testBasicPubSub = withTestService $ \u -> do
  -- Publisher client
  pubber <- MC.connectURI MC.mqttConfig u
  MC.publishq pubber "test/retained" "future message" True MC.QoS0 []

  -- Subscriber client
  mv <- newTVarIO mempty
  subber <- MC.connectURI MC.mqttConfig{_msgCB=MC.SimpleCallback (saveCB mv), _protocol=MC.Protocol50} u
  _ <- MC.subscribe subber [("test/#", MC.subOptions)] []

  -- Publish a few things
  MC.publishq pubber "nope/nosub" "no subscribers here" False MC.QoS0 []
  MC.publishq pubber "test/tv0" "test message 0" False MC.QoS0 []
  MC.publishq pubber "test/tv1" "test message 1" False MC.QoS1 []
  MC.publishq pubber "test/tv2" "test message 2" False MC.QoS2 []

  -- Wait for results.
  m <- atomically $ do
    m <- readTVar mv
    check (length m >= 4)
    pure m
  assertEqual "Got the message" m (Map.fromList [("test/tv0", "test message 0"),
                                                 ("test/tv1", "test message 1"),
                                                 ("test/tv2", "test message 2"),
                                                 ("test/retained", "future message")])

tests :: [TestTree]
tests = [
  testCase "Basic" testBasicPubSub
  ]
