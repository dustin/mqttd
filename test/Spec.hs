{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TypeApplications #-}

module Spec where

import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           Control.Concurrent       (myThreadId)
import           Control.Concurrent.STM   (atomically, newTBQueueIO, newTVarIO, readTVar)
import           Data.Either              (isLeft, isRight)
import           Data.Foldable            (for_)
import qualified Data.Map.Strict          as Map
import           Data.Word                (Word16, Word8)

import           Data.Password.Bcrypt     (PasswordHash (..))
import           Network.MQTT.Arbitrary   (MatchingTopic (..))
import qualified Network.MQTT.Types       as T
import           Network.MQTT.Topic       (Topic, toFilter)

import           Control.Lens             hiding (elements)
import           Effectful
import           MQTTD.ScheduleFX
import           Scheduler

import           Database.SQLite.Simple   (open)

import           MQTTD
import           MQTTD.Authorizer
import           MQTTD.Config
import           MQTTD.DB
import           MQTTD.Logging
import           MQTTD.Stats
import qualified MQTTD.SubTree            as SubTree
import           MQTTD.Types
import           MQTTD.Util               (runFailIO)

unit_configFiles :: Assertion
unit_configFiles =
  mapM_ aTest [
      ("test.conf", Config {_confDebug = True,
                            _confDefaults = ListenerOptions (Just True),
                            _confUsers = Map.fromList [
                               ("myuser", User "myuser" (Plaintext "mypw") []),
                               ("otheruser", User "otheruser" (HashedPass (PasswordHash "otherpw")) [
                                   Allow ACLPubSub "tmp/#", Allow ACLSub "$SYS/#", Deny "#"
                                   ])
                               ],
                            _confListeners = [MQTTListener "*" 1883 mempty,
                                              WSListener "*" 8080 mempty,
                                              MQTTSListener "*" 8883 "certificate.pem" "key.pem"
                                              (ListenerOptions (Just False) )],
                            _confPersist = PersistenceConfig "mqttd.db"}),
      ("test2.conf", Config {_confDebug = False,
                             _confUsers = mempty,
                             _confDefaults = mempty,
                             _confListeners = [MQTTListener "*" 1883 mempty,
                                               MQTTSListener "*" 8883 "certificate.pem" "key.pem" mempty],
                             _confPersist = PersistenceConfig ":memory:"})
      ]
  where
    aTest (f,w) = assertEqual f w =<< parseConfFile ("test/" <> f)

instance EqProp ListenerOptions where (=-=) = eq

instance Arbitrary ListenerOptions where
  arbitrary = ListenerOptions <$> arbitrary

unit_ACLs :: Assertion
unit_ACLs = mapM_ aTest [
  -- Subscription tests
  ([], IntentSubscribe, "empty/stuffs", isRight),
  ([Deny "#"], IntentSubscribe, "deny", isLeft),
  ([Allow ACLPubSub "tmp/#", Deny "#"], IntentSubscribe, "denied", isLeft),
  ([Allow ACLPubSub "tmp/#", Deny "#"], IntentSubscribe, "tmp/ok", isRight),
  ([Deny "tmp/#", Allow ACLPubSub "#"], IntentSubscribe, "tmp/denied", isLeft),
  -- Publish tests
  ([], IntentPublish, "empty/stuffs", isRight),
  ([Deny "#"], IntentPublish, "deny", isLeft),
  ([Allow ACLPubSub "#"], IntentPublish, "allow", isRight),
  ([Deny "tmp/#", Allow ACLPubSub "#"], IntentPublish, "allowed", isRight),
  ([Deny "tmp/#", Allow ACLSub "#"], IntentPublish, "denied", isLeft)
  ]
  where
    aTest (a,i,t,f) = assertBool (show (a, i, t)) $ f (authTopic (classifyTopic t) i a)

-- ACLParams is used to create arbitary parameters for the ACL test
-- property below.
--
-- Apologies for the Bool blindness, but it's True if
-- the given topic should be allowed to perform the given Intention
-- with the given ACL.
--
-- The ACL includes a non-matching ACL entry before and after the one
-- we want to match.  It doesn't match by virtue of it not looking
-- like anything we'd generate.
newtype ACLParams = ACLParams (Topic, Intention, [ACL], Bool) deriving (Eq, Show)

instance Arbitrary ACLParams where
  arbitrary = do
    MatchingTopic (t, ms) <- arbitrary
    let m = head ms
    i <- elements [IntentPublish, IntentSubscribe]
    acl <- ($ m) <$> anACL
    let want = case (i, acl) of
                 (IntentPublish, Allow ACLPubSub _)   -> True
                 (IntentSubscribe, Allow ACLPubSub _) -> True
                 (IntentSubscribe, Allow ACLSub _)    -> True
                 _                                    -> False
    (a1, a3) <- liftA2 (,) anACL anACL
    pure $ ACLParams (t, i, [a1 "$$$", acl, a3 "$$$"], want)

      where
        anACL = elements [Deny, Allow ACLPubSub, Allow ACLSub]

  shrink (ACLParams (t, i, [a,b,c], w)) = [ACLParams (t, i, [a,b], w),
                                           ACLParams (t, i, [b,c], w)]
  shrink _ = []

propACL :: ACLParams -> Property
propACL (ACLParams (t, i, a, want)) = want === isRight (authTopic (classifyTopic (toFilter t)) i a)

unit_topicClassification :: Assertion
unit_topicClassification = mapM_ aTest [
  ("a", Normal "a"),
  ("a/b", Normal "a/b"),
  ("$share/x", InvalidTopic),
  ("$share/a/b", SharedSubscription "a" "b"),
  ("$share/a/b/c", SharedSubscription "a" "b/c")
  ]
  where
    aTest (i, want) = assertEqual (show i) want (classifyTopic i)

propNextPacket :: (Enum a, Bounded a, Eq a, Num a) => NonZero a -> Property
propNextPacket (NonZero n) =
  ioProperty $ do
    v <- newTVarIO n
    i <- atomically $ nextPktID v
    pure (i /= 0)

unit_listenerOptions :: Assertion
unit_listenerOptions = do
  let tbl = [
        (Nothing, Just True, Just True),
        (Just True, Nothing, Just True),
        (Just False, Just True, Just False),
        (Just False, Just False, Just False),
        (Just True, Just False, Just True),
        (Nothing, Nothing, Nothing)
        ]
  for_ tbl $ \(a, b, want) ->
    assertEqual (show (a, b)) (ListenerOptions want) (ListenerOptions a <> ListenerOptions b)

unit_ephemeralSessionCleanup :: Assertion
unit_ephemeralSessionCleanup = do
  env <- newEnv
  db <- open ":memory:"
  initDB db
  dbq <- newTBQueueIO 100
  expirer <- newRunner :: IO (QueueRunner SessionID)
  tid <- myThreadId
  let req = T.ConnectRequest { T._username = Nothing
                             , T._password = Nothing
                             , T._lastWill = Nothing
                             , T._cleanSession = True
                             , T._keepAlive = 60
                             , T._connID = "client"
                             , T._connProperties = [T.PropSessionExpiryInterval 0]
                             }
      runAction = runEff . withUnliftStrategy (ConcUnlift Persistent Unlimited) .
                  runFailIO . runNoLogFX . runNewStats . runDB db dbq .
                  runSchedule expirer . runAuth (Authorizer mempty True) .
                  fmap fst . runMQTTD env
  runAction $ do
    (sess, _reuse) <- registerClient req 1 tid
    subscribe sess (T.SubscribeRequest 1 [("test/#", T.subOptions{T._subQoS=T.QoS1})] [])
    let sid = sess ^. sessionID
    unregisterClient T.Protocol50 sid 1
    e <- getEnv
    (sids, as) <- liftIO . atomically $ do
      sids <- readTVar (sessions e)
      as <- readTVar (allSubs e)
      pure (sids, as)
    liftIO $ do
      assertBool "session was not cleaned up after disconnect" (Map.null sids)
      assertBool "subscriptions were not cleaned up after disconnect" (all (Map.null . snd) $ SubTree.flatten as)
    flushStats
    stats <- retrieveStats
    let subCount = sum $ map (Map.size . snd) $ SubTree.flatten as
    liftIO $ assertEqual "subscription stat mismatch" (Just subCount) (Map.lookup StatSubscriptions stats)

test_Spec :: [TestTree]
test_Spec = [
  localOption (QC.QuickCheckTests 500) $
    testProperty "packet IDs (8bit) are never 0" (propNextPacket :: NonZero Word8 -> Property),
  localOption (QC.QuickCheckTests 100000) $
    testProperty "packet IDs (16bit) are never 0" (propNextPacket :: NonZero Word16 -> Property),

  testGroup "listener properties" [
      testProperties "semigroup" (unbatch $ semigroup (undefined :: ListenerOptions, undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: ListenerOptions))
      ]
  ]
