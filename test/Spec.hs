{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Spec where

import qualified Control.Exception        as E
import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           Control.Concurrent       (myThreadId)
import           Control.Concurrent.STM   (atomically, newTBQueueIO, readTVar)
import           Data.Either              (isLeft, isRight)
import           Data.Foldable            (for_)
import qualified Data.Map.Strict          as Map
import           Data.Maybe               (listToMaybe)

import           Data.Password.Bcrypt     (PasswordHash (..))
import           Network.MQTT.Arbitrary   (MatchingTopic (..))
import           Network.MQTT.Topic       (Topic, toFilter)
import qualified Network.MQTT.Types       as T

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
                                   Allow ACLPubSub "tmp/#", Allow ACLSub "$SYS/#", DenyWrite "ro/#", Deny "#"
                                   ])
                               ],
                            _confListeners = [MQTTListener "*" 1883 mempty,
                                              WSListener "*" 8080 mempty,
                                              MQTTSListener "*" 8883 "certificate.pem" "key.pem"
                                              (ListenerOptions (Just False) )],
                            _confPersist = PersistenceConfig "mqttd.db",
                            _confLimits = noLimits,
                            _confResponsePrefix = Nothing,
                            _confRedirect = Nothing,
                            _confClientEvents = False}),
      ("test2.conf", Config {_confDebug = False,
                             _confUsers = mempty,
                             _confDefaults = mempty,
                             _confListeners = [MQTTListener "*" 1883 mempty,
                                               MQTTSListener "*" 8883 "certificate.pem" "key.pem" mempty],
                             _confPersist = PersistenceConfig ":memory:",
                             _confLimits = noLimits,
                            _confResponsePrefix = Nothing,
                            _confRedirect = Nothing,
                            _confClientEvents = False}),
      ("test3.conf", Config {_confDebug = False,
                             _confUsers = mempty,
                             _confDefaults = mempty,
                             _confListeners = [MQTTListener "*" 1883 mempty],
                             _confPersist = PersistenceConfig ":memory:",
                             _confLimits = Limits{_limitPacketSize = Just 1048576, _limitKeepAlive = Just 120},
                             _confResponsePrefix = Just "responses/",
                             _confRedirect = Nothing,
                            _confClientEvents = False}),
      ("test4.conf", Config {_confDebug = False,
                             _confUsers = mempty,
                             _confDefaults = mempty,
                             _confListeners = [MQTTListener "*" 1883 mempty],
                             _confPersist = PersistenceConfig ":memory:",
                             _confLimits = noLimits,
                             _confResponsePrefix = Nothing,
                             _confRedirect = Just (ServerMoved "mqtt.example.com:1883"),
                             _confClientEvents = True})
      ]
  where
    aTest (f,w) = assertEqual f w =<< parseConfFile ("test/" <> f)

-- Anything the parser doesn't understand is an error, rather than the end
-- of the config.
unit_badConfigRejected :: Assertion
unit_badConfigRejected = E.try (parseConfFile "test/bad.conf") >>= \case
  Left (_ :: E.IOException) -> pure ()
  Right c                   -> assertFailure ("parsed: " <> show c)

-- The example config parses.
unit_exampleConfig :: Assertion
unit_exampleConfig = () <$ parseConfFile "mqttd.conf"

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
  ([Deny "tmp/#", Allow ACLSub "#"], IntentPublish, "denied", isLeft),
  -- deny write denies publishing, and leaves subscribing to later entries
  ([DenyWrite "ro/#"], IntentPublish, "ro/x", isLeft),
  ([DenyWrite "ro/#"], IntentSubscribe, "ro/x", isRight),
  ([DenyWrite "ro/#", Deny "#"], IntentSubscribe, "ro/x", isLeft),
  ([DenyWrite "ro/#", Allow ACLPubSub "#"], IntentPublish, "ro/x", isLeft),
  -- $SYS is the broker's: nobody can publish there, whatever the ACL says
  ([], IntentPublish, "$SYS/broker/version", isLeft),
  ([], IntentPublish, "$SYS", isLeft),
  ([Allow ACLPubSub "#"], IntentPublish, "$SYS/x", isLeft),
  ([Allow ACLPubSub "$SYS/#"], IntentPublish, "$SYS/x", isLeft),
  ([], IntentSubscribe, "$SYS/#", isRight),
  ([], IntentPublish, "$other/x", isRight)
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
    -- A topic and one of the filters matching it (there may be none).
    (t, m) <- arbitrary `suchThatMap` (\(MatchingTopic (t', ms)) -> (,) t' <$> listToMaybe ms)
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
  ("$share/a/b/c", SharedSubscription "a" "b/c"),
  -- K2: A share name is at least one character, with no wildcards
  -- [MQTT-4.8.2-1].
  ("$share//x", InvalidTopic),
  ("$share/+/x", InvalidTopic),
  ("$share/#/x", InvalidTopic),
  ("$share/a+/x", InvalidTopic)
  ]
  where
    aTest (i, want) = assertEqual (show i) want (classifyTopic i)

-- H5: The next packet ID is never 0 and never one that's in use.
propNextPktIDFree :: T.PktID -> [T.PktID] -> Property
propNextPktIDFree lastID used = case nextPktID (`elem` used) lastID of
  Nothing -> counterexample "no free ID" False
  Just i  -> counterexample (show i) (i /= 0 && i `notElem` used)

-- With nothing in use, it's the one after the last, wrapping to 1.
propNextPktIDSequential :: T.PktID -> Property
propNextPktIDSequential lastID =
  nextPktID (const False) lastID === Just (if lastID == maxBound then 1 else lastID + 1)

unit_nextPktIDSkipsInUseAcrossWrap :: Assertion
unit_nextPktIDSkipsInUseAcrossWrap = assertEqual "" (Just 3) (nextPktID (`elem` [65535, 1, 2]) 65534)

unit_nextPktIDExhausted :: Assertion
unit_nextPktIDExhausted = assertEqual "" Nothing (nextPktID (const True) 7)

-- M4: The keep alive timeout is one and a half times the Keep Alive,
-- and a 3.1.1 Keep Alive of 0 disables it (5.0 clients asking for 0 are
-- given a Server Keep Alive).
unit_keepAliveTimeouts :: Assertion
unit_keepAliveTimeouts = do
  assertEqual "3.1.1, 0" Nothing (keepAliveTimeout Nothing T.Protocol311 0)
  assertEqual "3.1.1, 2" (Just 3000000) (keepAliveTimeout Nothing T.Protocol311 2)
  assertEqual "5.0, 2" (Just 3000000) (keepAliveTimeout Nothing T.Protocol50 2)
  assertEqual "5.0, 0" (Just (seconds 2700)) (keepAliveTimeout Nothing T.Protocol50 0)
  -- With a maximum of 60s
  assertEqual "max, 5.0, 300" (Just (seconds 90)) (keepAliveTimeout (Just 60) T.Protocol50 300)
  assertEqual "max, 5.0, 0" (Just (seconds 90)) (keepAliveTimeout (Just 60) T.Protocol50 0)
  assertEqual "max, 5.0, 2" (Just 3000000) (keepAliveTimeout (Just 60) T.Protocol50 2)
  assertEqual "max, 3.1.1, 300" (Just (seconds 450)) (keepAliveTimeout (Just 60) T.Protocol311 300)

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
    (sess, _reuse) <- registerClient T.Protocol50 req 1 tid
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
    n <- subscriptionCount
    liftIO $ assertEqual "subscriptions left" 0 n

-- The subscription count includes shared subscriptions, and so doesn't
-- go negative when they're removed.
unit_sharedSubscriptionCount :: Assertion
unit_sharedSubscriptionCount = do
  env <- newEnv
  db <- open ":memory:"
  initDB db
  dbq <- newTBQueueIO 100
  expirer <- newRunner :: IO (QueueRunner SessionID)
  tid <- myThreadId
  let req = T.connectRequest{T._connID="counted", T._cleanSession=False}
      runAction = runEff . withUnliftStrategy (ConcUnlift Persistent Unlimited) .
                  runFailIO . runNoLogFX . runNewStats . runDB db dbq .
                  runSchedule expirer . runAuth (Authorizer mempty True) .
                  fmap fst . runMQTTD env
  runAction $ do
    (sess, _) <- registerClient T.Protocol311 req 1 tid
    subscribe sess (T.SubscribeRequest 1 [("a/b", T.subOptions), ("$share/g/c/d", T.subOptions)] [])
    _ <- unsubscribe sess ["$share/g/c/d"]
    n <- subscriptionCount
    liftIO $ assertEqual "subscriptions" 1 n

test_Spec :: [TestTree]
test_Spec = [
  testProperty "next packet ID is free" propNextPktIDFree,
  localOption (QC.QuickCheckTests 100000) $
    testProperty "next packet ID is sequential" propNextPktIDSequential,

  testGroup "listener properties" [
      testProperties "semigroup" (unbatch $ semigroup (undefined :: ListenerOptions, undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: ListenerOptions))
      ]
  ]
