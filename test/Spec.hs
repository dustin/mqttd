{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TypeApplications #-}

import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           Control.Applicative      (liftA2)
import           Control.Concurrent.STM   (atomically, newTVarIO)
import           Data.Either              (isLeft, isRight)
import           Data.List                (sort)
import qualified Data.Map.Strict          as Map
import           Data.Monoid              (Sum (..))
import           Data.Set                 (Set)
import           Data.String              (IsString (..))
import qualified Data.Text                as Txt
import           Data.Word                (Word16, Word8)

import           Data.Password.Bcrypt     (PasswordHash (..))
import           Network.MQTT.Arbitrary   (MatchingTopic (..), arbitraryMatchingTopic, arbitraryTopic)
import           Network.MQTT.Topic       (Filter, Topic, unTopic)

import           MQTTD
import           MQTTD.Config
import           MQTTD.SubTree            (SubTree)
import qualified MQTTD.SubTree            as Sub
import           MQTTD.Types

import qualified Integration

testConfigFiles :: Assertion
testConfigFiles =
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

instance Eq a => EqProp (SubTree a) where (=-=) = eq

instance (Monoid a, Arbitrary a, Eq a) => Arbitrary (SubTree a) where
  arbitrary = do
    filters <- resize 20 $ listOf1 (ttof <$> arbitraryTopic ['a'..'d'] (1,7) (1,3))
    subbers <- resize 20 $ listOf1 arbitrary
    Sub.fromList <$> (resize 50 $ listOf1 (liftA2 (,) (elements filters) (elements subbers)))

      where ttof = fromString . Txt.unpack . unTopic

  shrink = fmap Sub.fromList . shrinkList (const []) . Sub.flatten

testSubTree :: Assertion
testSubTree =
    mapM_ aTest [
    ("a/b/c", ["a/b/c", "a/+/c", "a/+/+", "+/+/+", "+/b/c", "#", "a/#"]),
    ("a/d/c", ["a/+/c", "a/+/+", "+/+/+", "#", "a/#"]),
    ("b/b/c", ["+/+/+", "+/b/c", "#", "b/#"]),
    ("b/real", ["#", "b/#"]),
    ("a/b/x", ["#","+/+/+","a/#","a/+/+"]),
    ("$special/case", ["$special/#"])]

  where
    someSubs = foldr (\x -> Sub.add x [x]) mempty [
      "a/b/c", "a/+/c", "a/+/+", "+/+/+", "+/b/c",
      "#", "a/#", "b/#", "$special/#"]
    aTest (f,w) = assertEqual (show f) (sort w) (sort $ Sub.find f someSubs)

testACLs :: Assertion
testACLs = mapM_ aTest [
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
propACL (ACLParams (t, i, a, want)) = want === isRight (authTopic (classifyTopic (ttof t)) i a)
  where ttof = fromString . Txt.unpack . unTopic


testTopicClassification :: Assertion
testTopicClassification = mapM_ aTest [
  ("a", Normal "a"),
  ("a/b", Normal "a/b"),
  ("$share/x", InvalidTopic),
  ("$share/a/b", SharedSubscription "a" "b"),
  ("$share/a/b/c", SharedSubscription "a" "b/c")
  ]
  where
    aTest (i, want) = assertEqual (show i) want (classifyTopic i)

newtype CollidingMatchingTopic = CollidingMatchingTopic (Topic, [Filter]) deriving (Show, Eq)

instance Arbitrary CollidingMatchingTopic where
  arbitrary = CollidingMatchingTopic <$> arbitraryMatchingTopic ['a'..'c'] (1,3) (1,6) (1,3)

propSubTreeMapping :: [CollidingMatchingTopic] -> Property
propSubTreeMapping matches = label ("collisions " <> collisions) $
                             all (\(t, m) -> m `elem` Sub.find t st) tp
  where
    tp = [(t, m) | (CollidingMatchingTopic (t, ms)) <- matches, m <- ms]
    st = foldr (\(_,t) -> Sub.add t [t]) mempty tp
    collisions = (\n -> show n <> "-" <> show (n+9)) . (*10) . (`div` 10) . getSum . foldMap (Sum . subtract 1 . length) $ st

propNextPacket :: (Enum a, Bounded a, Eq a, Num a) => NonZero a -> Property
propNextPacket (NonZero n) =
  ioProperty $ do
    v <- newTVarIO n
    i <- atomically $ nextPktID v
    pure (i /= 0)

roundTrips :: (Eq a, Show a, Arbitrary a) => (a -> b) -> (b -> a) -> a -> Property
roundTrips t f = f.t >>= (===)

tests :: [TestTree]
tests = [
  testCase "config files" testConfigFiles,
  testCase "ACLs" testACLs,
  testProperty "ACL props" propACL,

  localOption (QC.QuickCheckTests 500) $
    testProperty "packet IDs (8bit) are never 0" (propNextPacket :: NonZero Word8 -> Property),
  localOption (QC.QuickCheckTests 100000) $
    testProperty "packet IDs (16bit) are never 0" (propNextPacket :: NonZero Word16 -> Property),

  testCase "subtree" testSubTree,
  testGroup "subtree properties" [
      testProperty "flatten/fromList" $ roundTrips (filter (not.null . snd) . Sub.flatten @(SubTree (Set Int))) Sub.fromList,
      testProperty "finding" propSubTreeMapping,
      testProperties "functor" (unbatch $ functor (undefined :: SubTree ([Int], Int, Int))),
      testProperties "foldable" (unbatch $ foldable (undefined :: SubTree (Sum Int, Sum Int, Sum Int, Sum Int, Sum Int))),
      testProperties "traversable" (unbatch $ traversable (undefined :: SubTree (Sum Int, Sum Int, Sum Int))),
      testProperties "semigroup" (unbatch $ semigroup (undefined :: SubTree [Int], undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: SubTree [Int]))
      ],

  testCase "topic classification" testTopicClassification,

  testGroup "listener properties" [
      testProperties "semigroup" (unbatch $ semigroup (undefined :: ListenerOptions, undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: ListenerOptions))
      ],
  testGroup "Integration" Integration.tests
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
