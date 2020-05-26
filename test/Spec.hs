{-# LANGUAGE TypeApplications #-}

import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           Control.Applicative      (liftA2)
import           Data.Either              (isLeft, isRight)
import           Data.List                (intercalate, sort)
import qualified Data.Map.Strict          as Map
import           Data.Monoid              (Sum (..))
import           Data.Set                 (Set)
import           Data.Text                (Text, pack)

import           MQTTD
import           MQTTD.Config
import           MQTTD.SubTree            (SubTree (..))
import qualified MQTTD.SubTree            as Sub

testConfigFiles :: Assertion
testConfigFiles =
  mapM_ aTest [
      ("test.conf", Config {_confDebug = True,
                            _confDefaults = ListenerOptions (Just True),
                            _confUsers = Map.fromList [
                               ("myuser", User "myuser" "mypw" []),
                               ("otheruser", User "otheruser" "otherpw" [
                                   Allow "tmp/#", Deny "#"
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
    noops = ListenerOptions Nothing

instance EqProp ListenerOptions where (=-=) = eq

instance Arbitrary ListenerOptions where
  arbitrary = ListenerOptions <$> arbitrary

instance Eq a => EqProp (SubTree a) where (=-=) = eq

newtype ATopic = ATopic { unTopic :: Text } deriving (Eq, Show)

instance Arbitrary ATopic where
  arbitrary = do
    n <- choose (1, 6)
    ATopic . pack . intercalate "/" <$> vectorOf n seg
      where seg = choose (1, 8) >>= flip vectorOf (choose ('a', 'z'))

instance (Monoid a, Arbitrary a, Eq a) => Arbitrary (SubTree a) where
  arbitrary = do
    topics <- choose (1, 20) >>= flip vectorOf (unTopic <$> arbitrary)
    subbers <- choose (1, 20) >>= vector
    total <- choose (1, 50)
    Sub.fromList <$> vectorOf total (liftA2 (,) (elements topics) (elements subbers))

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
  ([], "empty/stuffs", isRight),
  ([Deny "#"], "deny", isLeft),
  ([Allow "#"], "allow", isRight),
  ([Allow "tmp/#", Deny "#"], "denied", isLeft),
  ([Allow "tmp/#", Deny "#"], "tmp/ok", isRight),
  ([Deny "tmp/#", Allow "#"], "tmp/denied", isLeft),
  ([Deny "tmp/#", Allow "#"], "allowed", isRight)
  ]
  where
    aTest (a,t,f) = assertBool (show (a, t)) $ f (authTopic t a)

roundTrips :: (Eq a, Show a, Arbitrary a) => (a -> b) -> (b -> a) -> a -> Property
roundTrips t f = f.t >>= (===)

tests :: [TestTree]
tests = [
  testCase "config files" testConfigFiles,
  testCase "ACLs" testACLs,

  testCase "subtree" testSubTree,
  testGroup "subtree properties" [
      testProperty "flatten/fromList" $ roundTrips (filter (not.null . snd) . Sub.flatten @(SubTree (Set Int))) Sub.fromList,
      testProperties "functor" (unbatch $ functor (undefined :: SubTree ([Int], Int, Int))),
      testProperties "foldable" (unbatch $ foldable (undefined :: SubTree (Sum Int, Sum Int, Sum Int, Sum Int, Sum Int))),
      testProperties "traversable" (unbatch $ traversable (undefined :: SubTree (Sum Int, Sum Int, Sum Int))),
      testProperties "semigroup" (unbatch $ semigroup (undefined :: SubTree [Int], undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: SubTree [Int]))
      ],

  testGroup "listener properties" [
      testProperties "semigroup" (unbatch $ semigroup (undefined :: ListenerOptions, undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: ListenerOptions))
      ]
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
