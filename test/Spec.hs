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
import           Data.Text                (Text)
import qualified Data.Text                as Text

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
    ATopic . Text.pack . intercalate "/" <$> vectorOf n seg
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

newtype Topic = Topic [Text] deriving (Show, Eq)

instance Arbitrary Topic where
  arbitrary = Topic <$> someSegs
    where someSegs = choose (1,8) >>= flip vectorOf aSeg
          aSeg = do
            n <- choose (1,8)
            Text.pack <$> vectorOf n aValidChar
          aValidChar = elements (['A'..'Z'] <> ['a'..'z'] <> ['0'..'9'])

  shrink (Topic x) = fmap Topic . shrinkList shrinkWord $ x
    where shrinkWord = fmap Text.pack . shrink . Text.unpack

data MatchingTopic = MatchingTopic Topic Topic deriving Eq

instance Show MatchingTopic where
  show (MatchingTopic t m) = concat ["MatchingTopic ",
                                     (Text.unpack $ topicPath t), " -> ", (Text.unpack $ topicPath m)]

instance Arbitrary MatchingTopic where
  arbitrary = do
    t@(Topic tsegs) <- arbitrary
    reps <- vectorOf (length tsegs) (elements [id, const "+", const "#"])
    let m = zipWith ($) reps tsegs
    pure $ MatchingTopic t (Topic $ clean m)
      where
        clean []       = []
        clean ("#":xs) = ["#"]
        clean (x:xs)   = x : clean xs

propSubTreeMapping :: [MatchingTopic] -> Bool
propSubTreeMapping matches = all (\(t, m) -> m `elem` Sub.find t st) tp
  where
    tp = [(topicPath t, topicPath m) | (MatchingTopic t m) <- matches]
    st = foldr (\(_,t) -> Sub.add t [t]) mempty tp

topicPath :: Topic -> Text
topicPath (Topic t) = Text.intercalate "/" t

roundTrips :: (Eq a, Show a, Arbitrary a) => (a -> b) -> (b -> a) -> a -> Property
roundTrips t f = f.t >>= (===)

tests :: [TestTree]
tests = [
  testCase "config files" testConfigFiles,
  testCase "ACLs" testACLs,

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

  testGroup "listener properties" [
      testProperties "semigroup" (unbatch $ semigroup (undefined :: ListenerOptions, undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: ListenerOptions))
      ]
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
