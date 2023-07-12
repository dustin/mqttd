{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TypeApplications #-}

module SubTree where

import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           Control.Applicative      (liftA2)
import           Data.List                (sort)
import           Data.Monoid              (Sum (..))
import           Data.Set                 (Set)

import           Network.MQTT.Arbitrary   (arbitraryMatchingTopic, arbitraryTopic)
import           Network.MQTT.Topic       (Filter, Topic, toFilter)

import           MQTTD.SubTree            (SubTree)
import qualified MQTTD.SubTree            as Sub

instance Eq a => EqProp (SubTree a) where (=-=) = eq

instance (Monoid a, Arbitrary a, Eq a) => Arbitrary (SubTree a) where
  arbitrary = do
    filters <- resize 20 $ listOf1 (toFilter <$> arbitraryTopic ['a'..'d'] (1,7) (1,3))
    subbers <- resize 20 $ listOf1 arbitrary
    Sub.fromList <$> resize 50 (listOf1 (liftA2 (,) (elements filters) (elements subbers)))

  shrink = fmap Sub.fromList . shrinkList (const []) . Sub.flatten

unit_SubTree :: Assertion
unit_SubTree =
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

roundTrips :: (Eq a, Show a, Arbitrary a) => (a -> b) -> (b -> a) -> a -> Property
roundTrips t f = f.t >>= (===)

test_SubTree :: [TestTree]
test_SubTree = [
  testProperty "flatten/fromList" $ roundTrips (filter (not.null . snd) . Sub.flatten @(SubTree (Set Int))) Sub.fromList,
  testProperty "finding" propSubTreeMapping,
  testProperties "functor" (unbatch $ functor (undefined :: SubTree ([Int], Int, Int))),
  testProperties "foldable" (unbatch $ foldable (undefined :: SubTree (Sum Int, Sum Int, Sum Int, Sum Int, Sum Int))),
  testProperties "traversable" (unbatch $ traversable (undefined :: SubTree (Maybe (Sum Int), Maybe (Sum Int), Int, Sum Int))),
  testProperties "semigroup" (unbatch $ semigroup (undefined :: SubTree [Int], undefined :: Int)),
  testProperties "monoid" (unbatch $ monoid (undefined :: SubTree [Int]))
  ]
