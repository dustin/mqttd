{-# LANGUAGE DeriveTraversable #-}

module MQTTD.SubTree (
  SubTree, empty, modify, add, addWith, find, findMap, flatten, fromList,
  ) where

import           Data.List.NonEmpty      (NonEmpty (..), (<|))
import qualified Data.List.NonEmpty      as NE
import           Data.Map.Strict         (Map)
import qualified Data.Map.Strict         as Map
import           Data.Maybe              (maybeToList)
import           Data.Semigroup          (sconcat)
import           Data.Text               (isPrefixOf)

import           Network.MQTT.Topic      (Filter, Topic, mkFilter, split, unTopic)

-- | MQTT Topic Subscription tree.
data SubTree a = SubTree {
  subs       :: Maybe a
  , children :: Map Filter (SubTree a)
  } deriving (Show, Eq, Functor, Foldable, Traversable)

instance Semigroup a => Semigroup (SubTree a) where
  a <> b = SubTree (subs a <> subs b) (Map.unionWith (<>) (children a) (children b))

-- | An empty SubTree.
--
-- This exists in addition to `mempty` so one can create a SubTree for
-- a values that are not monoidal.
empty :: SubTree a
empty = SubTree Nothing mempty

instance Monoid a => Monoid (SubTree a) where
  mempty = empty

-- | Modify a subscription for the given filter.
--
-- This will create structure along the path and will not clean up any
-- unused structure.
modify :: Filter -> (Maybe a -> Maybe a) -> SubTree a -> SubTree a
modify top f = go (split top)
  where
    go [] n@SubTree{..}     = n{subs=f subs}
    go (x:xs) n@SubTree{..} = n{children=Map.alter (fmap (go xs) . maybe (Just empty) Just) x children}

-- | Add a value at the given filter path.
add :: Monoid a => Filter -> a -> SubTree a -> SubTree a
add top = addWith top (<>)

-- | Add a value at the given filter path with the given collision function.
addWith :: Monoid a => Filter -> (a -> a -> a) -> a -> SubTree a -> SubTree a
addWith top f i = modify top (fmap (f i) . maybe (Just mempty) Just)

-- | Find all matching subscribers
findMap :: Monoid m => Topic -> (a -> m) -> SubTree a -> m
findMap top f = go mwc (maybe [] split (mkFilter . unTopic $ top))
  where
    go _ [] SubTree{subs} = maybe mempty f subs
    go d (x:xs) SubTree{children} = maybe mempty (go id xs)                 (Map.lookup x children)
                                    <> maybe mempty (go id xs)              (d $ Map.lookup "+" children)
                                    <> maybe mempty (maybe mempty f . subs) (d $ Map.lookup "#" children)
    mwc deeper
      | "$" `isPrefixOf` (unTopic top) = Nothing
      | otherwise = deeper

-- | Find subscribers of a given topic.
find :: Monoid a => Topic -> SubTree a -> a
find top = findMap top id

-- | flatten a SubTree to a list of (topic,a) pairs.
flatten :: SubTree a -> [(Filter, a)]
flatten = Map.foldMapWithKey (\k sn -> go (k:|[]) sn) . children
  where
    go ks SubTree{..} = [(cat ks, s) | s <- maybeToList subs]
                        <> Map.foldMapWithKey (\k sn -> go (k <| ks) sn) children
    cat = sconcat . NE.reverse

-- | Construct a SubTree from a list of filters and subscribers (assuming monoidal values).
fromList :: Monoid a => [(Filter, a)] -> SubTree a
fromList = foldr (uncurry add) mempty
