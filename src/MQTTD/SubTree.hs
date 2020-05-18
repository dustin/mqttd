module MQTTD.SubTree where

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Text       (Text, intercalate, splitOn)

data SubTree a = SubTree {
  subs     :: a,
  children :: Map Text (SubTree a)
  } deriving (Show, Eq)

instance Functor SubTree where
  fmap f SubTree{..} = SubTree (f subs) ((fmap.fmap) f children)

instance Foldable SubTree where
  foldMap f SubTree{..} = f subs <> (foldMap.foldMap) f children

instance Traversable SubTree where
  traverse f SubTree{..} = SubTree <$> f subs <*> (traverse.traverse) f children

instance Semigroup a => Semigroup (SubTree a) where
  a <> b = SubTree (subs a <> subs b) (Map.unionWith (<>) (children a) (children b))

instance Monoid a => Monoid (SubTree a) where
  mempty = SubTree mempty mempty

modify :: Monoid a => Text -> (a -> a) -> SubTree a -> SubTree a
modify top f = go (splitOn "/" top)
  where
    go [] n@SubTree{..}     = n{subs=f subs}
    go (x:xs) n@SubTree{..} = n{children=Map.alter (fmap (go xs) . maybe (Just mempty) Just) x children}

add :: Monoid a => Text -> a -> SubTree a -> SubTree a
add top i = modify top (i<>)

find :: Monoid a => Text -> SubTree a -> a
find top = go (splitOn "/" top)
  where
    go [] SubTree{subs} = subs
    go (x:xs) SubTree{children} = maybe mempty (go xs) (Map.lookup x children)
                               <> maybe mempty (go xs) (Map.lookup "+" children)
                               <> maybe mempty subs    (Map.lookup "#" children)

-- | flatten a SubTree to a list of (topic,a) pairs.
-- Note this will return 'mempty' a where there are none.
flatten :: SubTree a -> [(Text, a)]
flatten = Map.foldMapWithKey (\k sn -> go [k] sn) . children
  where
    go ks SubTree{..} = [(intercalate "/" (reverse ks), subs)]
                        <> Map.foldMapWithKey (\k sn -> go (k:ks) sn) children

fromList :: Monoid a => [(Text, a)] -> SubTree a
fromList = foldr (uncurry add) mempty
