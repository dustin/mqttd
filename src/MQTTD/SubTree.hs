module MQTTD.SubTree where

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Text       (Text, splitOn)

data SubTree a = SubTree {
  subs     :: [a],
  children :: Map Text (SubTree a)
  } deriving (Show, Eq)

instance Functor SubTree where
  fmap f SubTree{..} = SubTree (f <$> subs) ((fmap.fmap) f children)

instance Foldable SubTree where
  foldMap f SubTree{..} = foldMap f subs <> (foldMap.foldMap) f children

modifySub :: Text -> ([a] -> [a]) -> SubTree a -> SubTree a
modifySub top f = go (splitOn "/" top)
  where
    go [] n@SubTree{..} = n{subs=f subs}
    go (x:xs) n@SubTree{..} = n{children=Map.alter (fmap (go xs) . withChild) x children}
      where
        withChild Nothing   = Just (SubTree mempty mempty)
        withChild (Just n') = Just n'

addSub :: Text -> a -> SubTree a -> SubTree a
addSub top i = modifySub top (i:)

unsub :: Eq a => Text -> a -> SubTree a -> SubTree a
unsub top i = modifySub top (filter (/= i))

findSubd :: Text -> SubTree a -> [a]
findSubd top = go (splitOn "/" top)
  where
    go [] SubTree{subs} = subs
    go (x:xs) SubTree{children} = maybe [] (go xs) (Map.lookup x children)
                               <> maybe [] (go xs) (Map.lookup "+" children)
                               <> maybe [] subs    (Map.lookup "#" children)
