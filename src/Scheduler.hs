module Scheduler (QueueID(..), Clock(..), TimedQueue, add, ready, next, QueueRunner, newRunner, enqueue, enqueueSTM, cancelSTM, run, runOnce) where

import           Control.Concurrent.STM (STM, TVar, check, modifyTVar', newTVarIO, orElse, readTVar, registerDelay,
                                         writeTVar)
import           Control.Monad          (forever)
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..))
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromMaybe)
import           Data.Time.Clock        (NominalDiffTime, UTCTime (..), diffUTCTime, getCurrentTime)
import           UnliftIO               (atomically)

import           Data.Foldable          (Foldable (fold), traverse_)
import           MQTTD.Logging
import           MQTTD.Stats
import           MQTTD.Util

-- This bit is just about managing a schedule of tasks.

data QueueID = QueueID { _qidT :: !UTCTime, _qidI :: !Int }
  deriving (Show, Eq, Ord)

type TimedQueue a = Map QueueID [a]

add :: Ord a => QueueID -> a -> TimedQueue a -> TimedQueue a
add k v = Map.insertWith (<>) k [v]

ready :: UTCTime -> TimedQueue a -> ([a], TimedQueue a)
ready now tq = (fold rm <> fromMaybe [] mm, q)
  where
    (rm, mm, q) = Map.splitLookup (QueueID now 0) tq

next :: TimedQueue a -> Maybe UTCTime
next = fmap (_qidT . fst) . Map.lookupMin

-- The actual queue machination is below.

data QueueRunner a = QueueRunner {
  _tq   :: !(TVar (TimedQueue a)),
  _ider :: !(TVar Int)
  }

newRunner :: MonadIO m => m (QueueRunner a)
newRunner = QueueRunner <$> liftIO (newTVarIO mempty) <*> liftIO (newTVarIO 0)

enqueue :: (HasStats m, Ord a, MonadIO m) => UTCTime -> a -> QueueRunner a -> m QueueID
enqueue t a qr = statStore >>= \ss -> atomically $ enqueueSTM ss t a qr

enqueueSTM :: Ord a => StatStore -> UTCTime -> a -> QueueRunner a -> STM QueueID
enqueueSTM ss t a QueueRunner{..} = do
  incrementStatSTM StatsActionQueued 1 ss
  nextId <- modifyTVarRet _ider succ
  let k = QueueID t nextId
  modifyTVar' _tq (add k a)
  pure k

cancelSTM :: Ord a => StatStore -> QueueID -> QueueRunner a -> STM ()
cancelSTM ss qid QueueRunner{..} = do
  incrementStatSTM StatsActionCanceled 1 ss
  modifyTVar' _tq (Map.delete qid)

-- | Run forever.
run :: (HasStats m, MonadLogger m, MonadIO m) => (a -> m ()) -> QueueRunner a -> m ()
run action = forever . runOnce sysClock action

data Clock = Clock {
  getTime :: IO UTCTime,
  delay   :: Int -> IO (TVar Bool)
}

sysClock :: Clock
sysClock = Clock getCurrentTime registerDelay

blockUntilReady :: MonadIO m => Clock -> QueueRunner a -> m ()
blockUntilReady Clock{..} QueueRunner{..} = liftIO $ do
  now <- getTime
  mnext <- atomically (next <$> readTVar _tq)
  timedOut <- case diffTimeToMicros . (`diffUTCTime` now) <$> mnext of
                Nothing -> newTVarIO False
                Just d  -> delay d
  atomically $ (check =<< readTVar timedOut) `orElse` (check . (/= mnext) . next =<< readTVar _tq)

runReady :: MonadIO m => Clock -> (a -> m ()) -> QueueRunner a -> m [a]
runReady Clock{..} action QueueRunner{..} = do
  now <- liftIO getTime
  todo <- atomically $ do
    (todo, nq) <- ready now <$> readTVar _tq
    writeTVar _tq nq
    pure todo
  traverse_ action todo
  pure todo

-- | Block until an item might be ready and then run (and remove) all
-- ready items.  This will sometimes run 0 items.  It shouldn't ever
-- run any items that are scheduled for the future, and it shouldn't
-- forget any items that are ready.
runOnce :: (HasStats m, MonadLogger m, MonadIO m) => Clock -> (a -> m ()) -> QueueRunner a -> m ()
runOnce c action qr = do
  blockUntilReady c qr
  done <- runReady c action qr
  incrementStat StatsActionExecuted (length done)
  logDbgL ["Running ", (tshow . length) done, " actions"]

-- utilities

diffTimeToMicros :: NominalDiffTime -> Int
diffTimeToMicros dt = let (s, f) = properFraction dt
                          (μ, _) = properFraction (f * 1000000) in
                        s * 1000000 + μ
