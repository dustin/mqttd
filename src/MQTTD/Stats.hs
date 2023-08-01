{-# LANGUAGE FlexibleInstances #-}

module MQTTD.Stats where

import           Control.Concurrent.STM (STM, TBQueue, TVar, check, flushTBQueue, isEmptyTBQueue, modifyTVar')
import           Control.Monad          (forever)
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Reader   (ReaderT (..), ask)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           UnliftIO               (atomically, newTBQueueIO, newTVarIO, readTVarIO, writeTBQueue)

data StatKey = StatMsgSent
             | StatMsgRcvd
             | StatBytesSent
             | StatBytesRcvd
             | StatStoreTransactions
             | StatStoreOperations
             | StatsActionQueued
             | StatsActionCanceled
             | StatsActionExecuted
  deriving (Eq, Show, Ord, Enum)

statKeyName :: StatKey -> BL.ByteString
statKeyName StatMsgSent           = "$SYS/broker/messages/sent"
statKeyName StatMsgRcvd           = "$SYS/broker/messages/received"
statKeyName StatBytesRcvd         = "$SYS/broker/bytes/received"
statKeyName StatBytesSent         = "$SYS/broker/bytes/sent"
statKeyName StatStoreTransactions = "$SYS/broker/store/transactions"
statKeyName StatStoreOperations   = "$SYS/broker/store/operations"
statKeyName StatsActionQueued     = "$SYS/broker/actions/queued"
statKeyName StatsActionCanceled   = "$SYS/broker/actions/canceled"
statKeyName StatsActionExecuted   = "$SYS/broker/actions/executed"

type Increment = (StatKey, Int)

data StatStore = StatStore {
  _stats_queue :: TBQueue Increment,
  _stats_map   :: TVar (Map StatKey Int)
  }

class HasStats m where
  statStore :: m StatStore

instance Monad m => HasStats (ReaderT StatStore m) where
  statStore = ask

newStatStore :: MonadIO m => m StatStore
newStatStore = StatStore <$> newTBQueueIO 100 <*> newTVarIO mempty

applyStats :: MonadIO m => StatStore -> m ()
applyStats StatStore{..} = forever . atomically $ do
  check . not =<< isEmptyTBQueue _stats_queue
  todo <- flushTBQueue _stats_queue
  modifyTVar' _stats_map (Map.unionWith (+) (Map.fromListWith (+) todo))

incrementStatSTM :: StatKey -> Int -> StatStore -> STM ()
incrementStatSTM k i StatStore{..} = writeTBQueue _stats_queue (k, i)

incrementStat :: (MonadIO m, HasStats m) => StatKey -> Int -> m ()
incrementStat k i = statStore >>= atomically . incrementStatSTM k i

retrieveStats :: (MonadIO m, HasStats m) => m (Map StatKey Int)
retrieveStats = statStore >>= \StatStore{..} -> readTVarIO _stats_map
