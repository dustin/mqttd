module MQTTD.Stats where

import           Control.Concurrent.STM (STM, TBQueue, TVar, check, flushTBQueue, isEmptyTBQueue, modifyTVar')
import           Control.Monad          (forever)
import           Control.Monad.IO.Class (MonadIO (..))
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
  deriving (Eq, Show, Ord, Enum)

statKeyName :: StatKey -> BL.ByteString
statKeyName StatMsgSent = "$SYS/broker/messages/sent"
statKeyName StatMsgRcvd = "$SYS/broker/messages/received"
statKeyName StatBytesRcvd = "$SYS/broker/bytes/received"
statKeyName StatBytesSent = "$SYS/broker/bytes/sent"
statKeyName StatStoreTransactions = "$SYS/broker/store/transactions"
statKeyName StatStoreOperations = "$SYS/broker/store/operations"

type Increment = (StatKey, Int)

data StatStore = StatStore {
  _stats_queue :: TBQueue Increment,
  _stats_map   :: TVar (Map StatKey Int)
  }

newStatStore :: MonadIO m => m StatStore
newStatStore = StatStore <$> newTBQueueIO 100 <*> newTVarIO mempty

applyStats :: MonadIO m => StatStore -> m ()
applyStats StatStore{..} = forever . atomically $ do
  check . not =<< isEmptyTBQueue _stats_queue
  todo <- flushTBQueue _stats_queue
  modifyTVar' _stats_map (Map.unionWith (+) (Map.fromListWith (+) todo))

incrementStatSTM :: StatKey -> Int -> StatStore -> STM ()
incrementStatSTM k i StatStore{..} = writeTBQueue _stats_queue (k, i)

incrementStat :: MonadIO m => StatKey -> Int -> StatStore -> m ()
incrementStat k i s = atomically $ incrementStatSTM k i s

retrieveStats :: MonadIO m => StatStore -> m (Map StatKey Int)
retrieveStats StatStore{..} = readTVarIO _stats_map
