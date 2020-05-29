{-# LANGUAGE RankNTypes #-}

module MQTTD.GCStats where

import           Control.Monad.IO.Class (MonadIO (..))
import qualified Data.ByteString.Lazy   as BL
import           GHC.Stats

import           MQTTD.Types
import           MQTTD.Util

hasGCStats :: MonadIO m => m Bool
hasGCStats = liftIO getRTSStatsEnabled

pubGCStats :: forall m. PublishConstraint m => (BL.ByteString -> BL.ByteString -> m ()) -> m ()
pubGCStats pubBS = do
  RTSStats{..} <- liftIO getRTSStats

  pub "gcs" gcs
  pub "allocated" allocated_bytes
  pub "gc_elapsed" (gc_elapsed_ns `div` 1000000000) -- milliseconds

  let GCDetails{..} = gc
  pub "live_bytes" gcdetails_live_bytes
  pub "large_bytes" gcdetails_large_objects_bytes
  pub "compact_bytes" gcdetails_compact_bytes
  pub "mem_in_use" gcdetails_mem_in_use_bytes

  where
    pre = ("$SYS/broker/runtime/" <>)
    pub k = pubBS (pre k) . textToBL . tshow
