{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE ScopedTypeVariables #-}

module MQTTD.GCStats where

import           Cleff
import qualified Data.ByteString.Lazy as BL
import           GHC.Stats

import           MQTTD.Util

hasGCStats :: MonadIO m => m Bool
hasGCStats = liftIO getRTSStatsEnabled

pubGCStats :: forall es. IOE :> es => (BL.ByteString -> BL.ByteString -> Eff es ()) -> Eff es ()
pubGCStats pubBS = do
  RTSStats{..} <- liftIO getRTSStats

  pub "gcs" gcs
  pub "allocated" allocated_bytes
  pub "gc_elapsed" gc_elapsed_ns

  let GCDetails{..} = gc
  pub "live_bytes" gcdetails_live_bytes
  pub "large_bytes" gcdetails_large_objects_bytes
  pub "compact_bytes" gcdetails_compact_bytes
  pub "mem_in_use" gcdetails_mem_in_use_bytes

  where
    pre = ("$SYS/broker/runtime/" <>)
    pub :: Show a => BL.ByteString -> a -> Eff es ()
    pub k = pubBS (pre k) . textToBL . tshow
