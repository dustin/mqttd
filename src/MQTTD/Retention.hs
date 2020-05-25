module MQTTD.Retention where

import           Control.Concurrent.STM (TVar, modifyTVar', newTVarIO, readTVar)
import           Control.Lens
import           Control.Monad          (when, (<=<))
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..), logDebugN)
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Time.Clock        (UTCTime (..), addUTCTime, diffUTCTime, getCurrentTime)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..), atomically, readTVarIO)

import           MQTTD.DB
import           MQTTD.Types
import           MQTTD.Util
import qualified Scheduler

data Retainer = Retainer {
  _store   :: TVar (Map BLTopic Retained),
  _qrunner :: Scheduler.QueueRunner BLTopic
  }

newRetainer :: MonadIO m => m Retainer
newRetainer = Retainer <$> liftIO (newTVarIO mempty) <*> Scheduler.newRunner

cleanRetainer :: (MonadLogger m, HasDBConnection m, MonadUnliftIO m) => Retainer -> m ()
cleanRetainer Retainer{..} = Scheduler.run cleanup _qrunner
    where
      cleanup k = do
        now <- liftIO getCurrentTime
        logDebugN ("Probably removing persisted item: " <> tshow k)
        jk <- atomically $ do
          r <- (_retainExp <=< Map.lookup k) <$> readTVar _store
          when (r < Just now) $ modifyTVar' _store (Map.delete k)
          if r < Just now then pure (Just k) else pure Nothing
        justM deleteRetained jk

retain :: (MonadLogger m, HasDBConnection m, MonadIO m) => T.PublishRequest -> Retainer -> m ()
retain T.PublishRequest{_pubRetain=False} _ = pure ()
retain T.PublishRequest{_pubTopic,_pubBody=""} Retainer{..} = do
  deleteRetained _pubTopic
  atomically $ modifyTVar' _store (Map.delete _pubTopic)
retain pr@T.PublishRequest{..} Retainer{..} = do
  now <- liftIO getCurrentTime
  logDebugN ("Persisting " <> tshow _pubTopic)
  let e = pr ^? properties . folded . _PropMessageExpiryInterval . to (absExp now)
      ret = (Retained now e pr)
  atomically $ modifyTVar' _store (Map.insert _pubTopic ret)
  storeRetained ret
  justM (\t -> Scheduler.enqueue t _pubTopic _qrunner) e

restoreRetained :: (MonadIO m, HasDBConnection m) => Retainer -> m ()
restoreRetained Retainer{..} = mapM_ keep =<< loadRetained
  where
    keep r@Retained{..} = do
      let top = r ^. retainMsg . pubTopic
      atomically $ modifyTVar' _store (Map.insert top r)
      justM (\t -> Scheduler.enqueue t top _qrunner) _retainExp

matchRetained :: MonadIO m => Retainer -> T.Filter -> m [T.PublishRequest]
matchRetained Retainer{..} f = do
  now <- liftIO getCurrentTime
  fmap (adj now) . filter match . Map.elems <$> readTVarIO _store

  where
    match = T.match f . blToText . T._pubTopic . _retainMsg
    adj _ Retained{_retainExp=Nothing, _retainMsg} = _retainMsg
    adj now Retained{_retainExp=Just e, _retainMsg} =
      _retainMsg & properties . traversed . _PropMessageExpiryInterval .~ relExp now e

absExp :: Integral a => UTCTime -> a -> UTCTime
absExp now secs = addUTCTime (fromIntegral secs) now

relExp :: Integral p => UTCTime -> UTCTime -> p
relExp now e = fst . properFraction $ diffUTCTime e now
