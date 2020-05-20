module MQTTD.Retention where

import           Control.Concurrent.STM (TVar, modifyTVar', newTVarIO, readTVar)
import           Control.Lens
import           Control.Monad          (when, (<=<))
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..), logDebugN)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Time.Clock        (UTCTime (..), addUTCTime, diffUTCTime, getCurrentTime)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..), atomically, readTVarIO)

import           MQTTD.Util
import qualified Scheduler

data Retained = Retained {
  _retainTS  :: UTCTime,
  _retainExp :: Maybe UTCTime,
  _retainMsg :: T.PublishRequest
  } deriving Show

data Retainer = Retainer {
  _store   :: TVar (Map BL.ByteString Retained),
  _qrunner :: Scheduler.QueueRunner BL.ByteString
  }

newRetainer :: MonadIO m => m Retainer
newRetainer = Retainer <$> liftIO (newTVarIO mempty) <*> Scheduler.newRunner

cleanRetainer :: (MonadLogger m, MonadUnliftIO m) => Retainer -> m ()
cleanRetainer Retainer{..} = Scheduler.run cleanup _qrunner
    where
      cleanup k = do
        now <- liftIO getCurrentTime
        logDebugN ("Probably removing persisted item: " <> tshow k)
        atomically $ do
          r <- (_retainExp <=< Map.lookup k) <$> readTVar _store
          when (r < Just now) $ modifyTVar' _store (Map.delete k)

retain :: (MonadLogger m, MonadIO m) => T.PublishRequest -> Retainer -> m ()
retain T.PublishRequest{_pubRetain=False} _ = pure ()
retain T.PublishRequest{_pubTopic,_pubBody=""} Retainer{..} =
  atomically $ modifyTVar' _store (Map.delete _pubTopic)
retain pr@T.PublishRequest{..} Retainer{..} = do
  now <- liftIO getCurrentTime
  logDebugN ("Persisting " <> tshow _pubTopic)
  let e = pr ^? properties . folded . _PropMessageExpiryInterval . to (absExp now)
  atomically $ modifyTVar' _store (Map.insert _pubTopic (Retained now e pr))
  justM (\t -> Scheduler.enqueue t _pubTopic _qrunner) e

    where absExp now secs = addUTCTime (fromIntegral secs) now

matchRetained :: MonadIO m => Retainer -> T.Filter -> m [T.PublishRequest]
matchRetained Retainer{..} f = do
  now <- liftIO getCurrentTime
  fmap (adj now) . filter match . Map.elems <$> readTVarIO _store

  where
    match = T.match f . blToText . T._pubTopic . _retainMsg
    adj _ Retained{_retainExp=Nothing, _retainMsg} = _retainMsg
    adj now Retained{_retainExp=Just e, _retainMsg} =
      _retainMsg & properties . traversed . _PropMessageExpiryInterval .~ til
      where
        til = fst . properFraction $ diffUTCTime e now
