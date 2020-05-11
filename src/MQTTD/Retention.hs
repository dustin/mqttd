module MQTTD.Retention where

import           Control.Concurrent.STM (TVar, modifyTVar', newTVarIO, readTVar)
import           Control.Lens
import           Control.Monad          (when)
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..), logDebugN)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Time.Clock        (UTCTime (..), addUTCTime, diffUTCTime, getCurrentTime)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..), atomically)

import           MQTTD.Util
import qualified Scheduler

data Retained = Retained {
  _retainTS  :: UTCTime,
  _retainExp :: Maybe UTCTime,
  _retainMsg :: T.PublishRequest
  } deriving Show

data Persistence = Persistence {
  _store   :: TVar (Map BL.ByteString Retained),
  _qrunner :: Scheduler.QueueRunner BL.ByteString
  }

newPersistence :: MonadIO m => m Persistence
newPersistence = Persistence <$> liftIO (newTVarIO mempty) <*> Scheduler.newRunner

cleanPersistence :: (MonadLogger m, MonadUnliftIO m) => Persistence -> m ()
cleanPersistence Persistence{..} = Scheduler.run cleanup _qrunner
    where
      cleanup k = do
        now <- liftIO getCurrentTime
        logDebugN ("Probably removing persisted item: " <> tshow k)
        atomically $ do
          r <- (_retainExp =<<) . Map.lookup k <$> readTVar _store
          when (r < Just now) $ modifyTVar' _store (Map.delete k)

retain :: (MonadLogger m, MonadIO m) => T.PublishRequest -> Persistence -> m ()
retain T.PublishRequest{_pubRetain=False} _ = pure ()
retain T.PublishRequest{_pubTopic,_pubBody=""} Persistence{..} =
  atomically $ modifyTVar' _store (Map.delete _pubTopic)
retain pr@T.PublishRequest{..} Persistence{..} = do
  now <- liftIO getCurrentTime
  logDebugN ("Persisting " <> tshow _pubTopic)
  let e = pr ^? properties . folded . _PropMessageExpiryInterval . to (absExp now)
  atomically $ modifyTVar' _store (Map.insert _pubTopic (Retained now e pr))
  justM (\t -> Scheduler.enqueue t _pubTopic _qrunner) e

    where absExp now secs = addUTCTime (fromIntegral secs) now

matchRetained :: MonadIO m => Persistence -> T.Filter -> m [T.PublishRequest]
matchRetained Persistence{..} f = do
  now <- liftIO getCurrentTime
  fmap (adj now) . filter match . Map.elems <$> atomically (readTVar _store)

  where
    match = T.match f . blToText . T._pubTopic . _retainMsg
    adj _ Retained{_retainExp=Nothing, _retainMsg} = _retainMsg
    adj now Retained{_retainExp=Just e, _retainMsg} =
      _retainMsg & properties . traversed . _PropMessageExpiryInterval .~ til
      where
        til = fst . properFraction $ diffUTCTime e now
