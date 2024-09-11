module MQTTD.Retention where

import           Cleff
import           Control.Concurrent.STM (TVar, modifyTVar', newTVarIO, readTVar)
import           Control.Lens
import           Control.Monad          (unless, when, (<=<))
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Time.Clock        (UTCTime (..), addUTCTime, diffUTCTime, getCurrentTime)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (atomically, readTVarIO)

import           MQTTD.DB
import           MQTTD.Logging
import           MQTTD.Stats
import           MQTTD.Types
import           MQTTD.Util
import qualified Scheduler
import           Scheduler              (QueueID (..))

data Retainer = Retainer {
  _store   :: TVar (Map BLTopic Retained),
  _qrunner :: Scheduler.QueueRunner BLTopic
  }

newRetainer :: MonadIO m => m Retainer
newRetainer = Retainer <$> liftIO (newTVarIO mempty) <*> Scheduler.newRunner

cleanRetainer :: [IOE, Stats, DB, LogFX] :>> es => Retainer -> Eff es ()
cleanRetainer Retainer{..} = Scheduler.run cleanup _qrunner
    where
      cleanup k = do
        now <- liftIO getCurrentTime
        unless (isSys k) $ logDbgL ["Probably removing persisted item: ", tshow k]
        jk <- atomically $ do
          r <- (_retainExp <=< Map.lookup k) <$> readTVar _store
          when ((_qidT <$> r) < Just now) $ modifyTVar' _store (Map.delete k)
          if (_qidT <$> r) < Just now then pure (Just k) else pure Nothing
        justM deleteRetained jk

isSys :: BLTopic -> Bool
isSys = ("$SYS/" `BL.isPrefixOf`)

retain :: [IOE, Stats, DB, LogFX] :>> es => T.PublishRequest -> Retainer -> Eff es ()
retain T.PublishRequest{_pubRetain=False} _ = pure ()
retain T.PublishRequest{_pubTopic,_pubBody=""} Retainer{..} = do
  deleteRetained _pubTopic
  atomically $ modifyTVar' _store (Map.delete _pubTopic)
retain pr@T.PublishRequest{..} Retainer{..} = do
  now <- liftIO getCurrentTime
  ss <- getStatStore
  unless (isSys _pubTopic) $ logDbgL ["Persisting ", tshow _pubTopic]
  let e = pr ^? properties . folded . _PropMessageExpiryInterval . to (absExp now)
      ret = Retained now Nothing pr
  ret' <- atomically $ do
    qid <- traverse (\t -> Scheduler.enqueueSTM ss t _pubTopic _qrunner) e
    let ret' = ret{_retainExp=qid}
    existing <- Map.lookup _pubTopic <$> readTVar _store
    justM (\i -> Scheduler.cancelSTM ss i _qrunner) (_retainExp =<< existing)
    modifyTVar' _store (Map.insert _pubTopic ret')
    pure ret'
  unless (isSys _pubTopic) $ storeRetained ret'

restoreRetained :: [IOE, Stats, DB] :>> es => Retainer -> Eff es ()
restoreRetained Retainer{..} = do
  ss <- getStatStore
  mapM_ (keep ss) =<< loadRetained
  where
    keep ss r@Retained{..} = do
      let top = r ^. retainMsg . pubTopic
      atomically $ do
        qid <- traverse (\(QueueID t _) -> Scheduler.enqueueSTM ss t top _qrunner) _retainExp
        modifyTVar' _store (Map.insert top r{_retainExp=qid})

matchRetained :: MonadIO m => Retainer -> T.Filter -> m [T.PublishRequest]
matchRetained Retainer{..} f = do
  now <- liftIO getCurrentTime
  fmap (adj now) . filter match . Map.elems <$> readTVarIO _store

  where
    match x = maybe False (T.match f) (T.mkTopic . blToText . T._pubTopic . _retainMsg $ x)
    adj _ Retained{_retainExp=Nothing, _retainMsg} = _retainMsg
    adj now Retained{_retainExp=Just (QueueID e _), _retainMsg} =
      _retainMsg & properties . traversed . _PropMessageExpiryInterval .~ relExp now e

absExp :: Integral a => UTCTime -> a -> UTCTime
absExp now secs = addUTCTime (fromIntegral secs) now

relExp :: Integral p => UTCTime -> UTCTime -> p
relExp now e = fst . properFraction $ diffUTCTime e now
