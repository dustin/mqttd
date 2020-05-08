{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent     (ThreadId, threadDelay, throwTo)
import           Control.Concurrent.STM (STM, TBQueue, TVar, atomically, isFullTBQueue, modifyTVar', newTBQueue,
                                         newTVar, newTVarIO, readTVar, writeTBQueue, writeTVar)
import           Control.Lens
import           Control.Monad          (filterM, unless, when)
import           Control.Monad.Catch    (Exception, MonadCatch (..), MonadMask (..), MonadThrow (..))
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..), logDebugN)
import           Control.Monad.Reader   (MonadReader, ReaderT (..), asks, runReaderT)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import qualified Data.Set               as Set
import           Data.Text              (Text, pack)
import qualified Data.Text.Encoding     as TE
import           Data.Time.Clock        (UTCTime (..), addUTCTime, getCurrentTime)
import           Data.Word              (Word16)
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..))

import           Network.MQTT.Lens

import qualified Scheduler

data MQTTException = MQTTDuplicate deriving Show

instance Exception MQTTException

type PktQueue = TBQueue T.MQTTPkt
type ClientID = Int

data ConnectedClient = ConnectedClient {
  _clientConnReq :: T.ConnectRequest,
  _clientThread  :: ThreadId,
  _clientID      :: ClientID,
  _clientAliasIn :: TVar (Map Word16 BL.ByteString)
  }

makeLenses ''ConnectedClient

instance Show ConnectedClient where
  show ConnectedClient{..} = "ConnectedClient " <> show _clientConnReq

data Session = Session {
  _sessionClient  :: Maybe ConnectedClient,
  _sessionChan    :: PktQueue,
  _sessionSubs    :: TVar [T.Filter],
  _sessionExpires :: Maybe UTCTime,
  _sessionWill    :: Maybe T.LastWill
  }

makeLenses ''Session

data Env = Env {
  sessions    :: TVar (Map BL.ByteString Session),
  pktID       :: TVar Word16,
  clientIDGen :: TVar ClientID,
  queueRunner :: Scheduler.QueueRunner BL.ByteString
  }

newtype MQTTD m a = MQTTD
  { runMQTTD :: ReaderT Env m a
  } deriving (Applicative, Functor, Monad, MonadIO, MonadLogger,
              MonadCatch, MonadThrow, MonadMask, MonadReader Env, MonadFail)

instance MonadUnliftIO m => MonadUnliftIO (MQTTD m) where
  withRunInIO inner = MQTTD $ withRunInIO $ \run -> inner (run . runMQTTD)

liftSTM :: MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

runIO :: (MonadIO m, MonadLogger m) => Env -> MQTTD m a -> m a
runIO e m = runReaderT (runMQTTD m) e

newEnv :: MonadIO m => m Env
newEnv = liftIO $ Env <$> newTVarIO mempty <*> newTVarIO 1 <*> newTVarIO 0 <*> Scheduler.newRunner

seconds :: Num p => p -> p
seconds = (1000000 *)

sleep :: MonadIO m => Int -> m ()
sleep secs = liftIO (threadDelay (seconds secs))

nextID :: MonadIO m => MQTTD m Int
nextID = asks clientIDGen >>= \ig -> liftSTM $ modifyTVar' ig (+1) >> readTVar ig

runScheduler :: (MonadUnliftIO m, MonadLogger m) => MQTTD m ()
runScheduler = asks queueRunner >>= Scheduler.run expireSession

resolveAliasIn :: MonadIO m => Session -> T.PublishRequest -> m T.PublishRequest
resolveAliasIn Session{_sessionClient=Nothing} r = pure r
resolveAliasIn Session{_sessionClient=Just ConnectedClient{_clientAliasIn}} r =
  case r ^? properties . folded . _PropTopicAlias of
    Nothing -> pure r
    Just n  -> resolve n r

  where
    resolve n T.PublishRequest{_pubTopic} = do
      t <- liftSTM $ do
        when (_pubTopic /= "") $ modifyTVar' _clientAliasIn (Map.insert n _pubTopic)
        Map.findWithDefault "" n <$> readTVar _clientAliasIn
      pure $ r & pubTopic .~ t

findSubs :: MonadIO m => T.Topic -> MQTTD m [PktQueue]
findSubs t = do
  ss <- asks sessions
  sess <- Map.elems <$> liftSTM (readTVar ss)
  map _sessionChan <$> filterM subsTopic sess

  where
    subsTopic Session{_sessionSubs} = any (`T.match` t) <$> liftSTM (readTVar _sessionSubs)

subscribe :: MonadIO m => Session -> T.SubscribeRequest -> MQTTD m ()
subscribe Session{..} (T.SubscribeRequest _ topics _props) = do
  let new = map (blToText . fst) topics
  liftSTM $ modifyTVar' _sessionSubs (Set.toList . Set.fromList . (<> new))

modifySession :: MonadIO m => BL.ByteString -> (Session -> Maybe Session) -> MQTTD m ()
modifySession k f = asks sessions >>= \s -> liftSTM $ modifyTVar' s (Map.update f k)

registerClient :: MonadIO m => T.ConnectRequest -> ClientID -> ThreadId -> MQTTD m (Session, T.SessionReuse)
registerClient req@T.ConnectRequest{..} i o = do
  c <- asks sessions
  ai <- liftIO $ newTVarIO mempty
  let k = _connID
      nc = ConnectedClient req o i ai
  (o', x, ns) <- liftSTM $ do
    ch <- newTBQueue 1000
    m <- readTVar c
    subz <- newTVar mempty
    let s = Map.lookup k m
        o' = _sessionClient =<< s
        (ns, ruse) = maybeClean ch subz nc s
    writeTVar c (Map.insert k ns m)
    pure (o', ruse, ns)
  case o' of
    Nothing                  -> pure ()
    Just ConnectedClient{..} -> liftIO $ throwTo _clientThread MQTTDuplicate
  pure (ns, x)

    where
      maybeClean ch subz nc Nothing = (Session (Just nc) ch subz Nothing _lastWill, T.NewSession)
      maybeClean ch subz nc (Just s)
        | _cleanSession = (Session (Just nc) ch subz Nothing _lastWill, T.NewSession)
        | otherwise = (s{_sessionClient=Just nc,
                         _sessionExpires=Nothing,
                         _sessionWill=_lastWill}, T.ExistingSession)

expireSession :: (MonadLogger m, MonadUnliftIO m, MonadIO m) => BL.ByteString -> MQTTD m ()
expireSession k = do
  ss <- asks sessions
  possiblyCleanup =<< liftSTM (Map.lookup k <$> readTVar ss)

  where
    possiblyCleanup Nothing = pure ()
    possiblyCleanup (Just Session{_sessionClient=Just _}) = logDebugN (tshow k <> " is in use")
    possiblyCleanup (Just Session{_sessionClient=Nothing,
                                  _sessionExpires=Nothing}) = expireNow
    possiblyCleanup (Just Session{_sessionClient=Nothing,
                                  _sessionExpires=Just ex}) = do
      now <- liftIO getCurrentTime
      if ex > now
        then Scheduler.enqueue ex k =<< asks queueRunner
        else expireNow

    expireNow = do
      now <- liftIO getCurrentTime
      ss <- asks sessions
      kilt <- liftSTM $ do
        current <- Map.lookup k <$> readTVar ss
        case current ^? _Just . sessionExpires . _Just of
          Nothing -> pure Nothing
          Just x -> if now >= x
                    then
                      modifyTVar' ss (Map.delete k) >> pure current
                    else
                      pure Nothing
      case kilt of
        Nothing -> logDebugN ("Nothing expired for " <> tshow k)
        Just s  -> logDebugN ("Expired session for " <> tshow k) >> sessionDied s

    sessionDied Session{_sessionWill=Nothing} =
      logDebugN ("Session without will: " <> tshow k <> " has died")
    sessionDied Session{_sessionWill=Just T.LastWill{..}} = do
      logDebugN ("Session with will " <> tshow k <> " has died")
      broadcast (blToText _willTopic) _willMsg _willRetain _willQoS


unregisterClient :: (MonadLogger m, MonadUnliftIO m, MonadIO m) => BL.ByteString -> ClientID -> MQTTD m ()
unregisterClient k mid = do
  now <- liftIO getCurrentTime
  c <- asks sessions
  liftSTM $ modifyTVar' c $ Map.update (up now) k
  expireSession k

    where
      up now sess@Session{_sessionClient=Just cc@ConnectedClient{_clientID=i}}
        | mid == i =
          case cc ^? clientConnReq . properties . folded . _PropSessionExpiryInterval of
            Nothing -> Nothing
            Just 0  -> Nothing
            Just x  -> Just $ sess{_sessionExpires=Just (addUTCTime (fromIntegral x) now),
                                   _sessionClient=Nothing}
      up _ s = Just s

sendPacket :: PktQueue -> T.MQTTPkt -> STM Bool
sendPacket ch p = do
  full <- isFullTBQueue ch
  unless full $ writeTBQueue ch p
  pure full

sendPacketIO :: MonadIO m => PktQueue -> T.MQTTPkt -> m Bool
sendPacketIO ch = liftSTM . sendPacket ch

textToBL :: Text -> BL.ByteString
textToBL = BL.fromStrict . TE.encodeUtf8

blToText :: BL.ByteString -> Text
blToText = TE.decodeUtf8 . BL.toStrict

tshow :: Show a => a -> Text
tshow = pack . show

nextPktID :: TVar Word16 -> STM Word16
nextPktID x = do
  modifyTVar' x $ \pid -> if pid == maxBound then 1 else succ pid
  readTVar x

broadcast :: MonadIO m => T.Topic -> BL.ByteString -> Bool -> T.QoS -> MQTTD m ()
broadcast t m r q = do
  subs <- findSubs t
  pid <- liftSTM . nextPktID =<< asks pktID
  mapM_ (`sendPacketIO` pkt pid) subs
  -- TODO honor subscriber options.
  where pkt pid = T.PublishPkt T.PublishRequest{
          _pubDup=False,
          _pubRetain=r,
          _pubQoS=q,
          _pubTopic=textToBL t,
          _pubPktID=pid,
          _pubBody=m,
          _pubProps=mempty}
