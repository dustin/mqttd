{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent     (ThreadId, threadDelay, throwTo)
import           Control.Concurrent.STM (STM, TBQueue, TVar, atomically, isFullTBQueue, modifyTVar', newTBQueue,
                                         newTVar, newTVarIO, readTVar, swapTVar, writeTBQueue, writeTVar)
import           Control.Lens
import           Control.Monad          (filterM, forever, unless, when)
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
import           UnliftIO               (Async (..), MonadUnliftIO (..), async, cancel)

import           Network.MQTT.Lens

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
  cleaner     :: TVar (Maybe (Async ()))
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
newEnv = do
  liftIO $ Env <$> newTVarIO mempty <*> newTVarIO 1 <*> newTVarIO 0 <*> newTVarIO Nothing

runSessionCleaner :: forall m. (MonadLogger m, MonadUnliftIO m, MonadIO m) => MQTTD m ()
runSessionCleaner = do
  c <- asks cleaner
  maybeStart =<< liftSTM (readTVar c)

  where
    maybeStart :: Maybe (Async ()) -> MQTTD m ()
    maybeStart (Just _) = pure ()
    maybeStart Nothing = do
      c <- asks cleaner
      o <- async sessionCleaner
      prev <- liftSTM $ swapTVar c (Just o)
      case prev of
        Nothing -> pure ()
        Just o' -> cancel o' -- double-scheduled

sessionCleaner :: (MonadLogger m, MonadUnliftIO m) => MQTTD m ()
sessionCleaner = forever $ (sleep 1 >> clean1)
  where
    clean1 = do
      tm <- asks sessions
      now <- liftIO getCurrentTime
      ded <- liftSTM $ do
        ded <- Map.filter (not . keep now) <$> readTVar tm
        modifyTVar' tm (Map.filter (keep now))
        pure ded
      mapM_ sessionDied (Map.assocs ded)

    keep _ Session{_sessionClient=Just _} = True
    keep _ Session{_sessionExpires=Nothing} = False
    keep now Session{_sessionExpires=Just x} = now < x

sessionDied :: (MonadIO m, MonadLogger m) => (BL.ByteString, Session) -> MQTTD m ()
sessionDied (k, Session{_sessionWill=Nothing}) = logDebugN ("Session without will: " <> tshow k <> " has died")
sessionDied (k, Session{_sessionWill=Just T.LastWill{..}}) = do
  logDebugN ("Session with will " <> tshow k <> " has died")
  broadcast (blToText _willTopic) _willMsg _willRetain _willQoS

seconds :: Num p => p -> p
seconds = (1000000 *)

sleep :: MonadIO m => Int -> m ()
sleep secs = liftIO (threadDelay (seconds secs))

nextID :: MonadIO m => MQTTD m Int
nextID = asks clientIDGen >>= \ig -> liftSTM $ modifyTVar' ig (+1) >> readTVar ig

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
    subsTopic Session{_sessionSubs} = any (flip T.match t) <$> liftSTM (readTVar _sessionSubs)

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
        | otherwise = (s{_sessionClient=Just nc, _sessionExpires=Nothing, _sessionWill=_lastWill}, T.ExistingSession)

unregisterClient :: (MonadLogger m, MonadUnliftIO m, MonadIO m) => BL.ByteString -> ClientID -> MQTTD m ()
unregisterClient k mid = do
  runSessionCleaner
  now <- liftIO getCurrentTime
  c <- asks sessions
  rmd <- liftSTM $ do
    current <- Map.lookup k <$> readTVar c
    let upv = maybe Nothing (up now) current
    modifyTVar' c (Map.update (const upv) k)
    pure $ maybe current (const Nothing) upv
  case rmd of
    Nothing -> pure ()
    Just s  -> sessionDied (k,s)

    where
      up now sess@Session{_sessionClient=Just (cc@ConnectedClient{_clientID=i})}
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
  mapM_ (flip sendPacketIO (pkt pid)) subs
  where pkt pid = T.PublishPkt T.PublishRequest{
          _pubDup=False,
          _pubRetain=r,
          _pubQoS=q,
          _pubTopic=textToBL t,
          _pubPktID=pid,
          _pubBody=m,
          _pubProps=mempty}
