{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent     (ThreadId, threadDelay, throwTo)
import           Control.Concurrent.STM (STM, TBQueue, TVar, isFullTBQueue, modifyTVar', newTBQueue, newTVar, newTVarIO,
                                         readTVar, writeTBQueue, writeTVar)
import           Control.Lens
import           Control.Monad          (unless, void, when)
import           Control.Monad.Catch    (Exception, MonadCatch (..), MonadMask (..), MonadThrow (..))
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..), logDebugN)
import           Control.Monad.Reader   (MonadReader, ReaderT (..), asks, runReaderT)
import           Data.Bifunctor         (first)
import qualified Data.ByteString.Lazy   as BL
import           Data.Foldable          (foldl')
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Text              (Text, pack)
import qualified Data.Text.Encoding     as TE
import           Data.Time.Clock        (UTCTime (..), addUTCTime, getCurrentTime)
import           Data.Word              (Word16)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..))

import           MQTTD.Util
import           Retention
import qualified Scheduler

data MQTTException = MQTTPingTimeout | MQTTDuplicate deriving Show

instance Exception MQTTException

type PktQueue = TBQueue T.MQTTPkt
type ClientID = Int

-- TODO:  Outbound aliases.
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
  _sessionID      :: BL.ByteString,
  _sessionClient  :: Maybe ConnectedClient,
  _sessionChan    :: PktQueue,
  _sessionSubs    :: TVar (Map T.Filter T.SubOptions),
  _sessionExpires :: Maybe UTCTime,
  _sessionWill    :: Maybe T.LastWill
  }

makeLenses ''Session

data Env = Env {
  sessions    :: TVar (Map BL.ByteString Session),
  pktID       :: TVar Word16,
  clientIDGen :: TVar ClientID,
  queueRunner :: Scheduler.QueueRunner BL.ByteString,
  persistence :: Persistence
  }

newtype MQTTD m a = MQTTD
  { runMQTTD :: ReaderT Env m a
  } deriving (Applicative, Functor, Monad, MonadIO, MonadLogger,
              MonadCatch, MonadThrow, MonadMask, MonadReader Env, MonadFail)

instance MonadUnliftIO m => MonadUnliftIO (MQTTD m) where
  withRunInIO inner = MQTTD $ withRunInIO $ \run -> inner (run . runMQTTD)

runIO :: (MonadIO m, MonadLogger m) => Env -> MQTTD m a -> m a
runIO e m = runReaderT (runMQTTD m) e

newEnv :: MonadIO m => m Env
newEnv = liftIO $ Env <$> newTVarIO mempty <*> newTVarIO 1 <*> newTVarIO 0 <*> Scheduler.newRunner <*> newPersistence

seconds :: Num p => p -> p
seconds = (1000000 *)

sleep :: MonadIO m => Int -> m ()
sleep secs = liftIO (threadDelay (seconds secs))

nextID :: MonadIO m => MQTTD m Int
nextID = asks clientIDGen >>= \ig -> liftSTM $ modifyTVar' ig (+1) >> readTVar ig

sessionCleanup :: (MonadUnliftIO m, MonadLogger m) => MQTTD m ()
sessionCleanup = asks queueRunner >>= Scheduler.run expireSession

persistenceCleanup :: (MonadUnliftIO m, MonadLogger m) => MQTTD m ()
persistenceCleanup = asks persistence >>= cleanPersistence

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
      pure $ r & pubTopic .~ t & properties %~ cleanProps
    cleanProps = filter (\case
                            (T.PropTopicAlias _) -> False
                            _ -> True)

findSubs :: MonadIO m => T.Topic -> MQTTD m [(PktQueue, BL.ByteString, T.SubOptions)]
findSubs t = do
  ss <- asks sessions
  sess <- Map.elems <$> liftSTM (readTVar ss)
  mconcat <$> traverse sessTopic sess

  where
    sessTopic Session{..} = foldMap (\(m,o) -> [(_sessionChan, _sessionID, o) | T.match m t]
                                    ) . Map.assocs <$> liftSTM (readTVar _sessionSubs)

subscribe :: MonadIO m => Session -> T.SubscribeRequest -> MQTTD m ()
subscribe Session{..} (T.SubscribeRequest _ topics _props) = do
  let new = Map.fromList $ map (first blToText) topics
  liftSTM $ modifyTVar' _sessionSubs (Map.union new)

unsubscribe :: MonadIO m => Session -> [BL.ByteString] -> MQTTD m [T.UnsubStatus]
unsubscribe Session{..} topics =
  liftSTM $ do
    m <- readTVar _sessionSubs
    let (uns, n) = foldl' (\(r,m') t -> first ((:r) . unm) $ up t m') ([], m) topics
    writeTVar _sessionSubs n
    pure (reverse uns)

    where
      unm = maybe T.UnsubNoSubscriptionExisted (const T.UnsubSuccess)
      up t = Map.updateLookupWithKey (const.const $ Nothing) (blToText t)

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
      maybeClean ch subz nc Nothing = (Session _connID (Just nc) ch subz Nothing _lastWill, T.NewSession)
      maybeClean ch subz nc (Just s)
        | _cleanSession = (Session _connID (Just nc) ch subz Nothing _lastWill, T.NewSession)
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
      broadcast Nothing (blToText _willTopic) _willMsg _willRetain _willQoS


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

broadcast :: MonadIO m => Maybe BL.ByteString -> T.Topic -> BL.ByteString -> Bool -> T.QoS -> MQTTD m ()
broadcast src t m r qos = do
  subs <- findSubs t
  pid <- liftSTM . nextPktID =<< asks pktID
  mapM_ (\(q, s, o) -> maybe (pure ()) (void . sendPacketIO q) (pkt s o pid)) subs
  where
    pkt sid T.SubOptions{T._noLocal=True} _
      | Just sid == src = Nothing
    pkt _ opts pid = Just (T.PublishPkt T.PublishRequest{
                              _pubDup=False,
                              _pubRetain=mightRetain opts,
                              _pubQoS=maxQoS opts,
                              _pubTopic=textToBL t,
                              _pubPktID=pid,
                              _pubBody=m,
                              _pubProps=mempty})

    maxQoS T.SubOptions{_subQoS} = if qos > _subQoS then _subQoS else qos
    mightRetain T.SubOptions{_retainAsPublished=False} = False
    mightRetain _ = r
