{-# LANGUAGE ConstraintKinds            #-}
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
import           Data.Time.Clock        (UTCTime (..), addUTCTime, getCurrentTime)
import           Data.Word              (Word16)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..), atomically, readTVarIO)

import           MQTTD.Retention
import           MQTTD.Util
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
  _sessionQP      :: TVar (Map T.PktID T.PublishRequest),
  _sessionSubs    :: TVar (Map T.Filter T.SubOptions),
  _sessionExpires :: Maybe UTCTime,
  _sessionWill    :: Maybe T.LastWill
  }

makeLenses ''Session

data Env = Env {
  sessions    :: TVar (Map BL.ByteString Session),
  lastPktID   :: TVar Word16,
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
nextID = asks clientIDGen >>= \ig -> atomically $ modifyTVar' ig (+1) >> readTVar ig

type PublishConstraint m = (MonadLogger m, MonadFail m, MonadMask m, MonadUnliftIO m, MonadIO m)

sessionCleanup :: PublishConstraint m => MQTTD m ()
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
      t <- atomically $ do
        when (_pubTopic /= "") $ modifyTVar' _clientAliasIn (Map.insert n _pubTopic)
        Map.findWithDefault "" n <$> readTVar _clientAliasIn
      pure $ r & pubTopic .~ t & properties %~ cleanProps
    cleanProps = filter (\case
                            (T.PropTopicAlias _) -> False
                            _ -> True)

findSubs :: MonadIO m => T.Topic -> MQTTD m [(Session, T.SubOptions)]
findSubs t = do
  ss <- asks sessions
  sess <- Map.elems <$> readTVarIO ss
  mconcat <$> traverse sessTopic sess

  where
    sessTopic sess@Session{..} = foldMap (\(m,o) -> [(sess, o) | T.match m t]
                                         ) . Map.assocs <$> readTVarIO _sessionSubs

subscribe :: PublishConstraint m => Session -> T.SubscribeRequest -> MQTTD m ()
subscribe sess@Session{..} (T.SubscribeRequest _ topics _props) = do
  let new = Map.fromList $ map (first blToText) topics
  atomically $ modifyTVar' _sessionSubs (Map.union new)
  p <- asks persistence
  mapM_ (doRetained p) topics

  where
    doRetained _ (_, T.SubOptions{T._retainHandling=T.DoNotSendOnSubscribe}) = pure ()
    doRetained p (t, ops) = mapM_ (sendOne ops) =<< matchRetained p (blToText t)

    sendOne opts@T.SubOptions{..} ir@T.PublishRequest{..} = do
      pid <- atomically . nextPktID =<< asks lastPktID
      let r = ir{T._pubPktID=pid, T._pubRetain=mightRetain opts,
                 T._pubQoS = if _pubQoS > _subQoS then _subQoS else _pubQoS}
      publish sess r

        where
          mightRetain T.SubOptions{_retainAsPublished=False} = False
          mightRetain _ = _pubRetain

unsubscribe :: MonadIO m => Session -> [BL.ByteString] -> MQTTD m [T.UnsubStatus]
unsubscribe Session{..} topics =
  atomically $ do
    m <- readTVar _sessionSubs
    let (uns, n) = foldl' (\(r,m') t -> first ((:r) . unm) $ up t m') ([], m) topics
    writeTVar _sessionSubs n
    pure (reverse uns)

    where
      unm = maybe T.UnsubNoSubscriptionExisted (const T.UnsubSuccess)
      up t = Map.updateLookupWithKey (const.const $ Nothing) (blToText t)

modifySession :: MonadIO m => BL.ByteString -> (Session -> Maybe Session) -> MQTTD m ()
modifySession k f = asks sessions >>= \s -> atomically $ modifyTVar' s (Map.update f k)

registerClient :: MonadIO m => T.ConnectRequest -> ClientID -> ThreadId -> MQTTD m (Session, T.SessionReuse)
registerClient req@T.ConnectRequest{..} i o = do
  c <- asks sessions
  ai <- liftIO $ newTVarIO mempty
  let k = _connID
      nc = ConnectedClient req o i ai
  (o', x, ns) <- atomically $ do
    ch <- newTBQueue 1000
    q2 <- newTVar mempty
    m <- readTVar c
    subz <- newTVar mempty
    let s = Map.lookup k m
        o' = _sessionClient =<< s
        (ns, ruse) = maybeClean ch q2 subz nc s
    writeTVar c (Map.insert k ns m)
    pure (o', ruse, ns)
  case o' of
    Nothing                  -> pure ()
    Just ConnectedClient{..} -> liftIO $ throwTo _clientThread MQTTDuplicate
  pure (ns, x)

    where
      -- TODO: Default expiration?
      maybeClean ch q2 subz nc Nothing = (Session _connID (Just nc) ch q2 subz Nothing _lastWill, T.NewSession)
      maybeClean ch q2 subz nc (Just s)
        | _cleanSession = (Session _connID (Just nc) ch q2 subz Nothing _lastWill, T.NewSession)
        | otherwise = (s{_sessionClient=Just nc,
                         _sessionExpires=Nothing,
                         _sessionWill=_lastWill}, T.ExistingSession)

expireSession :: PublishConstraint m => BL.ByteString -> MQTTD m ()
expireSession k = do
  ss <- asks sessions
  possiblyCleanup =<< atomically (Map.lookup k <$> readTVar ss)

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
      kilt <- atomically $ do
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
      broadcast Nothing (T.PublishRequest{
                            T._pubDup=False,
                            T._pubQoS=_willQoS,
                            T._pubRetain=_willRetain,
                            T._pubTopic=_willTopic,
                            T._pubPktID=0,
                            T._pubBody=_willMsg,
                            T._pubProps=[]})

unregisterClient :: (MonadLogger m, MonadMask m, MonadFail m, MonadUnliftIO m, MonadIO m) => BL.ByteString -> ClientID -> MQTTD m ()
unregisterClient k mid = do
  now <- liftIO getCurrentTime
  c <- asks sessions
  atomically $ modifyTVar' c $ Map.update (up now) k
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
sendPacketIO ch = atomically . sendPacket ch

sendPacketIO_ :: MonadIO m => PktQueue -> T.MQTTPkt -> m ()
sendPacketIO_ ch = void . atomically . sendPacket ch

nextPktID :: TVar Word16 -> STM Word16
nextPktID x = do
  modifyTVar' x $ \pid -> if pid == maxBound then 1 else succ pid
  readTVar x

broadcast :: PublishConstraint m => Maybe BL.ByteString -> T.PublishRequest -> MQTTD m ()
broadcast src req@T.PublishRequest{..} = do
  asks persistence >>= retain req
  subs <- findSubs (blToText _pubTopic)
  pid <- atomically . nextPktID =<< asks lastPktID
  mapM_ (\(s@Session{..}, o) -> justM (publish s) (pkt _sessionID o pid)) subs
  where
    pkt sid T.SubOptions{T._noLocal=True} _
      | Just sid == src = Nothing
    pkt _ opts pid = Just req{
      T._pubDup=False,
      T._pubRetain=mightRetain opts,
      T._pubQoS=maxQoS opts,
      T._pubPktID=pid}

    maxQoS T.SubOptions{_subQoS} = if _pubQoS > _subQoS then _subQoS else _pubQoS
    mightRetain T.SubOptions{_retainAsPublished=False} = False
    mightRetain _ = _pubRetain

publish :: PublishConstraint m => Session -> T.PublishRequest -> MQTTD m ()
publish Session{..} pkt@T.PublishRequest{_pubQoS=T.QoS0} =
  -- QoS 0 is special-cased because it's fire-and-forget with no retries or anything.
  sendPacketIO_ _sessionChan (T.PublishPkt pkt)
publish Session{..} pkt = atomically $ do
  modifyTVar' _sessionQP $ Map.insert (pkt ^. pktID) pkt
  void $ sendPacket _sessionChan (T.PublishPkt pkt)

dispatch :: PublishConstraint m => Session -> T.MQTTPkt -> MQTTD m ()

dispatch Session{..} T.PingPkt = void $ sendPacketIO _sessionChan T.PongPkt

-- QoS 1 ACK (receiving client got our publish message)
dispatch Session{..} (T.PubACKPkt ack) = atomically $ modifyTVar' _sessionQP (Map.delete (ack ^. pktID))

-- QoS 2 ACK (receiving client received our message)
dispatch Session{..} (T.PubRECPkt ack) = atomically $ do
  modifyTVar' _sessionQP (Map.delete (ack ^. pktID))
  void $ sendPacket _sessionChan (T.PubRELPkt $ T.PubREL (ack ^. pktID) 0 mempty)

-- QoS 2 REL (publishing client says we can ship the message)
dispatch Session{..} (T.PubRELPkt rel) = do
  pkt <- atomically $ do
    (r, m) <- Map.updateLookupWithKey (const.const $ Nothing) (rel ^. pktID) <$> readTVar _sessionQP
    writeTVar _sessionQP m
    _ <- sendPacket _sessionChan (T.PubCOMPPkt (T.PubCOMP (rel ^. pktID) (maybe 0x92 (const 0) r) mempty))
    pure r
  justM (broadcast (Just _sessionID)) pkt

-- QoS 2 COMPlete (publishing client says publish is complete)
dispatch _ (T.PubCOMPPkt _) = pure ()

dispatch sess@Session{..} (T.SubscribePkt req@(T.SubscribeRequest pid subs props)) = do
  subscribe sess req
  void $ sendPacketIO _sessionChan (T.SubACKPkt (T.SubscribeResponse pid (map (const (Right T.QoS0)) subs) props))

dispatch sess@Session{..} (T.UnsubscribePkt (T.UnsubscribeRequest pid subs props)) = do
  uns <- unsubscribe sess subs
  void $ sendPacketIO _sessionChan (T.UnsubACKPkt (T.UnsubscribeResponse pid props uns))

dispatch sess@Session{..} (T.PublishPkt req) = do
  r@T.PublishRequest{..} <- resolveAliasIn sess req
  satisfyQoS _pubQoS r
    where
      satisfyQoS T.QoS0 r = broadcast (Just _sessionID) r
      satisfyQoS T.QoS1 r@T.PublishRequest{..} = do
        sendPacketIO_ _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0 mempty))
        broadcast (Just _sessionID) r
      satisfyQoS T.QoS2 r@T.PublishRequest{..} = atomically $ do
        void $ sendPacket _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0 mempty))
        modifyTVar' _sessionQP (Map.insert _pubPktID r)

dispatch sess (T.DisconnectPkt (T.DisconnectRequest T.DiscoNormalDisconnection _props)) = do
  let Just sid = sess ^? sessionClient . _Just . clientConnReq . connID
  modifySession sid (Just . set sessionWill Nothing)

-- TODO: other disconnection types.

dispatch _ x = fail ("unhandled: " <> show x)
