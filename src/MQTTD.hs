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
import           Control.Concurrent.STM (STM, TBQueue, TChan, TVar, isFullTBQueue, modifyTVar', newTBQueue, newTChan,
                                         newTVar, newTVarIO, readTChan, readTVar, writeTBQueue, writeTChan, writeTVar)
import           Control.Lens
import           Control.Monad          (unless, void, when)
import           Control.Monad.Catch    (Exception, MonadCatch (..), MonadMask (..), MonadThrow (..), bracket_)
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
import           UnliftIO               (MonadUnliftIO (..), atomically, cancel, withAsync)

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
  _sessionResp    :: TVar (Map Word16 (TChan T.MQTTPkt)),
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

gotResponse :: MonadIO m => Session -> T.PktID -> T.MQTTPkt -> MQTTD m ()
gotResponse Session{..} i p = atomically $ do
  mch <- Map.lookup i <$> readTVar _sessionResp
  maybe (pure ()) (`writeTChan` p) mch

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
  sess <- Map.elems <$> atomically (readTVar ss)
  mconcat <$> traverse sessTopic sess

  where
    sessTopic sess@Session{..} = foldMap (\(m,o) -> [(sess, o) | T.match m t]
                                         ) . Map.assocs <$> atomically (readTVar _sessionSubs)

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
    rm <- newTVar mempty
    m <- readTVar c
    subz <- newTVar mempty
    let s = Map.lookup k m
        o' = _sessionClient =<< s
        (ns, ruse) = maybeClean ch rm subz nc s
    writeTVar c (Map.insert k ns m)
    pure (o', ruse, ns)
  case o' of
    Nothing                  -> pure ()
    Just ConnectedClient{..} -> liftIO $ throwTo _clientThread MQTTDuplicate
  pure (ns, x)

    where
      maybeClean ch rm subz nc Nothing = (Session _connID (Just nc) ch rm subz Nothing _lastWill, T.NewSession)
      maybeClean ch rm subz nc (Just s)
        | _cleanSession = (Session _connID (Just nc) ch rm subz Nothing _lastWill, T.NewSession)
        | otherwise = (s{_sessionClient=Just nc,
                         _sessionExpires=Nothing,
                         _sessionResp=rm,
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
  mapM_ (\(s@Session{..}, o) -> maybe (pure ()) (publish s) (pkt _sessionID o pid)) subs
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
publish Session{..} pkt = do
  ch <- atomically newTChan
  let pid = pkt ^. pktID
  bracket_ (observe pid ch) (ignore pid) (publishAndWait ch)

    where
      observe pid ch = atomically $ modifyTVar' _sessionResp (Map.insert pid ch)
      ignore pid = atomically $ modifyTVar' _sessionResp (Map.delete pid)

      publishAndWait _
        | pkt ^. pubQoS == T.QoS0 = sendPacketIO_ _sessionChan (T.PublishPkt pkt)
      publishAndWait ch   = withAsync (pub pkt) (\p -> satisfyQoS p ch (pkt ^. pubQoS))

      pub p = do
        sendPacketIO_ _sessionChan (T.PublishPkt p)
        liftIO $ threadDelay 5000000
        logDebugN ("Retransmitting " <> tshow p)
        pub (p & pubDup .~ True)

      satisfyQoS p ch q
        | q == T.QoS0 = pure ()
        | q == T.QoS1 = void $ do
            (T.PubACKPkt (T.PubACK _ st pprops)) <- atomically $ readTChan ch
            when (st /= 0) $ fail ("qos 1 publish error: " <> show st <> " " <> show pprops)
        | q == T.QoS2 = waitRec
        | otherwise = error "invalid QoS"

        where
          waitRec = do
            rpkt <- atomically $ readTChan ch
            case rpkt of
              T.PubRECPkt (T.PubREC _ st recprops) -> do
                when (st /= 0) $ fail ("qos 2 REC publish error: " <> show st <> " " <> show recprops)
                sendPacketIO_ _sessionChan (T.PubRELPkt $ T.PubREL (pkt ^. pktID) 0 mempty)
                cancel p -- must not publish after rel
              T.PubCOMPPkt (T.PubCOMP _ st' compprops) ->
                when (st' /= 0) $ fail ("qos 2 COMP publish error: " <> show st' <> " " <> show compprops)
              wtf -> fail ("unexpected packet received in QoS2 publish: " <> show wtf)
