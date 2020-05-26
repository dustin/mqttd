{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent     (ThreadId, throwTo)
import           Control.Concurrent.STM (STM, TBQueue, TVar, isFullTBQueue, modifyTVar', newTBQueue, newTBQueueIO,
                                         newTVar, newTVarIO, readTVar, writeTBQueue, writeTVar)
import           Control.Lens
import           Control.Monad          (unless, void, when)
import           Control.Monad.Catch    (MonadCatch (..), MonadMask (..), MonadThrow (..))
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..), logDebugN, logInfoN)
import           Control.Monad.Reader   (MonadReader (..), ReaderT (..), asks, local)
import           Data.Bifunctor         (first, second)
import           Data.Either            (rights)
import           Data.Foldable          (fold, foldl')
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (fromMaybe)
import           Data.Time.Clock        (addUTCTime, getCurrentTime)
import           Data.Word              (Word16)
import           Database.SQLite.Simple (Connection)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..), atomically, readTVarIO)

import           MQTTD.Config           (ACL (..), User (..))
import           MQTTD.DB
import           MQTTD.Retention
import           MQTTD.SubTree          (SubTree)
import qualified MQTTD.SubTree          as SubTree
import           MQTTD.Types
import           MQTTD.Util

import qualified Scheduler

data Env = Env {
  sessions     :: TVar (Map SessionID Session),
  lastPktID    :: TVar Word16,
  clientIDGen  :: TVar ClientID,
  allSubs      :: TVar (SubTree (Map SessionID T.SubOptions)),
  queueRunner  :: Scheduler.QueueRunner SessionID,
  retainer     :: Retainer,
  authorizer   :: Authorizer,
  dbConnection :: Connection,
  dbQ          :: TBQueue DBOperation
  }

newtype MQTTD m a = MQTTD
  { runMQTTD :: ReaderT Env m a
  } deriving (Applicative, Functor, Monad, MonadIO, MonadLogger,
              MonadCatch, MonadThrow, MonadMask, MonadReader Env, MonadFail)

instance MonadUnliftIO m => MonadUnliftIO (MQTTD m) where
  withRunInIO inner = MQTTD $ withRunInIO $ \run -> inner (run . runMQTTD)

instance (Monad m, MonadReader Env m) => HasDBConnection m where
  dbConn = asks dbConnection
  dbQueue = asks dbQ

runIO :: (MonadIO m, MonadLogger m) => Env -> MQTTD m a -> m a
runIO e m = runReaderT (runMQTTD m) e

newEnv :: MonadIO m => Authorizer -> Connection -> m Env
newEnv a d = liftIO $ Env
         <$> newTVarIO mempty
         <*> newTVarIO 1
         <*> newTVarIO 0
         <*> newTVarIO mempty
         <*> Scheduler.newRunner
         <*> newRetainer
         <*> pure a
         <*> pure d
         <*> newTBQueueIO 100

modifyAuthorizer :: Monad m => (Authorizer -> Authorizer) -> MQTTD m a -> MQTTD m a
modifyAuthorizer f = local (\e@Env{..} -> e{authorizer=f authorizer})

seconds :: Num p => p -> p
seconds = (1000000 *)

nextID :: MonadIO m => MQTTD m Int
nextID = asks clientIDGen >>= \ig -> atomically $ modifyTVarRet ig (+1)

type PublishConstraint m = (MonadLogger m, MonadFail m, MonadMask m, MonadUnliftIO m, MonadIO m)

sessionCleanup :: PublishConstraint m => MQTTD m ()
sessionCleanup = asks queueRunner >>= Scheduler.run expireSession

retainerCleanup :: (MonadUnliftIO m, MonadLogger m) => MQTTD m ()
retainerCleanup = asks retainer >>= cleanRetainer

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
  subs <- asks allSubs
  sess <- asks sessions
  atomically $ do
    sm <- readTVar sess
    foldMap (\(sid,os) -> maybe [] (\s -> [(s,os)]) $ Map.lookup sid sm) . SubTree.findMap t Map.assocs <$> readTVar subs

restoreSessions :: PublishConstraint m => MQTTD m ()
restoreSessions = do
  ss <- loadSessions
  subs <- SubTree.fromList . fold <$> traverse flatSubs ss
  sessv <- asks sessions
  subv <- asks allSubs
  atomically $ do
    writeTVar sessv (Map.fromList . map (\s@Session{..} -> (_sessionID, s)) $ ss)
    writeTVar subv subs
  mapM_ (expireSession . _sessionID) ss

  where
    flatSubs :: MonadIO m => Session -> m [(T.Filter, Map SessionID T.SubOptions)]
    flatSubs Session{..} = Map.foldMapWithKey (\k v -> [(k, Map.singleton _sessionID v)]) <$> readTVarIO _sessionSubs

restoreRetained :: MonadIO m => MQTTD m ()
restoreRetained = asks retainer >>= MQTTD.Retention.restoreRetained

subscribe :: PublishConstraint m => Session -> T.SubscribeRequest -> MQTTD m [Either T.SubErr T.QoS]
subscribe sess@Session{..} (T.SubscribeRequest _ topics _props) = do
  subs <- asks allSubs
  let topics' = map (\(t,o) -> let t' = blToText t in
                                 bimap (const T.SubErrNotAuthorized) (const (t', o)) $ authTopic t' _sessionACL) topics
      new = Map.fromList $ rights topics'
  atomically $ do
    modifyTVar' _sessionSubs (Map.union new)
    modifyTVar' subs (upSub new)
  p <- asks retainer
  mapM_ (doRetained p) (Map.assocs new)
  storeSession sess
  pure $ map (second (T._subQoS . snd)) topics'

  where
    upSub m subs = Map.foldrWithKey (\k x -> SubTree.add k (Map.singleton _sessionID x)) subs m

    doRetained _ (_, T.SubOptions{T._retainHandling=T.DoNotSendOnSubscribe}) = pure ()
    doRetained p (t, ops) = mapM_ (sendOne ops) =<< matchRetained p t

    sendOne opts@T.SubOptions{..} ir@T.PublishRequest{..} = do
      pid <- atomically . nextPktID =<< asks lastPktID
      let r = ir{T._pubPktID=pid, T._pubRetain=mightRetain opts,
                 T._pubQoS = if _pubQoS > _subQoS then _subQoS else _pubQoS}
      publish sess r

        where
          mightRetain T.SubOptions{_retainAsPublished=False} = False
          mightRetain _ = _pubRetain

removeSubs :: TVar (SubTree (Map SessionID T.SubOptions)) -> SessionID -> [T.Filter] -> STM ()
removeSubs subt sid ts = modifyTVar' subt up
  where
    up s = foldr (\t -> SubTree.modify t (Map.delete sid <$>)) s ts

unsubscribe :: MonadIO m => Session -> [BLFilter] -> MQTTD m [T.UnsubStatus]
unsubscribe Session{..} topics = asks allSubs >>= \subs ->
  atomically $ do
    m <- readTVar _sessionSubs
    let (uns, n) = foldl' (\(r,m') t -> first ((:r) . unm) $ up t m') ([], m) topics
    writeTVar _sessionSubs n
    removeSubs subs _sessionID (blToText <$> topics)
    pure (reverse uns)

    where
      unm = maybe T.UnsubNoSubscriptionExisted (const T.UnsubSuccess)
      up t = Map.updateLookupWithKey (const.const $ Nothing) (blToText t)

modifySession :: MonadIO m => SessionID -> (Session -> Maybe Session) -> MQTTD m ()
modifySession k f = asks sessions >>= \s -> atomically $ modifyTVar' s (Map.update f k)

registerClient :: MonadIO m => T.ConnectRequest -> ClientID -> ThreadId -> MQTTD m (Session, T.SessionReuse)
registerClient req@T.ConnectRequest{..} i o = do
  c <- asks sessions
  ai <- liftIO $ newTVarIO mempty
  ao <- liftIO $ newTVarIO mempty
  l <- liftIO $ newTVarIO (fromMaybe 0 (req ^? properties . folded . _PropTopicAliasMaximum))
  authr <- asks (_authUsers . authorizer)
  let k = _connID
      nc = ConnectedClient req o i ai ao l
      acl = fromMaybe [] (fmap (\(User _ _ a) -> a) . (`Map.lookup` authr) =<< _username)
  (o', x, ns) <- atomically $ do
    emptySession <- Session _connID acl (Just nc) <$> newTBQueue 1000 <*> newTVar mempty <*> newTVar mempty
                    <*> pure Nothing <*> pure _lastWill
    m <- readTVar c
    let s = Map.lookup k m
        o' = _sessionClient =<< s
        (ns, ruse) = maybeClean emptySession s
    writeTVar c (Map.insert k ns m)
    pure (o', ruse, ns)
  case o' of
    Nothing                  -> pure ()
    Just ConnectedClient{..} -> liftIO $ throwTo _clientThread (MQTTDuplicate _connID)
  pure (ns, x)

    where
      maybeClean ns Nothing = (ns, T.NewSession)
      maybeClean ns (Just s)
        | _cleanSession = (ns, T.NewSession)
        | otherwise = (s{_sessionClient=_sessionClient ns,
                         _sessionExpires=Nothing,
                         _sessionChan=_sessionChan ns,
                         _sessionWill=_lastWill}, T.ExistingSession)

expireSession :: PublishConstraint m => SessionID -> MQTTD m ()
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
        Just s@Session{..}  -> do
          logDebugN ("Expired session for " <> tshow k)
          subt <- asks allSubs
          atomically $ do
            subs <- readTVar _sessionSubs
            removeSubs subt _sessionID (Map.keys subs)
          deleteSession _sessionID
          sessionDied s

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
                            T._pubProps=_willProps})

unregisterClient :: (MonadLogger m, MonadMask m, MonadFail m, MonadUnliftIO m, MonadIO m) => SessionID -> ClientID -> MQTTD m ()
unregisterClient k mid = do
  now <- liftIO getCurrentTime
  c <- asks sessions
  atomically $ modifyTVar' c $ Map.update (up now) k
  expireSession k

    where
      up now sess@Session{_sessionClient=Just cc@ConnectedClient{_clientID=i}}
        | mid == i =
          case cc ^? clientConnReq . properties . folded . _PropSessionExpiryInterval of
            -- Default expiry
            Nothing -> Just $ sess{_sessionExpires=Just (addUTCTime defaultSessionExp now),
                                   _sessionClient=Nothing}
            -- Specifically destroy now
            Just 0 -> Nothing
            -- Hold on for maybe a bit.
            Just x  -> Just $ sess{_sessionExpires=Just (addUTCTime (fromIntegral x) now),
                                   _sessionClient=Nothing}
      up _ s = Just s

sendPacket :: PktQueue -> T.MQTTPkt -> STM Bool
sendPacket ch p = do
  full <- isFullTBQueue ch
  unless full $ writeTBQueue ch p
  pure full

sendPacket_ :: PktQueue -> T.MQTTPkt -> STM ()
sendPacket_ q = void . sendPacket q

sendPacketIO :: MonadIO m => PktQueue -> T.MQTTPkt -> m Bool
sendPacketIO ch = atomically . sendPacket ch

sendPacketIO_ :: MonadIO m => PktQueue -> T.MQTTPkt -> m ()
sendPacketIO_ ch = void . atomically . sendPacket ch

modifyTVarRet :: TVar a -> (a -> a) -> STM a
modifyTVarRet v f = modifyTVar' v f >> readTVar v

nextPktID :: TVar Word16 -> STM Word16
nextPktID x = modifyTVarRet x $ \pid -> if pid == maxBound then 1 else succ pid

broadcast :: PublishConstraint m => Maybe SessionID -> T.PublishRequest -> MQTTD m ()
broadcast src req@T.PublishRequest{..} = do
  asks retainer >>= retain req
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
publish Session{..} pkt@T.PublishRequest{..} = atomically $ do
  -- QoS 0 is special-cased because it's fire-and-forget with no retries or anything.
  when (_pubQoS /= T.QoS0) $ modifyTVar' _sessionQP $ Map.insert (pkt ^. pktID) pkt
  p <- maybe (pure pkt) (`aliasOut` pkt) _sessionClient
  sendPacket_ _sessionChan (T.PublishPkt p)

aliasOut :: ConnectedClient -> T.PublishRequest -> STM T.PublishRequest
aliasOut ConnectedClient{..} pkt@T.PublishRequest{..} =
  maybe allocate existing . Map.lookup _pubTopic =<< readTVar _clientAliasOut
    where
      existing n = pure pkt{T._pubTopic="", T._pubProps=T.PropTopicAlias n:_pubProps}
      allocate = readTVar _clientALeft >>= \l ->
        if l == 0 then pure pkt
        else do
          modifyTVar' _clientALeft pred
          modifyTVar' _clientAliasOut (Map.insert _pubTopic l)
          pure pkt{T._pubProps=T.PropTopicAlias l:_pubProps}

authTopic :: T.Topic -> [ACL] -> Either String ()
authTopic t = foldr check (Right ())
  where
    check (Allow f) o
      | T.match f t = Right ()
      | otherwise = o
    check (Deny f) o
      | T.match f t = Left "unauthorized topic"
      | otherwise = o

dispatch :: PublishConstraint m => Session -> T.MQTTPkt -> MQTTD m ()

dispatch Session{..} T.PingPkt = sendPacketIO_ _sessionChan T.PongPkt

-- QoS 1 ACK (receiving client got our publish message)
dispatch Session{..} (T.PubACKPkt ack) = atomically $ modifyTVar' _sessionQP (Map.delete (ack ^. pktID))

-- QoS 2 ACK (receiving client received our message)
dispatch Session{..} (T.PubRECPkt ack) = atomically $ do
  modifyTVar' _sessionQP (Map.delete (ack ^. pktID))
  sendPacket_ _sessionChan (T.PubRELPkt $ T.PubREL (ack ^. pktID) 0 mempty)

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

dispatch sess@Session{..} (T.SubscribePkt req@(T.SubscribeRequest pid _ props)) = do
  r <- subscribe sess req
  sendPacketIO_ _sessionChan (T.SubACKPkt (T.SubscribeResponse pid r props))

dispatch sess@Session{..} (T.UnsubscribePkt (T.UnsubscribeRequest pid subs props)) = do
  uns <- unsubscribe sess subs
  sendPacketIO_ _sessionChan (T.UnsubACKPkt (T.UnsubscribeResponse pid props uns))

dispatch sess@Session{..} (T.PublishPkt req) = do
  r@T.PublishRequest{..} <- resolveAliasIn sess req
  case authTopic (blToText _pubTopic) _sessionACL of
    Left _  -> logInfoN ("Unauthorized topic: " <> tshow _pubTopic) >> nak _pubQoS r
    Right _ -> satisfyQoS _pubQoS r

    where
      nak T.QoS0 _ = pure ()
      nak T.QoS1 T.PublishRequest{..} =
        sendPacketIO_ _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0x87 mempty))
      nak T.QoS2 T.PublishRequest{..} =
        sendPacketIO_ _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0x87 mempty))

      satisfyQoS T.QoS0 r = broadcast (Just _sessionID) r
      satisfyQoS T.QoS1 r@T.PublishRequest{..} = do
        sendPacketIO_ _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0 mempty))
        broadcast (Just _sessionID) r
      satisfyQoS T.QoS2 r@T.PublishRequest{..} = atomically $ do
        sendPacket_ _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0 mempty))
        modifyTVar' _sessionQP (Map.insert _pubPktID r)

dispatch sess (T.DisconnectPkt (T.DisconnectRequest T.DiscoNormalDisconnection _props)) = do
  let Just sid = sess ^? sessionClient . _Just . clientConnReq . connID
  modifySession sid (Just . set sessionWill Nothing)

dispatch _ (T.DisconnectPkt (T.DisconnectRequest T.DiscoDisconnectWithWill _props)) = pure ()

dispatch _ x = fail ("unhandled: " <> show x)
