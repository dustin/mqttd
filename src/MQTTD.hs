{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Cleff
import           Cleff.Fail
import           Cleff.State
import           Control.Concurrent     (ThreadId, threadDelay, throwTo)
import           Control.Concurrent.STM (STM, TBQueue, TVar, isFullTBQueue, modifyTVar', newTBQueue, newTBQueueIO,
                                         newTVar, newTVarIO, readTVar, tryReadTBQueue, writeTBQueue, writeTVar)
import           Control.Lens
import           Control.Monad          (forever, unless, void, when)
import           Data.Bifunctor         (first, second)
import           Data.Either            (rights)
import           Data.Foldable          (fold, foldl')
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Maybe             (catMaybes, fromMaybe, isJust)
import           Data.Monoid            (Sum (..))
import           Data.String            (IsString (..))
import qualified Data.Text              as Txt
import           Data.Time.Clock        (addUTCTime, getCurrentTime)
import           Data.Word              (Word16)
import           Database.SQLite.Simple (Connection)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           System.Random          (getStdGen, newStdGen, randomR)
import           UnliftIO               (atomically, readTVarIO)

import           MQTTD.Config           (ACL (..), ACLAction (..), User (..))
import           MQTTD.DB
import           MQTTD.GCStats
import           MQTTD.Logging
import           MQTTD.Retention
import           MQTTD.Stats
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
  sharedSubs   :: TVar (SubTree (Map SubscriberName [(SessionID, T.SubOptions)])),
  queueRunner  :: Scheduler.QueueRunner SessionID,
  retainer     :: Retainer,
  authorizer   :: Authorizer,
  dbConnection :: Connection,
  dbQ          :: TBQueue DBOperation,
  stats        :: StatStore
  }

data MQTTD :: Effect where
  GetEnv :: MQTTD m Env
  ModifyEnv :: (Env -> Env) -> MQTTD m ()

makeEffect ''MQTTD

data Intention = IntentPublish | IntentSubscribe deriving (Eq, Show)

newEnv :: MonadIO m => Authorizer -> Connection -> m Env
newEnv a d = liftIO $ Env
         <$> newTVarIO mempty
         <*> newTVarIO 1
         <*> newTVarIO 0
         <*> newTVarIO mempty -- all subs
         <*> newTVarIO mempty -- shared subs
         <*> Scheduler.newRunner
         <*> newRetainer
         <*> pure a
         <*> pure d
         <*> newTBQueueIO 100
         <*> newStatStore

runMQTTD :: forall es a. Env -> Eff (MQTTD : es) a -> Eff es (a, Env)
runMQTTD initialEnv = runState initialEnv . reinterpret \case
    GetEnv      -> get
    ModifyEnv f -> modify f

modifyAuthorizer :: MQTTD :> es => (Authorizer -> Authorizer) -> Eff es a -> Eff es a
modifyAuthorizer f action = do
  auth <- asks authorizer
  modifyEnv (\e -> e{authorizer=f auth})
  result <- action
  modifyEnv (\e -> e{authorizer=auth}) -- restore the original
  pure result

seconds :: Num p => p -> p
seconds = (1000000 *)

asks :: MQTTD :> es => (Env -> a) -> Eff es a
asks f = f <$> getEnv

nextID :: [IOE, MQTTD] :>> es => Eff es Int
nextID = asks clientIDGen >>= \ig -> atomically $ modifyTVarRet ig (+1)

sessionCleanup :: [IOE, MQTTD, Stats, DB, LogFX] :>> es => Eff es ()
sessionCleanup = asks queueRunner >>= Scheduler.run expireSession

retainerCleanup :: [IOE, LogFX, MQTTD, Stats, DB] :>> es => Eff es ()
retainerCleanup = asks retainer >>= cleanRetainer

isClientConnected :: SessionID -> TVar (Map SessionID Session) -> STM Bool
isClientConnected sid sidsv = readTVar sidsv >>= \sids -> pure $ isJust (_sessionClient =<< Map.lookup sid sids)

publishStats :: [IOE, MQTTD, Stats, DB, LogFX] :>> es => Eff es ()
publishStats = forever (pubStats >> sleep 15)
  where
    sleep = liftIO . threadDelay . seconds

    pubStats = do
      pubClients
      pubSubs
      pubRetained
      pubCounters
      gce <- hasGCStats
      when gce $ pubGCStats pubBS

    pub k = pubBS k . textToBL . tshow

    pubBS k v = broadcast Nothing (T.PublishRequest {
                                      T._pubDup=False,
                                      T._pubQoS=T.QoS2,
                                      T._pubRetain=True,
                                      T._pubTopic=k,
                                      T._pubPktID=0,
                                      T._pubBody=v,
                                      T._pubProps=[T.PropMessageExpiryInterval 60]})

    pubClients = do
      ssv <- asks sessions
      ss <- readTVarIO ssv
      pub "$SYS/broker/clients/total" (length ss)
      pub "$SYS/broker/clients/connected" (length . filter (isJust . _sessionClient) . Map.elems $ ss)

    pubRetained = do
      r <- asks retainer
      m <- readTVarIO (_store r)
      pub "$SYS/broker/retained messages/count" (length m)

    pubSubs = do
      m <- readTVarIO =<< asks allSubs
      pub "$SYS/broker/subscriptions/count" (getSum $ foldMap (Sum . length) m)

    pubCounters = do
      m <- retrieveStats
      mapM_ (\(k, v) -> pub (statKeyName k) v) (Map.assocs m)

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
                            _                    -> True)

findSubs :: [IOE, MQTTD] :>> es => T.Topic -> Eff es [(Session, T.SubOptions)]
findSubs t = liftA2 (<>) getRegularSubs getSharedSubs
  where
    getRegularSubs = do
      subs <- asks allSubs
      sess <- asks sessions
      atomically $ do
        sm <- readTVar sess
        foldMap (\(sid,os) -> maybe [] (\s -> [(s,os)]) $ Map.lookup sid sm) . SubTree.findMap t Map.assocs <$> readTVar subs

    getSharedSubs = do
      subs <- asks sharedSubs
      sess <- asks sessions
      r <- liftIO getStdGen
      _ <- liftIO newStdGen

      atomically $ do
        sm <- readTVar sess
        foldMap (\(sid,os) -> maybe [] (\s -> [(s,os)]) $ Map.lookup sid sm) . SubTree.findMap t (foldMap (someElem sm r) . Map.elems) <$> readTVar subs

    someElem sm r els = let els' = filter ((`Map.member` sm) . fst) els
                            (o, _) = randomR (0, length els' - 1) r
                        in [els' !! o]

restoreSessions :: [IOE, MQTTD, Stats, LogFX, DB] :>> es => Eff es ()
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

restoreRetained :: [IOE, MQTTD, Stats, LogFX, DB] :>> es => Eff es ()
restoreRetained = asks retainer >>= MQTTD.Retention.restoreRetained

subscribe :: [IOE, MQTTD, Stats, LogFX, DB] :>> es => Session -> T.SubscribeRequest -> Eff es ()
subscribe sess@Session{..} (T.SubscribeRequest pid topics props) = do
  subs <- asks allSubs
  subsShared <- asks sharedSubs
  let topics' = map (\(t,o) ->
                       case (T.mkFilter . blToText) t of
                         Nothing -> Left T.SubErrNotAuthorized
                         Just t' ->
                           bimap (const T.SubErrNotAuthorized)
                                 (const (t', o))
                                 (authTopic (classifyTopic t') IntentSubscribe _sessionACL)) topics
      (shared, normal) = partitionShared (rights topics')
      new = Map.fromList normal
  atomically $ do
    -- session copy of subscriptions
    modifyTVar' _sessionSubs . Map.union . Map.fromList . rights $ topics'
    -- regular subscriptions
    modifyTVar' subs (upSub new)
    -- shared subscriptions
    modifyTVar' subsShared (upShared shared)
    let r = map (second (T._subQoS . snd)) topics'
    sendPacket_ _sessionChan (T.SubACKPkt (T.SubscribeResponse pid r props))
  p <- asks retainer
  mapM_ (doRetained p) (Map.assocs new)
  storeSession sess

  where
    upSub m subs = Map.foldrWithKey (\k x -> SubTree.add k (Map.singleton _sessionID x)) subs m
    upShared m subs = foldr (\(n,f,o) -> SubTree.addWith f (Map.unionWith (<>)) (Map.singleton n [(_sessionID, o)])) subs m

    doRetained _ (_, T.SubOptions{T._retainHandling=T.DoNotSendOnSubscribe}) = pure ()
    doRetained p (t, ops)                                                    = mapM_ (sendOne ops) =<< matchRetained p t

    sendOne opts@T.SubOptions{..} ir@T.PublishRequest{..} = do
      pid' <- atomically . nextPktID =<< asks lastPktID
      let r = ir{T._pubPktID=pid', T._pubRetain=mightRetain opts,
                 T._pubQoS = if _pubQoS > _subQoS then _subQoS else _pubQoS}
      publish sess r

        where
          mightRetain T.SubOptions{_retainAsPublished=False} = False
          mightRetain _                                      = _pubRetain

removeSubs :: TVar (SubTree (Map SubscriberName [(SessionID, T.SubOptions)]))
           -> TVar (SubTree (Map SessionID T.SubOptions))
           -> SessionID
           -> [T.Filter]
           -> STM ()
removeSubs sharedt subt sid ts = do
  let (shared, normal) = partitionShared (map (,()) ts)
  modifyTVar' subt (up normal)
  modifyTVar' sharedt (upShared shared)

  where
    up l s = foldr (\(t,_) -> SubTree.modify t (Map.delete sid <$>)) s l
    upShared l s = foldr (\(t,_,_) s' -> maybe s' (\t' -> SubTree.modify t' cleanShared s') (T.mkFilter t)) s l
      where
        cleanShared = (fmap . fmap) (filter ((== sid) . fst))

unsubscribe :: [IOE, MQTTD] :>> es => Session -> [BLFilter] -> Eff es [T.UnsubStatus]
unsubscribe Session{..} topics = asks allSubs >>= \subs -> asks sharedSubs >>= \ssubs ->
  atomically $ do
    m <- readTVar _sessionSubs
    let mtopics = T.mkFilter . blToText <$> topics
        (uns, n) = foldl' (\(r,m') t -> first ((:r) . unm) $ up t m') ([], m) mtopics
    writeTVar _sessionSubs n
    removeSubs ssubs subs _sessionID (catMaybes mtopics)
    pure (reverse uns)

    where
      unm = maybe T.UnsubNoSubscriptionExisted (const T.UnsubSuccess)
      up Nothing  = (Nothing,)
      up (Just t) = Map.updateLookupWithKey (const.const $ Nothing) t

modifySession :: [IOE, MQTTD] :>> es => SessionID -> (Session -> Maybe Session) -> Eff es ()
modifySession k f = asks sessions >>= \s -> atomically $ modifyTVar' s (Map.update f k)

registerClient :: [IOE, Fail, MQTTD] :>> es
               => T.ConnectRequest -> ClientID -> ThreadId -> Eff es (Session, T.SessionReuse)
registerClient req@T.ConnectRequest{..} i o = do
  c <- asks sessions
  ai <- liftIO $ newTVarIO mempty
  ao <- liftIO $ newTVarIO mempty
  l <- liftIO $ newTVarIO (fromMaybe 0 (req ^? properties . folded . _PropTopicAliasMaximum))
  authr <- asks (_authUsers . authorizer)
  let k = _connID
      nc = ConnectedClient req o i ai ao l
      acl = fromMaybe [] (fmap (\(User _ _ a) -> a) . (`Map.lookup` authr) =<< _username)
      maxInFlight = fromMaybe maxBound (req ^? properties . folded . _PropReceiveMaximum)
  when (maxInFlight == 0) $ fail "max in flight must be greater than zero"
  (o', x, ns) <- atomically $ do
    emptySession <- Session _connID acl (Just nc) <$> newTBQueue defaultQueueSize
                    <*> newTVar maxInFlight <*> newTBQueue defaultQueueSize
                    <*> newTVar mempty <*> newTVar mempty
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
                         _sessionBacklog=_sessionBacklog ns,
                         _sessionFlight=_sessionFlight ns,
                         _sessionWill=_lastWill}, T.ExistingSession)

expireSession :: [IOE, MQTTD, Stats, DB, LogFX] :>> es => SessionID -> Eff es ()
expireSession k = do
  ss <- asks sessions
  possiblyCleanup =<< atomically (Map.lookup k <$> readTVar ss)

  where
    possiblyCleanup Nothing = pure ()
    possiblyCleanup (Just Session{_sessionClient=Just _}) = logDbgL [tshow k, " is in use"]
    possiblyCleanup (Just Session{_sessionClient=Nothing,
                                  _sessionExpires=Nothing}) = expireNow
    possiblyCleanup (Just Session{_sessionClient=Nothing,
                                  _sessionExpires=Just ex,
                                  _sessionSubs=subsv}) = do
      now <- liftIO getCurrentTime
      subs <- readTVarIO subsv
      if hasHighQoS subs && ex > now
        then void . Scheduler.enqueue ex k =<< asks queueRunner
        else expireNow

    hasHighQoS = isJust . preview (folded . subQoS . filtered (> T.QoS0))

    expireNow = do
      now <- liftIO getCurrentTime
      ss <- asks sessions
      kilt <- atomically $ do
        current <- Map.lookup k <$> readTVar ss
        subs <- maybe (pure mempty) (readTVar . _sessionSubs) current
        case current ^? _Just . sessionExpires . _Just of
          Nothing -> pure Nothing
          Just x -> if not (hasHighQoS subs) || now >= x
                    then
                      modifyTVar' ss (Map.delete k) >> pure current
                    else
                      pure Nothing
      case kilt of
        Nothing -> logDbgL ["Nothing expired for ", tshow k]
        Just s@Session{..}  -> do
          logDbgL ["Expired session for ", tshow k]
          subt <- asks allSubs
          ssubst <- asks sharedSubs
          atomically $ do
            subs <- readTVar _sessionSubs
            removeSubs ssubst subt _sessionID (Map.keys subs)
          deleteSession _sessionID
          sessionDied s

    sessionDied Session{_sessionWill=Nothing} =
      logDbgL ["Session without will: ", tshow k, " has died"]
    sessionDied Session{_sessionWill=Just T.LastWill{..}} = do
      logDbgL ["Session with will ", tshow k, " has died"]
      broadcast Nothing (T.PublishRequest{
                            T._pubDup=False,
                            T._pubQoS=_willQoS,
                            T._pubRetain=_willRetain,
                            T._pubTopic=_willTopic,
                            T._pubPktID=0,
                            T._pubBody=_willMsg,
                            T._pubProps=_willProps})

unregisterClient :: [IOE, MQTTD, LogFX, Stats, DB] :>> es => SessionID -> ClientID -> Eff es ()
unregisterClient k mid = do
  now <- liftIO getCurrentTime
  modifySession k (up now)
  expireSession k

    where
      up now sess@Session{_sessionClient=Just cc@ConnectedClient{_clientID=i}}
        | mid == i =
          case cc ^? clientConnReq . properties . folded . _PropSessionExpiryInterval of
            -- Default expiry
            Nothing -> Just $ sess{_sessionExpires=Just (addUTCTime defaultSessionExp now),
                                   _sessionClient=Nothing}
            -- Specifically destroy now
            Just 0 -> Just $ sess{_sessionExpires=Nothing, _sessionClient=Nothing}
            -- Hold on for maybe a bit.
            Just x  -> Just $ sess{_sessionExpires=Just (addUTCTime (fromIntegral x) now),
                                   _sessionClient=Nothing}
      up _ s = Just s

tryWriteQ :: TBQueue a -> a -> STM Bool
tryWriteQ q a = do
  full <- isFullTBQueue q
  unless full $ writeTBQueue q a
  pure full

sendPacket :: PktQueue -> T.MQTTPkt -> STM Bool
sendPacket = tryWriteQ

sendPacket_ :: PktQueue -> T.MQTTPkt -> STM ()
sendPacket_ q = void . sendPacket q

sendPacketIO :: MonadIO m => PktQueue -> T.MQTTPkt -> m Bool
sendPacketIO ch = atomically . sendPacket ch

sendPacketIO_ :: MonadIO m => PktQueue -> T.MQTTPkt -> m ()
sendPacketIO_ ch = void . atomically . sendPacket ch

nextPktID :: (Enum a, Bounded a, Eq a, Num a) => TVar a -> STM a
nextPktID x = modifyTVarRet x $ \pid -> if pid == maxBound then 1 else succ pid

broadcast :: [IOE, MQTTD, Stats, DB, LogFX] :>> es => Maybe SessionID -> T.PublishRequest -> Eff es ()
broadcast src req@T.PublishRequest{..} = do
  asks retainer >>= retain req
  subs <- maybe (pure []) findSubs ((T.mkTopic . blToText) _pubTopic)
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
    mightRetain _                                      = _pubRetain

publish :: [IOE, MQTTD, Stats] :>> es => Session -> T.PublishRequest -> Eff es ()
publish sess@Session{..} pkt@T.PublishRequest{..}
  -- QoS 0 is special-cased because it's fire-and-forget with no retries or anything.
  | _pubQoS == T.QoS0 = getStatStore >>= \ss -> atomically $ deliver ss sess pkt
  | otherwise = getStatStore >>= \ss -> atomically $ do
      modifyTVar' _sessionQP $ Map.insert (pkt ^. pktID) pkt
      tokens <- readTVar _sessionFlight
      if tokens == 0
        then void $ tryWriteQ _sessionBacklog pkt
        else deliver ss sess pkt

deliver :: StatStore -> Session -> T.PublishRequest -> STM ()
deliver ss Session{..} pkt@T.PublishRequest{..} = do
  when (_pubQoS > T.QoS0) $ modifyTVar' _sessionFlight pred
  p <- maybe (pure pkt) (`aliasOut` pkt) _sessionClient
  sendPacket_ _sessionChan (T.PublishPkt p)
  incrementStatSTM StatMsgSent 1 ss

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


topicTypeFilter :: TopicType -> T.Filter
topicTypeFilter (Normal t)               = t
topicTypeFilter (SharedSubscription _ t) = t
topicTypeFilter _                        = ""

authTopic :: TopicType -> Intention -> [ACL] -> Either String ()
authTopic InvalidTopic _ = const $ Left "invalid topics are invalid"
authTopic tt _
  | topicTypeFilter tt == "" = const $ Left "empty topics are not valid"
authTopic tt action = foldr check (Right ())
  where
    t = topicTypeFilter tt
    check (Allow aclt f) o
      | actOK aclt action && T.match f (ftot t) = Right ()
      | T.match f (ftot t) = Left "unauthorized topic"
      | otherwise = o
    check (Deny f) o
      | T.match f (ftot t) = Left "unauthorized topic"
      | otherwise = o
    actOK ACLSub IntentSubscribe    = True
    actOK ACLPubSub IntentSubscribe = True
    actOK ACLPubSub IntentPublish   = True
    actOK _ _                       = False

    ftot = fromString . Txt.unpack . T.unFilter

releasePubSlot :: StatStore -> Session -> STM ()
releasePubSlot ss sess@Session{..} = do
  modifyTVar' _sessionFlight succ
  justM (deliver ss sess) =<< tryReadTBQueue _sessionBacklog

dispatch :: [IOE, Fail, MQTTD, DB, LogFX, Stats] :>> es => Session -> T.MQTTPkt -> Eff es ()

dispatch Session{..} T.PingPkt = sendPacketIO_ _sessionChan T.PongPkt

-- QoS 1 ACK (receiving client got our publish message)
dispatch sess@Session{..} (T.PubACKPkt ack) = getStatStore >>= \st -> atomically $ do
  modifyTVar' _sessionQP (Map.delete (ack ^. pktID))
  releasePubSlot st sess

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
dispatch sess (T.PubCOMPPkt _) = getStatStore >>= \st -> atomically $ releasePubSlot st sess

-- Subscribe response is sent from the `subscribe` action because the
-- interaction is a bit complicated.
dispatch sess (T.SubscribePkt req) = subscribe sess req

dispatch sess@Session{..} (T.UnsubscribePkt (T.UnsubscribeRequest pid subs props)) = do
  uns <- unsubscribe sess subs
  sendPacketIO_ _sessionChan (T.UnsubACKPkt (T.UnsubscribeResponse pid props uns))

dispatch sess@Session{..} (T.PublishPkt req) = do
  r@T.PublishRequest{..} <- resolveAliasIn sess req
  case authPub (maybe InvalidTopic classifyTopic $ (T.mkFilter . blToText) _pubTopic) _sessionACL of
    Left _  -> logInfoL ["Unauthorized topic: ", tshow _pubTopic] >> nak _pubQoS r
    Right _ -> satisfyQoS _pubQoS r

    where
      nak T.QoS0 _ = pure ()
      nak T.QoS1 T.PublishRequest{..} =
        sendPacketIO_ _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0x87 mempty))
      nak T.QoS2 T.PublishRequest{..} =
        sendPacketIO_ _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0x87 mempty))

      satisfyQoS T.QoS0 r = broadcast (Just _sessionID) r >> countIn
      satisfyQoS T.QoS1 r@T.PublishRequest{..} = do
        sendPacketIO_ _sessionChan (T.PubACKPkt (T.PubACK _pubPktID 0 mempty))
        broadcast (Just _sessionID) r
        countIn
      satisfyQoS T.QoS2 r@T.PublishRequest{..} = getStatStore >>= \ss -> atomically $ do
        sendPacket_ _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0 mempty))
        modifyTVar' _sessionQP (Map.insert _pubPktID r)
        incrementStatSTM StatMsgRcvd 1 ss

      countIn = incrementStat StatMsgRcvd 1

      authPub :: TopicType -> [ACL] -> Either String ()
      authPub t acl = case t of
                        Normal _ -> Right ()
                        _        -> Left "invalid topic"
                      >> authTopic t IntentPublish acl

dispatch sess (T.DisconnectPkt (T.DisconnectRequest T.DiscoNormalDisconnection _props)) = do
  let sid = sess ^?! sessionClient . _Just . clientConnReq . connID
  modifySession sid (Just . set sessionWill Nothing)

dispatch _ (T.DisconnectPkt (T.DisconnectRequest T.DiscoDisconnectWithWill _props)) = pure ()

dispatch _ x = fail ("unhandled: " <> show x)
