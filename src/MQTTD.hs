{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent            (ThreadId, threadDelay, throwTo)
import           Control.Concurrent.STM        (STM, TBQueue, TVar, isFullTBQueue, modifyTVar', newTBQueue, newTVar, newTVarIO,
                                                readTVar, tryReadTBQueue, writeTBQueue, writeTVar)
import           Control.Lens
import           Control.Monad                 (forever, guard, mfilter, unless, void, when)
import           Data.Bifunctor                (first, second)
import           Data.Either                   (rights)
import qualified Data.ByteString.Lazy          as BL
import           Data.Foldable                 (fold, foldl')
import           Data.Map.Strict               (Map)
import qualified Data.Map.Strict               as Map
import qualified Data.Set                      as Set
import           Data.Maybe                    (catMaybes, fromMaybe, isJust, listToMaybe)
import           Data.Monoid                   (Sum (..))
import           Data.String                   (IsString (..))
import qualified Data.Text                     as Txt
import           Data.Time.Clock               (NominalDiffTime, UTCTime, addUTCTime,
                                                getCurrentTime)
import           Data.Word                     (Word16)
import           Effectful
import           Effectful.Dispatch.Dynamic    (reinterpret)
import           Effectful.Fail
import           Effectful.State.Static.Shared
import           Effectful.TH
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic            as T
import qualified Network.MQTT.Types            as T
import           System.Random                 (getStdGen, newStdGen, randomR)
import           UnliftIO                      (atomically, readTVarIO)

import           MQTTD.Authorizer
import           MQTTD.Config           (ACL (..), ACLAction (..))
import           MQTTD.DB
import           MQTTD.GCStats
import           MQTTD.Logging
import           MQTTD.Retention
import           MQTTD.Stats
import           MQTTD.SubTree          (SubTree)
import qualified MQTTD.SubTree          as SubTree
import           MQTTD.Types
import           MQTTD.Util

import           MQTTD.ScheduleFX

data Env = Env {
  sessions    :: TVar (Map SessionID Session),
  clientIDGen :: TVar ClientID,
  allSubs     :: TVar (SubTree (Map SessionID T.SubOptions)),
  sharedSubs  :: TVar (SubTree (Map SubscriberName [(SessionID, T.SubOptions)])),
  retainer    :: Retainer
  }

data MQTTD :: Effect where
  GetEnv :: MQTTD m Env
  ModifyEnv :: (Env -> Env) -> MQTTD m ()

makeEffect ''MQTTD

data Intention = IntentPublish | IntentSubscribe deriving (Eq, Show)

newEnv :: MonadIO m => m Env
newEnv = liftIO $ Env
         <$> newTVarIO mempty
         <*> newTVarIO 0
         <*> newTVarIO mempty -- all subs
         <*> newTVarIO mempty -- shared subs
         <*> newRetainer

runMQTTD :: forall es a. Env -> Eff (MQTTD : es) a -> Eff es (a, Env)
runMQTTD initialEnv = reinterpret (runState initialEnv) \_ -> \case
    GetEnv      -> get
    ModifyEnv f -> modify f

seconds :: Num p => p -> p
seconds = (1000000 *)

-- | The Keep Alive in effect.  When a client asks for 0, a 5.0 client is
-- sent a Server Keep Alive of this.
effectiveKeepAlive :: Word16 -> Word16
effectiveKeepAlive 0 = 1800
effectiveKeepAlive x = x

-- | How long to wait for a packet from a client before disconnecting
-- it, in microseconds: one and a half times the Keep Alive
-- [3.1.1 MQTT-3.1.2-24], [5.0 MQTT-3.1.2-22].  A 3.1.1 Keep Alive of
-- 0 turns the mechanism off.
keepAliveTimeout :: T.ProtocolLevel -> Word16 -> Maybe Int
keepAliveTimeout T.Protocol311 0 = Nothing
keepAliveTimeout _ ka = Just (seconds (fromIntegral (effectiveKeepAlive ka)) * 3 `div` 2)

asks :: MQTTD :> es => (Env -> a) -> Eff es a
asks f = f <$> getEnv

nextID :: (IOE :> es, MQTTD :> es) => Eff es Int
nextID = asks clientIDGen >>= \ig -> atomically $ modifyTVarRet ig (+1)

retainerCleanup :: (IOE :> es, LogFX :> es, MQTTD :> es, Stats :> es, DB :> es) => Eff es ()
retainerCleanup = asks retainer >>= cleanRetainer

isClientConnected :: SessionID -> TVar (Map SessionID Session) -> STM Bool
isClientConnected sid sidsv = readTVar sidsv >>= \sids -> pure $ isJust (_sessionClient =<< Map.lookup sid sids)

publishStats :: (IOE :> es, MQTTD :> es, Stats :> es, DB :> es, LogFX :> es) => Eff es ()
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

findSubs :: (IOE :> es, MQTTD :> es) => T.Topic -> Eff es [(Session, T.SubOptions)]
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

restoreSessions :: (IOE :> es, ScheduleFX SessionID :> es, MQTTD :> es, Stats :> es, LogFX :> es, DB :> es) => Eff es ()
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

restoreRetained :: (IOE :> es, MQTTD :> es, Stats :> es, LogFX :> es, DB :> es) => Eff es ()
restoreRetained = asks retainer >>= MQTTD.Retention.restoreRetained

subscribe :: (IOE :> es, MQTTD :> es, Stats :> es, LogFX :> es, DB :> es) => Session -> T.SubscribeRequest -> Eff es ()
subscribe sess@Session{..} (T.SubscribeRequest pid topics props) = do
  subs <- asks allSubs
  subsShared <- asks sharedSubs
  ss <- getStatStore
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
    oldSessionSubs <- readTVar _sessionSubs
    -- session copy subscriptions
    modifyTVar' _sessionSubs . Map.union . Map.fromList . rights $ topics'
    -- regular subscriptions
    modifyTVar' subs (upSub new)
    -- shared subscriptions
    modifyTVar' subsShared (upShared shared)
    let added = Map.size $ Map.difference new oldSessionSubs
    incrementStatSTM StatSubscriptions added ss
    let r = map (second (T._subQoS . snd)) topics'
    sendPacket_ _sessionChan (T.SubACKPkt (T.SubscribeResponse pid r props'))
  p <- asks retainer
  mapM_ (doRetained p) (Map.assocs new)
  storeSession sess

  where
    upSub m subs = Map.foldrWithKey (\k x -> SubTree.add k (Map.singleton _sessionID x)) subs m
    upShared m subs = foldr (\(n,f,o) -> SubTree.addWith f (Map.unionWith (<>)) (Map.singleton n [(_sessionID, o)])) subs m

    doRetained _ (_, T.SubOptions{T._retainHandling=T.DoNotSendOnSubscribe}) = pure ()
    doRetained p (t, ops)                                                    = mapM_ (sendOne ops) =<< matchRetained p t

    props' = filter isSubAckProp props
        where
        isSubAckProp (T.PropUserProperty _ _) = True
        isSubAckProp (T.PropReasonString _)   = True
        isSubAckProp _                        = False

    sendOne T.SubOptions{..} ir@T.PublishRequest{..} = do
      now <- liftIO getCurrentTime
      let r = ir{T._pubRetain=True, T._pubQoS = min _pubQoS _subQoS}
      publish (deadline now r) sess r

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
    upShared l s = foldr (\(n,f,_) -> SubTree.modify f (cleanShared n)) s l
      where
        -- Drop this session from the named share group (only), and drop
        -- the group if it's left empty.
        cleanShared n = fmap (Map.filter (not . null) . Map.adjust (filter ((/= sid) . fst)) n)

unsubscribe :: (IOE :> es, MQTTD :> es, Stats :> es) => Session -> [BLFilter] -> Eff es [T.UnsubStatus]
unsubscribe Session{..} topics = asks allSubs >>= \subs -> asks sharedSubs >>= \ssubs -> getStatStore >>= \st ->
  atomically $ do
    m <- readTVar _sessionSubs
    let mtopics = T.mkFilter . blToText <$> topics
        (uns, n) = foldl' (\(r,m') t -> first ((:r) . unm) $ up t m') ([], m) mtopics
    writeTVar _sessionSubs n
    removeSubs ssubs subs _sessionID (catMaybes mtopics)
    let removed = Map.size m - Map.size n
    incrementStatSTM StatSubscriptions (-removed) st
    pure (reverse uns)

    where
      unm = maybe T.UnsubNoSubscriptionExisted (const T.UnsubSuccess)
      up Nothing  = (Nothing,)
      up (Just t) = Map.updateLookupWithKey (const.const $ Nothing) t

modifySession :: (IOE :> es, MQTTD :> es) => SessionID -> (Session -> Maybe Session) -> Eff es ()
modifySession k f = asks sessions >>= \s -> atomically $ modifyTVar' s (Map.update f k)

registerClient :: (IOE :> es, AuthFX :> es, Fail :> es, MQTTD :> es, Stats :> es, DB :> es, LogFX :> es)
               => T.ConnectRequest -> ClientID -> ThreadId -> Eff es (Session, T.SessionReuse)
registerClient req@T.ConnectRequest{..} i o = do
  c <- asks sessions
  subt <- asks allSubs
  ssubt <- asks sharedSubs
  st <- getStatStore
  ai <- liftIO $ newTVarIO mempty
  ao <- liftIO $ newTVarIO mempty
  l <- liftIO $ newTVarIO (fromMaybe 0 (req ^? properties . folded . _PropTopicAliasMaximum))
  acl <- getACL _username
  let k = _connID
      nc = ConnectedClient req o i ai ao l
      maxInFlight = fromMaybe maxBound (req ^? properties . folded . _PropReceiveMaximum)
      dropSubs subsv = do
        subs <- readTVar subsv
        removeSubs ssubt subt k (Map.keys subs)
        incrementStatSTM StatSubscriptions (-Map.size subs) st
  when (maxInFlight == 0) $ fail "max in flight must be greater than zero"
  (o', x, ns, discarded, oldWill) <- atomically $ do
    emptySession <- Session _connID acl (Just nc) <$> newTBQueue defaultQueueSize
                    <*> newTVar maxInFlight <*> newTBQueue defaultQueueSize
                    <*> newTVar mempty <*> newTVar mempty
                    <*> pure Nothing <*> pure _lastWill <*> pure Nothing
                    <*> newTVar 0 <*> newTVar mempty
    m <- readTVar c
    let s = Map.lookup k m
        o' = _sessionClient =<< s
        (ns, ruse) = maybeClean emptySession s
        discarded = if _cleanSession then s else Nothing
    mapM_ (dropSubs . _sessionSubs) discarded
    writeTVar c (Map.insert k ns m)
    pure (o', ruse, ns, discarded, replacedWill =<< s)
  -- The cascade removes the discarded session's persisted subscriptions.
  when (isJust discarded) $ deleteSession k
  mapM_ (publishWill k) oldWill
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
                         _sessionWill=_lastWill,
                         _sessionWillAt=Nothing}, T.ExistingSession)

      -- The will of the connection being taken over, or a delayed will
      -- still pending on a disconnected session.  A new connection to the
      -- session cancels a delayed will [MQTT-3.1.3-9], but it's sent if
      -- the session is ending (clean start) or a taken over connection had
      -- no delay [MQTT-3.1.4-3].
      replacedWill old = do
        w <- _sessionWill old
        let connected = isJust (_sessionClient old)
        guard (connected || isJust (_sessionWillAt old))
        guard (_cleanSession || (connected && willDelay w == 0))
        pure w

expireSession :: (IOE :> es, ScheduleFX SessionID :> es, MQTTD :> es, Stats :> es, DB :> es, LogFX :> es) => SessionID -> Eff es ()
expireSession k = do
  now <- liftIO getCurrentTime
  ss <- asks sessions
  -- Take a due will (atomically, so it's only sent once) and send it.
  (due, current) <- atomically $ do
    m <- readTVar ss
    case Map.lookup k m of
      Just sess@Session{_sessionClient=Nothing, _sessionWillAt=Just t, _sessionWill=Just w}
        | t <= now -> do
            let sess' = sess{_sessionWill=Nothing, _sessionWillAt=Nothing}
            writeTVar ss (Map.insert k sess' m)
            pure (Just w, Just sess')
      current -> pure (Nothing, current)
  mapM_ (publishWill k) due
  possiblyCleanup now current

  where
    possiblyCleanup _ Nothing = pure ()
    possiblyCleanup _ (Just Session{_sessionClient=Just _}) = logDbgL [tshow k, " is in use"]
    possiblyCleanup _ (Just Session{_sessionClient=Nothing,
                                    _sessionExpires=Nothing}) = expireNow
    possiblyCleanup now (Just Session{_sessionClient=Nothing,
                                      _sessionExpires=Just ex,
                                      _sessionWillAt=willAt})
      | ex > now = do
          void $ enqueue ex k
          -- Come back for a delayed will, unless the session ends first
          -- (which sends it).
          mapM_ (void . (`enqueue` k)) (mfilter (< ex) willAt)
      | otherwise = expireNow

    expireNow = do
      now <- liftIO getCurrentTime
      ss <- asks sessions
      kilt <- atomically $ do
        current <- Map.lookup k <$> readTVar ss
        case current of
          -- Re-checked here, since the client may have reconnected.
          Just Session{_sessionClient=Nothing, _sessionExpires=Nothing} ->
            modifyTVar' ss (Map.delete k) >> pure current
          Just Session{_sessionClient=Nothing, _sessionExpires=Just ex}
            | now >= ex -> modifyTVar' ss (Map.delete k) >> pure current
          _ -> pure Nothing
      case kilt of
        Nothing -> logDbgL ["Nothing expired for ", tshow k]
        Just s@Session{..}  -> do
          logDbgL ["Expired session for ", tshow k]
          subt <- asks allSubs
          ssubst <- asks sharedSubs
          st <- getStatStore
          atomically $ do
            subs <- readTVar _sessionSubs
            removeSubs ssubst subt _sessionID (Map.keys subs)
            incrementStatSTM StatSubscriptions (-Map.size subs) st
          deleteSession _sessionID
          sessionDied s

    sessionDied Session{_sessionWill=Nothing} =
      logDbgL ["Session without will: ", tshow k, " has died"]
    sessionDied Session{_sessionWill=Just w} = do
      logDbgL ["Session with will ", tshow k, " has died"]
      publishWill k w

-- | The Will Delay Interval of a will (0 if absent, as always in 3.1.1).
willDelay :: T.LastWill -> NominalDiffTime
willDelay w = maybe 0 fromIntegral (T._willProps w ^? folded . _PropWillDelayInterval)

-- | The PUBLISH a will is sent as.  Will Delay Interval is only a will
-- property and isn't allowed on PUBLISH [5.0 §3.3.2.3].
willPublish :: T.LastWill -> T.PublishRequest
willPublish T.LastWill{..} = T.PublishRequest{
  T._pubDup=False,
  T._pubQoS=_willQoS,
  T._pubRetain=_willRetain,
  T._pubTopic=_willTopic,
  T._pubPktID=0,
  T._pubBody=_willMsg,
  T._pubProps=filter (hasn't _PropWillDelayInterval) _willProps}

publishWill :: (IOE :> es, MQTTD :> es, Stats :> es, DB :> es, LogFX :> es) => SessionID -> T.LastWill -> Eff es ()
publishWill k w = do
  logDbgL ["Publishing will for ", tshow k]
  broadcast Nothing (willPublish w)

unregisterClient :: (IOE :> es, MQTTD :> es, ScheduleFX SessionID :> es, LogFX :> es, Stats :> es, DB :> es) => T.ProtocolLevel -> SessionID -> ClientID -> Eff es ()
unregisterClient pl k mid = do
  now <- liftIO getCurrentTime
  modifySession k (up now)
  expireSession k

    where
      up now sess@Session{_sessionClient=Just cc@ConnectedClient{_clientID=i,
                                                                 _clientConnReq=creq}}
        -- A will left on the session (i.e. not discarded by DISCONNECT) is
        -- due after its delay [MQTT-3.1.2-8], [5.0 §3.1.3.2.2].
        | mid == i = Just $ sess{_sessionExpires=expiry, _sessionClient=Nothing,
                                 _sessionWillAt=(\w -> addUTCTime (willDelay w) now) <$> _sessionWill sess}
        where
          expiry = case pl of
            T.Protocol311
              | T._cleanSession creq -> Nothing
              | otherwise -> Just (addUTCTime defaultSessionExp now)
            T.Protocol50
              | Just 0 <- creq ^? properties . folded . _PropSessionExpiryInterval -> Nothing
              | Just x <- creq ^? properties . folded . _PropSessionExpiryInterval ->
                  Just (addUTCTime (fromIntegral x) now)
              | otherwise -> Nothing
            _ -> Just (addUTCTime defaultSessionExp now)
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

-- | The next packet ID after the given one that isn't in use, wrapping
-- around and skipping 0, or Nothing if every ID is in use.
nextPktID :: (T.PktID -> Bool) -> T.PktID -> Maybe T.PktID
nextPktID inUse lastID = listToMaybe [i | i <- candidates, not (inUse i)]
  where
    candidates = take (fromIntegral (maxBound :: T.PktID)) (drop 1 (iterate next lastID))
    next i = if i == maxBound then 1 else succ i

broadcast :: (IOE :> es, MQTTD :> es, Stats :> es, DB :> es, LogFX :> es) => Maybe SessionID -> T.PublishRequest -> Eff es ()
broadcast src req@T.PublishRequest{..} = do
  asks retainer >>= retain req
  subs <- maybe (pure []) findSubs ((T.mkTopic . blToText) _pubTopic)
  beforeF <- deadline <$> liftIO getCurrentTime
  mapM_ (\(s@Session{..}, o) -> justM (\p -> publish (beforeF p) s p) (pkt _sessionID o)) subs
  where
    pkt sid T.SubOptions{T._noLocal=True}
      | Just sid == src = Nothing
    -- publish assigns the packet ID.
    pkt _ opts = Just req{
      T._pubDup=False,
      T._pubRetain=mightRetain opts,
      T._pubQoS=maxQoS opts,
      T._pubPktID=0}

    maxQoS T.SubOptions{_subQoS} = min _pubQoS _subQoS
    mightRetain T.SubOptions{_retainAsPublished=False} = False
    mightRetain _                                      = _pubRetain

publish :: (IOE :> es, MQTTD :> es, Stats :> es) => Maybe UTCTime -> Session -> T.PublishRequest -> Eff es ()
publish before sess@Session{..} pkt@T.PublishRequest{..} = whenM deliverable publish'
  where
    deliverable = maybe (pure True) (\t -> liftIO getCurrentTime >>= \now -> pure (now < t)) before
    publish'
      -- QoS 0 is special-cased because it's fire-and-forget with no retries or anything.
      | _pubQoS == T.QoS0 = getStatStore >>= \ss -> atomically . void $ deliver ss sess pkt
      | otherwise = getStatStore >>= \ss -> atomically $ do
          -- Packet IDs are per session, and an ID can't be reused while
          -- it's in flight [MQTT-2.2.1-3].
          inFlight <- readTVar _sessionQP
          rel <- readTVar _sessionRel
          nextPktID (\i -> Map.member i inFlight || Set.member i rel) <$> readTVar _sessionPktID >>= \case
            Nothing -> pure () -- every packet ID is in flight; drop it
            Just pid -> do
              writeTVar _sessionPktID pid
              let pkt' = pkt{T._pubPktID=pid}
              modifyTVar' _sessionQP $ Map.insert pid (before, pkt')
              tokens <- readTVar _sessionFlight
              if tokens == 0
                then void $ tryWriteQ _sessionBacklog (before, pkt')
                else void $ deliver ss sess pkt'

-- | Send a PUBLISH to a session, returning whether it was sent.
deliver :: StatStore -> Session -> T.PublishRequest -> STM Bool
deliver ss Session{..} pkt@T.PublishRequest{..}
  | maybe False (`tooLargeFor` pkt) _sessionClient = do
      -- Discard it as if it had been delivered [MQTT-3.1.2-25].
      modifyTVar' _sessionQP (Map.delete _pubPktID)
      pure False
  | otherwise = do
      when (_pubQoS > T.QoS0) $ modifyTVar' _sessionFlight pred
      p <- maybe (pure pkt) (`aliasOut` pkt) _sessionClient
      sendPacket_ _sessionChan (T.PublishPkt p)
      incrementStatSTM StatMsgSent 1 ss
      pure True

-- | Whether a PUBLISH is bigger than the client's Maximum Packet Size
-- [MQTT-3.1.2-24].  It's measured before any topic alias is applied,
-- which can only make it smaller.
tooLargeFor :: ConnectedClient -> T.PublishRequest -> Bool
tooLargeFor ConnectedClient{_clientConnReq} p =
  maybe False (\m -> BL.length (T.toByteString T.Protocol50 (T.PublishPkt p)) > fromIntegral m)
        (_clientConnReq ^? properties . folded . _PropMaximumPacketSize)

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

releasePubSlot :: UTCTime -> StatStore -> Session -> STM ()
releasePubSlot now ss sess@Session{..} = do
  modifyTVar' _sessionFlight succ
  getOne
  where
    getOne = tryReadTBQueue _sessionBacklog >>= \case
      Nothing -> pure () -- Nothing to do
      Just (Nothing, p) -> deliverOrNext p -- No deadline
      Just (Just t, p) | now < t -> deliverOrNext p -- Deadline not yet reached
      -- This one's expired [MQTT-3.3.2-5].  It's no longer in flight, so
      -- free its packet ID and see if there's another one.
      Just (Just _, p) -> modifyTVar' _sessionQP (Map.delete (p ^. pktID)) >> getOne
    -- If it wasn't sent (e.g. it's too big), the slot is still free.
    deliverOrNext p = deliver ss sess p >>= \sent -> unless sent getOne

dispatch :: (IOE :> es, Fail :> es, MQTTD :> es, DB :> es, LogFX :> es, Stats :> es) => Session -> T.MQTTPkt -> Eff es ()

dispatch Session{..} T.PingPkt = sendPacketIO_ _sessionChan T.PongPkt

-- QoS 1 ACK (receiving client got our publish message)
dispatch sess@Session{..} (T.PubACKPkt ack) = liftIO getCurrentTime >>= \now -> getStatStore >>= \st -> atomically $ do
  modifyTVar' _sessionQP (Map.delete (ack ^. pktID))
  releasePubSlot now st sess

-- QoS 2 ACK (receiving client received our message)
dispatch Session{..} (T.PubRECPkt ack) = atomically $ do
  let pid = ack ^. pktID
  (r, m) <- Map.updateLookupWithKey (const.const $ Nothing) pid <$> readTVar _sessionQP
  writeTVar _sessionQP m
  -- Keep the PUBREL state until PUBCOMP so it can be resent
  -- [5.0 MQTT-4.4.0-1], [3.1.1 §4.4].
  when (isJust r) $ modifyTVar' _sessionRel (Set.insert pid)
  sendPacket_ _sessionChan (T.PubRELPkt $ T.PubREL pid 0 mempty)

-- QoS 2 REL (publishing client says we can ship the message)
dispatch Session{..} (T.PubRELPkt rel) = do
  pkt <- atomically $ do
    (r, m) <- Map.updateLookupWithKey (const.const $ Nothing) (rel ^. pktID) <$> readTVar _sessionQP
    writeTVar _sessionQP m
    _ <- sendPacket _sessionChan (T.PubCOMPPkt (T.PubCOMP (rel ^. pktID) (maybe 0x92 (const 0) r) mempty))
    pure r
  justM (broadcast (Just _sessionID) . snd) pkt

-- QoS 2 COMPlete (publishing client says publish is complete)
dispatch sess@Session{..} (T.PubCOMPPkt comp) = liftIO getCurrentTime >>= \now -> getStatStore >>= \st -> atomically $ do
  modifyTVar' _sessionRel (Set.delete (comp ^. pktID))
  releasePubSlot now st sess

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
      satisfyQoS T.QoS2 r@T.PublishRequest{..} = liftIO getCurrentTime >>= \now -> getStatStore >>= \ss -> atomically $ do
        sendPacket_ _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0 mempty))
        modifyTVar' _sessionQP (Map.insert _pubPktID (deadline now r, r))
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
