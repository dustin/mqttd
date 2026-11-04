{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE MultiWayIf                 #-}
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

-- | The Topic Alias Maximum the server sends in CONNACK.
topicAliasMaximum :: Word16
topicAliasMaximum = 100

-- | The Receive Maximum the server sends in CONNACK: how many QoS 2
-- PUBLISHes a 5.0 client may have waiting for PUBREL [5.0 §3.3.4].
receiveMaximum :: Word16
receiveMaximum = 100

-- | Resolve a PUBLISH's topic alias, or give the reason it's a protocol
-- error [5.0 §3.3.2.3.4].
resolveAliasIn :: MonadIO m => Session -> T.PublishRequest -> m (Either T.DiscoReason T.PublishRequest)
resolveAliasIn Session{_sessionClient=Nothing} r = pure (Right r)
resolveAliasIn Session{_sessionClient=Just ConnectedClient{_clientAliasIn}} r =
  case r ^? properties . folded . _PropTopicAlias of
    Nothing -> pure (Right r)
    Just n
      | n == 0 || n > topicAliasMaximum -> pure (Left T.DiscoTopicAliasInvalid)
      | otherwise -> resolve n r

  where
    resolve n T.PublishRequest{_pubTopic} = do
      mt <- atomically $ do
        when (_pubTopic /= "") $ modifyTVar' _clientAliasIn (Map.insert n _pubTopic)
        Map.lookup n <$> readTVar _clientAliasIn
      -- An empty topic with an alias that was never set.
      pure $ maybe (Left T.DiscoProtocolError) (\t -> Right (r & pubTopic .~ t & properties %~ cleanProps)) mt
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
  -- Shared subscriptions go back in their own tree, as subscribe puts them.
  (shared, normal) <- partitionShared . fold <$> traverse sessSubs ss
  let subs = SubTree.fromList [(f, Map.singleton sid o) | (f, (sid, o)) <- normal]
      ssubs = foldr (\(n, f, (sid, o)) -> SubTree.addWith f (Map.unionWith (<>)) (Map.singleton n [(sid, o)])) mempty shared
  sessv <- asks sessions
  subv <- asks allSubs
  ssubv <- asks sharedSubs
  atomically $ do
    writeTVar sessv (Map.fromList . map (\s@Session{..} -> (_sessionID, s)) $ ss)
    writeTVar subv subs
    writeTVar ssubv ssubs
  mapM_ (expireSession . _sessionID) ss

  where
    sessSubs :: MonadIO m => Session -> m [(T.Filter, (SessionID, T.SubOptions))]
    sessSubs Session{..} = map (second (_sessionID,)) . Map.assocs <$> readTVarIO _sessionSubs

restoreRetained :: (IOE :> es, MQTTD :> es, Stats :> es, LogFX :> es, DB :> es) => Eff es ()
restoreRetained = asks retainer >>= MQTTD.Retention.restoreRetained

subscribe :: (IOE :> es, MQTTD :> es, Stats :> es, LogFX :> es, DB :> es) => Session -> T.SubscribeRequest -> Eff es ()
subscribe sess@Session{..} (T.SubscribeRequest pid topics props) = do
  subs <- asks allSubs
  subsShared <- asks sharedSubs
  ss <- getStatStore
  let topics' = map (\(t,o) ->
                       case (T.mkFilter . blToText) t of
                         Just t' | classifyTopic t' /= InvalidTopic ->
                           bimap (const (subErr T.SubErrNotAuthorized))
                                 (const (t', o))
                                 (authTopic (classifyTopic t') IntentSubscribe _sessionACL)
                         _ -> Left (subErr T.SubErrTopicFilterInvalid)) topics
      -- 3.1.1 has only the one failure code [MQTT-3.9.3-2].
      subErr e = if sessionIs50 sess then e else T.SubErrUnspecifiedError
      (shared, normal) = partitionShared (rights topics')
      new = Map.fromList normal
  existing <- atomically $ do
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
    pure oldSessionSubs
  p <- asks retainer
  mapM_ (doRetained p existing) (Map.assocs new)
  storeSession sess

  where
    upSub m subs = Map.foldrWithKey (\k x -> SubTree.add k (Map.singleton _sessionID x)) subs m
    upShared m subs = foldr (\(n,f,o) -> SubTree.addWith f (Map.unionWith (<>)) (Map.singleton n [(_sessionID, o)])) subs m

    doRetained _ _ (_, T.SubOptions{T._retainHandling=T.DoNotSendOnSubscribe}) = pure ()
    -- Only for a subscription that didn't already exist [MQTT-3.3.1-10].
    doRetained _ existing (t, T.SubOptions{T._retainHandling=T.SendOnSubscribeNew})
      | Map.member t existing = pure ()
    doRetained p _ (t, ops) = mapM_ (sendOne ops) =<< matchRetained p t

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
      unm (Left e)  = e
      unm (Right r) = maybe T.UnsubNoSubscriptionExisted (const T.UnsubSuccess) r
      up (Just t) | classifyTopic t /= InvalidTopic = first Right . Map.updateLookupWithKey (const.const $ Nothing) t
      -- [5.0 §3.11.3]
      up _ = (Left T.UnsubTopicFilterInvalid,)

modifySession :: (IOE :> es, MQTTD :> es) => SessionID -> (Session -> Maybe Session) -> Eff es ()
modifySession k f = asks sessions >>= \s -> atomically $ modifyTVar' s (Map.update f k)

registerClient :: (IOE :> es, AuthFX :> es, Fail :> es, MQTTD :> es, Stats :> es, DB :> es, LogFX :> es)
               => T.ProtocolLevel -> T.ConnectRequest -> ClientID -> ThreadId -> Eff es (Session, T.SessionReuse)
registerClient pl req@T.ConnectRequest{..} i o = do
  c <- asks sessions
  subt <- asks allSubs
  ssubt <- asks sharedSubs
  st <- getStatStore
  ai <- liftIO $ newTVarIO mempty
  ao <- liftIO $ newTVarIO mempty
  l <- liftIO $ newTVarIO (fromMaybe 0 (req ^? properties . folded . _PropTopicAliasMaximum))
  acl <- getACL _username
  let k = _connID
      nc = ConnectedClient req o i ai ao l pl
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
                    <*> newTVar 0 <*> newTVar mempty <*> newTVar mempty
    m <- readTVar c
    let s = Map.lookup k m
        o' = _sessionClient =<< s
        -- A session that ends with its connection ends when it's taken
        -- over, too [MQTT-3.1.2-6].
        clean = _cleanSession || any endsWithConnection o'
        (ns, ruse) = maybeClean clean emptySession s
        discarded = if clean then s else Nothing
    mapM_ (dropSubs . _sessionSubs) discarded
    -- The connection being taken over is told why [5.0 §3.1.4].
    case (s, o') of
      (Just Session{_sessionChan=oldChan}, Just ConnectedClient{_clientProtocol=T.Protocol50}) ->
        sendPacket_ oldChan (T.DisconnectPkt (T.DisconnectRequest T.DiscoSessiontakenOver []))
      _ -> pure ()
    writeTVar c (Map.insert k ns m)
    pure (o', ruse, ns, discarded, replacedWill clean =<< s)
  -- The cascade removes the discarded session's persisted subscriptions.
  when (isJust discarded) $ deleteSession k
  mapM_ (publishWill k) oldWill
  case o' of
    Nothing                  -> pure ()
    Just ConnectedClient{..} -> liftIO $ throwTo _clientThread (MQTTDuplicate _connID)
  pure (ns, x)

    where
      maybeClean _ ns Nothing = (ns, T.NewSession)
      maybeClean clean ns (Just s)
        | clean = (ns, T.NewSession)
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
      replacedWill clean old = do
        w <- _sessionWill old
        let connected = isJust (_sessionClient old)
        guard (connected || isJust (_sessionWillAt old))
        guard (clean || (connected && willDelay w == 0))
        pure w

-- | Whether a connected client's session ends when its connection does:
-- CleanSession=1 in 3.1.1, or a Session Expiry Interval of 0 in 5.0
-- [MQTT-3.1.2-6], [5.0 §3.1.2.11.2].
endsWithConnection :: ConnectedClient -> Bool
endsWithConnection ConnectedClient{_clientProtocol, _clientConnReq=req}
  | _clientProtocol == T.Protocol50 = maybe True (== 0) (req ^? properties . folded . _PropSessionExpiryInterval)
  | otherwise = T._cleanSession req

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

-- | Set a PUBLISH's Message Expiry Interval to the time left before its
-- deadline [MQTT-3.3.2-6].
adjExpiry :: UTCTime -> Maybe UTCTime -> T.PublishRequest -> T.PublishRequest
adjExpiry _ Nothing p    = p
adjExpiry now (Just t) p = p & properties . traversed . _PropMessageExpiryInterval .~ relExp now t

releasePubSlot :: UTCTime -> StatStore -> Session -> STM ()
releasePubSlot now ss sess@Session{..} = do
  modifyTVar' _sessionFlight succ
  getOne
  where
    getOne = tryReadTBQueue _sessionBacklog >>= \case
      Nothing -> pure () -- Nothing to do
      Just (Nothing, p) -> deliverOrNext p -- No deadline
      -- Deadline not yet reached.  It goes out with the time it has
      -- left [MQTT-3.3.2-6].
      Just (Just t, p) | now < t -> deliverOrNext (adjExpiry now (Just t) p)
      -- This one's expired [MQTT-3.3.2-5].  It's no longer in flight, so
      -- free its packet ID and see if there's another one.
      Just (Just _, p) -> modifyTVar' _sessionQP (Map.delete (p ^. pktID)) >> getOne
    -- If it wasn't sent (e.g. it's too big), the slot is still free.
    deliverOrNext p = deliver ss sess p >>= \sent -> unless sent getOne

-- | Whether the session's connected client speaks MQTT 5.0.
sessionIs50 :: Session -> Bool
sessionIs50 Session{_sessionClient} = (_sessionClient ^? _Just . clientProtocol) == Just T.Protocol50

-- | Whether a client with the given ACL may have this will.
authWill :: [ACL] -> T.LastWill -> Either String ()
authWill acl T.LastWill{_willTopic} = case classifyTopic (fromString (Txt.unpack (blToText _willTopic))) of
  t@(Normal _) -> authTopic t IntentPublish acl
  _            -> Left "invalid will topic"

dispatch :: (IOE :> es, Fail :> es, MQTTD :> es, DB :> es, LogFX :> es, Stats :> es) => Session -> T.MQTTPkt -> Eff es ()

dispatch Session{..} T.PingPkt = sendPacketIO_ _sessionChan T.PongPkt

-- QoS 1 ACK (receiving client got our publish message)
-- Only an ack for a message in flight frees its slot [5.0 MQTT-3.3.4-7].
dispatch sess@Session{..} (T.PubACKPkt ack) = liftIO getCurrentTime >>= \now -> getStatStore >>= \st -> atomically $ do
  (r, m) <- Map.updateLookupWithKey (const.const $ Nothing) (ack ^. pktID) <$> readTVar _sessionQP
  writeTVar _sessionQP m
  when (isJust r) $ releasePubSlot now st sess

-- QoS 2 ACK (receiving client received our message)
dispatch sess@Session{..} (T.PubRECPkt (T.PubREC pid st _)) = liftIO getCurrentTime >>= \now -> getStatStore >>= \ss -> atomically $ do
  (r, m) <- Map.updateLookupWithKey (const.const $ Nothing) pid <$> readTVar _sessionQP
  writeTVar _sessionQP m
  known <- (isJust r ||) . Set.member pid <$> readTVar _sessionRel
  if
    -- An error code ends the exchange: no PUBREL, and the slot is free
    -- [5.0 §4.3.3], [5.0 §4.9].
    | st >= 0x80 -> when (isJust r) $ releasePubSlot now ss sess
    -- Keep the PUBREL state until PUBCOMP so it can be resent
    -- [5.0 MQTT-4.4.0-1], [3.1.1 §4.4].  (A repeated PUBREC gets the
    -- PUBREL again.)
    | known -> do
        modifyTVar' _sessionRel (Set.insert pid)
        sendPacket_ _sessionChan (T.PubRELPkt $ T.PubREL pid 0 mempty)
    -- Packet Identifier not found [5.0 §3.6.2.1].
    | otherwise -> sendPacket_ _sessionChan (T.PubRELPkt $ T.PubREL pid 0x92 mempty)

-- QoS 2 REL (publishing client says we can ship the message)
dispatch Session{..} (T.PubRELPkt rel) = do
  pkt <- atomically $ do
    (r, m) <- Map.updateLookupWithKey (const.const $ Nothing) (rel ^. pktID) <$> readTVar _sessionInbound
    writeTVar _sessionInbound m
    _ <- sendPacket _sessionChan (T.PubCOMPPkt (T.PubCOMP (rel ^. pktID) (maybe 0x92 (const 0) r) mempty))
    pure r
  justM (broadcast (Just _sessionID) . snd) pkt

-- QoS 2 COMPlete (publishing client says publish is complete)
dispatch sess@Session{..} (T.PubCOMPPkt comp) = liftIO getCurrentTime >>= \now -> getStatStore >>= \st -> atomically $ do
  let pid = comp ^. pktID
  known <- Set.member pid <$> readTVar _sessionRel
  modifyTVar' _sessionRel (Set.delete pid)
  when known $ releasePubSlot now st sess

-- Subscribe response is sent from the `subscribe` action because the
-- interaction is a bit complicated.
-- mqttd doesn't support Subscription Identifiers (and CONNACK says so),
-- so one in a SUBSCRIBE is a protocol error [5.0 §3.8.2.1.2].
dispatch Session{..} (T.SubscribePkt (T.SubscribeRequest _ _ props))
  | any (has _PropSubscriptionIdentifier) props = do
      sendPacketIO_ _sessionChan (T.DisconnectPkt (T.DisconnectRequest T.DiscoSubscriptionIdentifiersNotSupported []))
      fail "subscription identifiers aren't supported"
-- No Local on a shared subscription is a protocol error [MQTT-3.8.3-4].
dispatch Session{..} (T.SubscribePkt (T.SubscribeRequest _ subs _))
  | any noLocalShared subs = do
      sendPacketIO_ _sessionChan (T.DisconnectPkt (T.DisconnectRequest T.DiscoProtocolError []))
      fail "No Local on a shared subscription"
  where noLocalShared (f, T.SubOptions{T._noLocal}) =
          _noLocal && maybe False (\case SharedSubscription{} -> True; _ -> False) (classifyTopic <$> T.mkFilter (blToText f))
dispatch sess (T.SubscribePkt req) = subscribe sess req

dispatch sess@Session{..} (T.UnsubscribePkt (T.UnsubscribeRequest pid subs props)) = do
  uns <- unsubscribe sess subs
  sendPacketIO_ _sessionChan (T.UnsubACKPkt (T.UnsubscribeResponse pid props uns))

dispatch sess@Session{..} (T.PublishPkt req) = resolveAliasIn sess req >>= either protocolError pub

    where
      -- 3.1.1 has no DISCONNECT from the server; the connection is just closed.
      protocolError reason = do
        when is50 $ sendPacketIO_ _sessionChan (T.DisconnectPkt (T.DisconnectRequest reason []))
        fail ("protocol error: " <> show reason)

      pub r@T.PublishRequest{..}
        -- A topic name can't contain wildcards [MQTT-3.3.2-2].
        | BL.any (`elem` [0x23, 0x2b]) _pubTopic = protocolError T.DiscoTopicNameInvalid
        | otherwise =
            case authPub (maybe InvalidTopic classifyTopic $ (T.mkFilter . blToText) _pubTopic) _sessionACL of
              Left _  -> logInfoL ["Unauthorized topic: ", tshow _pubTopic] >> nak _pubQoS r
              Right _ -> satisfyQoS _pubQoS r

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
      satisfyQoS T.QoS2 r@T.PublishRequest{..} = do
        now <- liftIO getCurrentTime
        ss <- getStatStore
        ok <- atomically $ do
          inbound <- readTVar _sessionInbound
          -- A 5.0 client may not have more than our Receive Maximum QoS 2
          -- messages unreleased [5.0 §3.3.4].  (A resend doesn't count.)
          if is50 && Map.size inbound >= fromIntegral receiveMaximum && Map.notMember _pubPktID inbound
            then pure False
            else do
              sendPacket_ _sessionChan (T.PubRECPkt (T.PubREC _pubPktID 0 mempty))
              modifyTVar' _sessionInbound (Map.insert _pubPktID (deadline now r, r))
              incrementStatSTM StatMsgRcvd 1 ss
              pure True
        unless ok $ protocolError T.DiscoReceiveMaximumExceeded

      is50 = sessionIs50 sess

      countIn = incrementStat StatMsgRcvd 1

      authPub :: TopicType -> [ACL] -> Either String ()
      authPub t acl = case t of
                        Normal _ -> Right ()
                        _        -> Left "invalid topic"
                      >> authTopic t IntentPublish acl

dispatch sess@Session{..} (T.DisconnectPkt (T.DisconnectRequest reason props))
  | reason `elem` [T.DiscoNormalDisconnection, T.DiscoDisconnectWithWill] = do
      let creq = sess ^?! sessionClient . _Just . clientConnReq
          sid = creq ^. connID
          connExp = creq ^? properties . folded . _PropSessionExpiryInterval
          discoExp = props ^? folded . _PropSessionExpiryInterval
      -- Setting a Session Expiry Interval when CONNECT's was 0 is a
      -- protocol error [MQTT-3.14.2-2].
      when (maybe True (== 0) connExp && maybe False (/= 0) discoExp) $ do
        sendPacketIO_ _sessionChan (T.DisconnectPkt (T.DisconnectRequest T.DiscoProtocolError []))
        fail "session expiry set on DISCONNECT after CONNECT's was 0"
      -- Otherwise it replaces CONNECT's [5.0 §3.14.2.2.2].
      mapM_ (\x -> modifySession sid (Just . over (sessionClient . _Just . clientConnReq . properties) (setExpiry x))) discoExp
      -- A normal disconnection discards the will [MQTT-3.14.4-3].
      when (reason == T.DiscoNormalDisconnection) $ modifySession sid (Just . set sessionWill Nothing)

    where
      setExpiry x ps = T.PropSessionExpiryInterval x : filter (hasn't _PropSessionExpiryInterval) ps

-- No authentication methods are supported, so AUTH is a protocol error
-- [5.0 §4.12].
dispatch Session{..} (T.AuthPkt _) = do
  sendPacketIO_ _sessionChan (T.DisconnectPkt (T.DisconnectRequest T.DiscoProtocolError []))
  fail "AUTH without an authentication method"

dispatch _ x = fail ("unhandled: " <> show x)
