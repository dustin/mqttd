{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeApplications  #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module MQTTD.DB where

import           Control.Concurrent.STM          (TBQueue, check, flushTBQueue, isEmptyTBQueue, newTBQueueIO, newTVarIO,
                                                  readTVarIO)
import           Control.Lens
import           Control.Monad                   (forever)
import           Control.Monad.IO.Class          (MonadIO (..))
import           Control.Monad.Logger            (MonadLogger (..), logDebugN)
import qualified Data.Attoparsec.ByteString.Lazy as A
import qualified Data.ByteString.Lazy            as BL
import qualified Data.Map.Strict                 as Map
import           Data.Maybe                      (fromMaybe)
import           Data.String                     (fromString)
import           Data.Time.Clock                 (addUTCTime, getCurrentTime)
import           Database.SQLite.Simple          hiding (bind, close)
import           Database.SQLite.Simple.ToField
import           Text.RawString.QQ               (r)
import           UnliftIO                        (atomically, writeTBQueue)

import qualified Network.MQTT.Lens               as T
import qualified Network.MQTT.Topic              as T
import qualified Network.MQTT.Types              as T

import           MQTTD.Types
import           MQTTD.Util

class Monad m => HasDBConnection m where
  dbConn :: m Connection
  dbQueue :: m (TBQueue DBOperation)

data DBOperation = DeleteSession SessionID
                 | StoreSession Session
                 | DeleteRetained BLTopic
                 | StoreRetained Retained

initQueries :: [(Int, Query)]
initQueries = [
  (1, [r|create table if not exists
         sessions (session_id text primary key,
         expiry integer,
         will_topic text,
         will_retain bool,
         will_qos integer,
         will_body blob,
         will_props blob
         )|]),
  (1, [r|create table if not exists
         session_subs (session_id text, topic text,
                       retain_handling text,
                       retain_as_published bool,
                       no_local bool,
                       qos integer,
                       foreign key (session_id) references sessions(session_id) on delete cascade
                       )|]),
  (1, [r|create index session_subs_bysession on session_subs(session_id)|]),
  (1, [r|create table if not exists
         persisted (topic text primary key, qos int, value blob, properties blob, stored datetime, expires datetime)|])
  ]

initTables :: Connection -> IO ()
initTables db = do
  [Only uv] <- query_ db "pragma user_version"
  mapM_ (execute_ db) [q | (v,q) <- initQueries, v > uv]
  -- binding doesn't work on this because it happens at query compile time.
  execute_ db $ "pragma user_version = " <> (fromString . show . maximum . fmap fst $ initQueries)

initDB :: Connection -> IO ()
initDB db = do
  mapM_ (execute_ db) ["pragma foreign_keys = ON"]
  initTables db

runOperations :: (HasDBConnection m, MonadIO m, MonadLogger m) => m ()
runOperations = do
  db <- dbConn
  q <- dbQueue
  forever $ go db q
    where
      go db q = do
        ops <- atomically $ do
          check . not =<< isEmptyTBQueue q
          flushTBQueue q
        store ops
          where
            store ops = do
              logDebugN ("Storing a batch of " <> tshow (length ops) <> " operations")
              liftIO . withTransaction db $
                mapM_ store1 ops
            store1 (DeleteSession i) = deleteSessionL db i
            store1 (DeleteRetained i) = deleteRetainedL db i
            store1 (StoreSession i) = storeSessionL db i
            store1 (StoreRetained i) = storeRetainedL db i

writeDBQueue :: (HasDBConnection m, MonadIO m) => DBOperation -> m ()
writeDBQueue o = dbQueue >>= \q -> writeTBQueueIO q o
  where
    writeTBQueueIO q = liftIO . atomically . writeTBQueue q

deleteSessionL :: MonadIO m => Connection -> SessionID -> m ()
deleteSessionL db sid = liftIO $ execute db "delete from sessions where session_id = ?" (Only sid)

deleteSession :: (HasDBConnection m, MonadIO m) => SessionID -> m ()
deleteSession = writeDBQueue . DeleteSession

deleteRetained :: (HasDBConnection m, MonadIO m) => BLTopic -> m ()
deleteRetained = writeDBQueue . DeleteRetained

deleteRetainedL :: MonadIO m => Connection -> BLTopic -> m ()
deleteRetainedL db k = liftIO $ execute db "delete from persisted where topic = ?" (Only k)

storeSession :: (HasDBConnection m, MonadIO m) => Session -> m ()
storeSession = writeDBQueue . StoreSession

instance ToRow Session where
  toRow s@Session{..} = [
    toField _sessionID,
    toField ((fst . properFraction $ sto) :: Int),
    toField (s ^? sessionWill . _Just . T.willTopic),
    toField (s ^? sessionWill . _Just . T.willRetain),
    toField (s ^? sessionWill . _Just . T.willQoS . to fromEnum),
    toField (s ^? sessionWill . _Just . T.willMsg),
    toField (s ^? sessionWill . _Just . T.willProps . to (T.bsProps T.Protocol50))
    ]
    where
      sto = fromMaybe defaultSessionExp (_sessionClient ^? _Just . clientConnReq . T.properties . folded . T._PropSessionExpiryInterval . to fromIntegral)

storeSessionL :: MonadIO m => Connection -> Session -> m ()
storeSessionL db sess@Session{..} = liftIO $ do
  -- TODO:  For a session with an already-absolute expiry, try to expire at the right time.
  execute db [r|insert into sessions (session_id, expiry,
                                      will_topic, will_retain,
                                      will_qos, will_body, will_props) values (?, ?, ?, ?, ?, ?, ?)
                 on conflict (session_id)
                 do update
                   set expiry = excluded.expiry,
                       will_topic = excluded.will_topic,
                       will_retain = excluded.will_retain,
                       will_qos = excluded.will_qos,
                       will_body = excluded.will_body,
                       will_props = excluded.will_props|] sess

  execute db "delete from session_subs where session_id = ?" (Only _sessionID)
  executeMany db [r|insert into session_subs
                    (session_id, topic, retain_handling, retain_as_published, no_local, qos)
                     values (?, ?, ?, ?, ?, ?)|] =<< subs

  where
    subs = Map.foldMapWithKey (\t T.SubOptions{..} -> [(_sessionID, t, show _retainHandling,
                                                        _retainAsPublished, _noLocal, fromEnum _subQoS)])
           <$> readTVarIO _sessionSubs

data StoredSub = StoredSub {
  _ss_sessID :: SessionID,
  _ss_topic  :: T.Filter,
  _ss_opts   :: T.SubOptions
  }

instance FromRow StoredSub where
  fromRow = do
    _ss_sessID <- field
    _ss_topic <- field
    _retainHandling <- rhFromStr <$> field
    _retainAsPublished <- field
    _noLocal <- field
    _subQoS <- toEnum <$> field
    let _ss_opts = T.SubOptions{..}
    pure StoredSub{..}

      where
        rhFromStr :: String -> T.RetainHandling
        rhFromStr "SendOnSuscribe" = T.SendOnSubscribe
        rhFromStr "SendOnSuscribeNew" = T.SendOnSubscribeNew
        rhFromStr "DoNotSendOnSubscribe" = T.DoNotSendOnSubscribe
        rhFromStr x = error ("Invalid retain handling: " <> show x)

data StoredSession = StoredSession {
  _sts_sessionID  :: SessionID,
  _sts_expiry     :: Int,
  _sts_willTopic  :: Maybe BLTopic,
  _sts_willRetain :: Maybe Bool,
  _sts_willQoS    :: Maybe T.QoS,
  _sts_willBody   :: Maybe BL.ByteString,
  _sts_willProps  :: [T.Property]
  }

instance FromRow StoredSession where
  fromRow = StoredSession
            <$> field
            <*> field
            <*> field
            <*> field
            <*> (fmap toEnum <$> field)
            <*> field
            <*> fmap props field

      where
        props :: Maybe BL.ByteString -> [T.Property]
        props Nothing = []
        props (Just ps) = case A.parse (T.parseProperties T.Protocol50) ps of
                            A.Fail{}     -> []
                            (A.Done _ p) -> p

loadSessions :: (HasDBConnection m, MonadIO m) => m [Session]
loadSessions = liftIO . fetch =<< dbConn
  where fetch db = withTransaction db do
          now <- getCurrentTime
          ssubs <- query_ db "select session_id, topic, retain_handling, retain_as_published, no_local, qos from session_subs"
          let subs = Map.fromListWith (<>) . map (\StoredSub{..} -> (_ss_sessID, [(_ss_topic, _ss_opts)])) $ ssubs
          traverse (mkSessions now subs) =<< query_ db "select session_id, expiry, will_topic, will_retain, will_qos, will_body, will_props from sessions"

        mkSessions now subs StoredSession{..} = do
          let _sessionACL = mempty
              _sessionClient = Nothing
              _sessionWill = will
              _sessionExpires = Just (addUTCTime (fromIntegral _sts_expiry) now)
              _sessionID = _sts_sessionID
          _sessionChan <- newTBQueueIO 1000
          _sessionQP <- newTVarIO mempty
          _sessionSubs <- newTVarIO $ Map.fromList (Map.findWithDefault [] _sessionID subs)
          pure Session{..}

            where will = T.LastWill <$> _sts_willRetain <*> _sts_willQoS
                         <*> _sts_willTopic <*> _sts_willBody <*> pure _sts_willProps

instance ToRow Retained where
  toRow Retained{_retainTS, _retainExp, _retainMsg} = [
    toField (T._pubTopic _retainMsg),
    toField $ fromEnum (T._pubQoS _retainMsg),
    toField (T._pubBody _retainMsg),
    toField props,
    toField _retainTS,
    toField _retainExp
    ]
    where
      props = T.bsProps T.Protocol50 (_retainMsg ^. T.properties)

instance FromRow Retained where
  fromRow = do
    let _pubDup = False
        _pubRetain = True
        _pubPktID = 0
    _pubTopic <- field
    _pubQoS <- toEnum <$> field
    _pubBody <- field
    allProps <- props <$> field
    _retainTS <- field
    _retainExp <- field
    let _pubProps = allProps
        _retainMsg = T.PublishRequest{..}
    pure Retained{..}

      where
        props :: Maybe BL.ByteString -> [T.Property]
        props Nothing = []
        props (Just ps) = case A.parse (T.parseProperties T.Protocol50) ps of
                            A.Fail{}     -> []
                            (A.Done _ p) -> p

storeRetained :: (HasDBConnection m, MonadIO m) => Retained -> m ()
storeRetained = writeDBQueue . StoreRetained

storeRetainedL :: MonadIO m => Connection -> Retained -> m ()
storeRetainedL db p = liftIO $ up (T._pubBody . _retainMsg $ p)
  where up "" = execute db "delete from persisted where topic = ?" (Only (T._pubTopic . _retainMsg $ p))
        up _ = execute db [r|insert into persisted (topic, qos, value, properties, stored, expires) values (?,?,?,?,?,?)
                             on conflict (topic)
                               do update
                                 set qos = excluded.qos,
                                     value = excluded.value,
                                     properties = excluded.properties,
                                     stored = excluded.stored,
                                     expires = excluded.expires|] p

loadRetained :: (HasDBConnection m, MonadIO m) => m [Retained]
loadRetained = liftIO . fetch =<< dbConn
  where
    fetch db = query_ db "select topic, qos, value, properties, stored, expires from persisted"
