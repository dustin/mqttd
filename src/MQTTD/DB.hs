{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes       #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TypeApplications  #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module MQTTD.DB where

import           Control.Concurrent.STM          (newTBQueueIO, newTVarIO, readTVarIO)
import           Control.Lens
import           Control.Monad.IO.Class          (MonadIO (..))
import qualified Data.Attoparsec.ByteString.Lazy as A
import qualified Data.ByteString.Lazy            as BL
import qualified Data.Map.Strict                 as Map
import           Data.Maybe                      (fromMaybe)
import           Data.String                     (fromString)
import           Data.Time.Clock                 (addUTCTime, getCurrentTime)
import           Database.SQLite.Simple          hiding (bind, close)
import           Database.SQLite.Simple.ToField
import           Text.RawString.QQ               (r)

import qualified Network.MQTT.Lens               as T
import qualified Network.MQTT.Topic              as T
import qualified Network.MQTT.Types              as T

import           MQTTD.Types

class Monad m => HasDBConnection m where
  dbConn :: m Connection

initQueries :: [(Int, Query)]
initQueries = [
  (1, [r|create table if not exists
         sessions (session_id text primary key, expiry integer)|]),
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

deleteSession :: (HasDBConnection m, MonadIO m) => BL.ByteString -> m ()
deleteSession sid = liftIO . del =<< dbConn
  where del db = execute db "delete from sessions where session_id = ?" (Only sid)

storeSession :: (HasDBConnection m, MonadIO m) => Session -> m ()
storeSession Session{..} = liftIO . up =<< dbConn
  where up db = withTransaction db do
          -- TODO:  For a session with an already-absolute expiry, try to expire at the right time.
          execute db [r|insert into sessions (session_id, expiry) values (?, ?)
                          on conflict (session_id)
                             do update
                                set expiry = excluded.expiry|] (_sessionID, (fst . properFraction $ sto) :: Int)

          execute db "delete from session_subs where session_id = ?" (Only _sessionID)
          executeMany db [r|insert into session_subs
                            (session_id, topic, retain_handling, retain_as_published, no_local, qos)
                            values (?, ?, ?, ?, ?, ?)|] =<< subs

        subs = Map.foldMapWithKey (\t T.SubOptions{..} -> [(_sessionID, t, show _retainHandling,
                                                            _retainAsPublished, _noLocal, fromEnum _subQoS)])
                           <$> readTVarIO _sessionSubs

        sto = fromMaybe defaultSessionExp (_sessionClient ^? _Just . clientConnReq . T.properties . folded . T._PropSessionExpiryInterval . to fromIntegral)

data StoredSub = StoredSub {
  _ss_sessID :: BL.ByteString,
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

loadSessions :: (HasDBConnection m, MonadIO m) => m [Session]
loadSessions = liftIO . fetch =<< dbConn
  where fetch db = withTransaction db do
          now <- getCurrentTime
          ssubs <- query_ db "select session_id, topic, retain_handling, retain_as_published, no_local, qos from session_subs"
          let subs = Map.fromListWith (<>) . map (\StoredSub{..} -> (_ss_sessID, [(_ss_topic, _ss_opts)])) $ ssubs
          traverse (mkSessions now subs) =<< query_ db "select session_id, expiry from sessions"

        mkSessions now subs (_sessionID, expires) = do
          let _sessionACL = mempty
              _sessionClient = Nothing
              _sessionWill = Nothing -- TODO:  Probably want to implement will here
              _sessionExpires = Just (addUTCTime (fromIntegral @Int expires) now)
          _sessionChan <- newTBQueueIO 1000
          _sessionQP <- newTVarIO mempty
          _sessionSubs <- newTVarIO $ Map.fromList (Map.findWithDefault [] _sessionID subs)
          pure Session{..}

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
storeRetained p = liftIO . (up (T._pubBody . _retainMsg $ p)) =<< dbConn
  where up "" db = execute db "delete from persisted where topic = ?" (Only (T._pubTopic . _retainMsg $ p))
        up _ db = execute db [r|insert into persisted (topic, qos, value, properties, stored, expires) values (?,?,?,?,?,?)
                               on conflict (topic)
                                 do update
                                   set qos = excluded.qos,
                                       value = excluded.value,
                                       properties = excluded.properties,
                                       stored = excluded.stored,
                                       expires = excluded.expires|] p

deleteRetained :: (HasDBConnection m, MonadIO m) => BL.ByteString -> m ()
deleteRetained k = liftIO . del =<< dbConn
  where del db = execute db "delete from persisted where topic = ?" (Only k)

loadRetained :: (HasDBConnection m, MonadIO m) => m [Retained]
loadRetained = liftIO . fetch =<< dbConn
  where
    fetch db = query_ db "select topic, qos, value, properties, stored, expires from persisted"
