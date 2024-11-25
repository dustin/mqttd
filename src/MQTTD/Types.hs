{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE StrictData                 #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeApplications           #-}
{-# LANGUAGE UndecidableInstances       #-}
{-# LANGUAGE ViewPatterns               #-}

module MQTTD.Types where

import           Control.Concurrent     (ThreadId)
import           Control.Concurrent.STM (TBQueue, TVar)
import           Control.Lens
import           Control.Monad.Catch    (Exception, MonadMask (..))
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..))
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Text              as Txt
import           Data.Time.Clock        (NominalDiffTime, UTCTime (..), addUTCTime, diffUTCTime)
import           Data.Word              (Word16)
import           Network.MQTT.Lens
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..))

import           MQTTD.Config           (ACL (..), User (..))
import           Scheduler              (QueueID (..))

data MQTTException = MQTTPingTimeout | MQTTDuplicate SessionID
  deriving Show

instance Exception MQTTException

type PktQueue = TBQueue T.MQTTPkt
type ClientID = Int
type SessionID = BL.ByteString
type BLTopic = BL.ByteString
type BLFilter = BL.ByteString
type SubscriberName = Txt.Text

defaultQueueSize :: Num a => a
defaultQueueSize = 1000

data ConnectedClient = ConnectedClient {
  _clientConnReq  :: T.ConnectRequest,
  _clientThread   :: ThreadId,
  _clientID       :: ClientID,
  _clientAliasIn  :: TVar (Map Word16 BL.ByteString),
  _clientAliasOut :: TVar (Map BL.ByteString Word16),
  _clientALeft    :: TVar Word16
  }

makeLenses ''ConnectedClient

instance Show ConnectedClient where
  show ConnectedClient{..} = "ConnectedClient " <> show _clientConnReq

data Session = Session {
  _sessionID      :: SessionID,
  _sessionACL     :: [ACL],
  _sessionClient  :: Maybe ConnectedClient,
  _sessionChan    :: PktQueue,
  _sessionFlight  :: TVar Word16,
  _sessionBacklog :: TBQueue (Maybe UTCTime, T.PublishRequest),
  _sessionQP      :: TVar (Map T.PktID T.PublishRequest),
  _sessionSubs    :: TVar (Map T.Filter T.SubOptions),
  _sessionExpires :: Maybe UTCTime,
  _sessionWill    :: Maybe T.LastWill
  }

makeLenses ''Session

defaultSessionExp :: NominalDiffTime
defaultSessionExp = 300

data Authorizer = Authorizer {
  _authUsers :: Map BL.ByteString User,
  _authAnon  :: Bool
  } deriving Show

makeLenses ''Authorizer

data Retained = Retained {
  _retainTS  :: UTCTime,
  _retainExp :: Maybe QueueID,
  _retainMsg :: T.PublishRequest
  } deriving Show

makeLenses ''Retained

type PublishConstraint m = (MonadLogger m, MonadFail m, MonadMask m, MonadUnliftIO m, MonadIO m)

data TopicType
  = Normal T.Filter
  | SharedSubscription SubscriberName T.Filter
  | InvalidTopic
  deriving (Eq, Show)

classifyTopic :: T.Filter -> TopicType
classifyTopic f@(T.unFilter -> txt)
  | "$share/" `Txt.isPrefixOf` txt = case Txt.break (== '/') (Txt.drop 7 txt) of
                                       (_, "") -> InvalidTopic
                                       (n, f')  -> case (T.mkFilter . Txt.drop 1) f' of
                                                    Just f'' -> SharedSubscription n f''
                                                    _        -> InvalidTopic
  | otherwise = Normal f

partitionShared :: [(T.Filter, o)] -> ([(SubscriberName, T.Filter, o)], [(T.Filter, o)])
partitionShared = foldr (\x@(t,o) (s,n) -> case classifyTopic t of
                                             Normal _                 -> (s, x:n)
                                             SharedSubscription sn sf -> ((sn, sf, o):s, n)
                                             _                        -> (s, n)) ([], [])

deadline :: UTCTime -> T.PublishRequest -> Maybe UTCTime
deadline now req = req ^? properties . folded . _PropMessageExpiryInterval . to (absExp now)

absExp :: Integral a => UTCTime -> a -> UTCTime
absExp now secs = addUTCTime (fromIntegral secs) now

relExp :: Integral p => UTCTime -> UTCTime -> p
relExp now e = fst . properFraction $ diffUTCTime e now
