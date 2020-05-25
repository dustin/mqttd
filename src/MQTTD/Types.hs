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

module MQTTD.Types where

import           Control.Concurrent     (ThreadId)
import           Control.Concurrent.STM (TBQueue, TVar)
import           Control.Lens
import           Control.Monad.Catch    (Exception)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import           Data.Time.Clock        (NominalDiffTime, UTCTime (..))
import           Data.Word              (Word16)
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T

import           MQTTD.Config           (ACL (..), User (..))

data MQTTException = MQTTPingTimeout | MQTTDuplicate deriving Show

instance Exception MQTTException

type PktQueue = TBQueue T.MQTTPkt
type ClientID = Int

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
  _sessionID      :: BL.ByteString,
  _sessionACL     :: [ACL],
  _sessionClient  :: Maybe ConnectedClient,
  _sessionChan    :: PktQueue,
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
  _retainExp :: Maybe UTCTime,
  _retainMsg :: T.PublishRequest
  } deriving Show

makeLenses ''Retained
