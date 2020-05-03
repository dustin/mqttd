{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent.STM (TChan, atomically, writeTChan)
import           Control.Concurrent.STM (STM, TChan, TVar, atomically, modifyTVar', newTChanIO, newTVarIO, readTChan,
                                         readTVar, writeTChan, writeTVar)
import           Control.Monad.Catch    (Exception, MonadCatch (..), MonadMask (..), MonadThrow (..),
                                         SomeException (..), bracket_, catch)
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (Loc (..), LogLevel (..), LogSource, LogStr, MonadLogger (..), ToLogStr (..),
                                         logDebugN, logErrorN, logInfoN, monadLoggerLog)
import           Control.Monad.Reader   (MonadReader, ReaderT (..), asks, runReaderT)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Text              (Text, pack)
import qualified Data.Text.Encoding     as TE
import           Data.Word              (Word16)
import qualified Network.MQTT.Types     as T
import           UnliftIO               (Async (..), MonadUnliftIO (..), cancelWith)

data MQTTException = MQTTDuplicate deriving Show

instance Exception MQTTException

data ConnectedClient = ConnectedClient {
  _clientConnReq :: T.ConnectRequest,
  _clientChan    :: TChan T.MQTTPkt,
  _clientThread  :: Async ()
  }

instance Show ConnectedClient where
  show ConnectedClient{..} = "ConnectedClient " <> show _clientConnReq

data Session = Session {
  _sessionClient :: Maybe ConnectedClient
  }

data Env = Env {
  subs     :: TVar (Map Text [TChan T.MQTTPkt]),
  sessions :: TVar (Map BL.ByteString Session),
  pktID    :: TVar Word16
  }

newtype MQTTD m a = MQTTD
  { runMQTTD :: ReaderT Env m a
  } deriving (Applicative, Functor, Monad, MonadIO, MonadLogger,
              MonadCatch, MonadThrow, MonadMask, MonadReader Env, MonadFail)

instance MonadUnliftIO m => MonadUnliftIO (MQTTD m) where
  withRunInIO inner = MQTTD $ withRunInIO $ \run -> inner (run . runMQTTD)

liftSTM :: MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

runIO :: (MonadIO m, MonadLogger m) => Env -> MQTTD m a -> m a
runIO e m = runReaderT (runMQTTD m) e

newEnv :: MonadIO m => m Env
newEnv = liftIO $ Env <$> newTVarIO mempty <*> newTVarIO mempty <*> newTVarIO 1

findSubs :: MonadIO m => Text -> MQTTD m [TChan T.MQTTPkt]
findSubs t = asks subs >>= \st -> liftSTM $ Map.findWithDefault [] t <$> readTVar st

subscribe :: MonadIO m => T.SubscribeRequest -> TChan T.MQTTPkt -> MQTTD m ()
subscribe (T.SubscribeRequest _ topics _props) ch = do
  let m = Map.fromList $ map (\(sbs,_) -> (blToText sbs, [ch])) topics
  st <- asks subs
  liftSTM $ modifyTVar' st (Map.unionWith (<>) m)

registerClient :: MonadIO m => T.ConnectRequest -> TChan T.MQTTPkt -> Async () -> MQTTD m Session
registerClient req@T.ConnectRequest{..} ch o = do
  c <- asks sessions
  let k = _connID
      nc = ConnectedClient req ch o
  (o', ns) <- liftSTM $ do
    m <- readTVar c
    let s = maybeClean $ Map.findWithDefault (Session Nothing) k m
        o' = _sessionClient s
        ns = s{_sessionClient=Just nc}
    writeTVar c (Map.insert k ns m)
    pure (o', ns)
  case o' of
    Nothing                      -> pure ()
    Just a@(ConnectedClient{..}) -> cancelWith _clientThread MQTTDuplicate
  pure ns

    where
      maybeClean x
        | _cleanSession = Session Nothing
        | otherwise = x

unregisterClient :: MonadIO m => BL.ByteString -> TChan T.MQTTPkt -> MQTTD m ()
unregisterClient k ch = do
  c <- asks sessions
  liftSTM $ modifyTVar' c (Map.update up k)

    where
      up Session{_sessionClient=Just (ConnectedClient{_clientChan=ch})} = Nothing
      up s = Just s

unSubAll :: MonadIO m => TChan T.MQTTPkt -> MQTTD m ()
unSubAll ch = asks subs >>= \st -> liftSTM $ modifyTVar' st (Map.map (filter (/= ch)))

sendPacket :: TChan T.MQTTPkt -> T.MQTTPkt -> STM ()
sendPacket ch p = writeTChan ch p

sendPacketIO :: MonadIO m => TChan T.MQTTPkt -> T.MQTTPkt -> m ()
sendPacketIO ch = liftSTM . sendPacket ch

textToBL :: Text -> BL.ByteString
textToBL = BL.fromStrict . TE.encodeUtf8

blToText :: BL.ByteString -> Text
blToText = TE.decodeUtf8 . BL.toStrict

tshow :: Show a => a -> Text
tshow = pack . show

nextPktID :: TVar Word16 -> STM Word16
nextPktID x = do
  modifyTVar' x $ \pid -> if pid == maxBound then 1 else succ pid
  readTVar x

broadcast :: MonadIO m => Text -> BL.ByteString -> Bool -> T.QoS -> MQTTD m ()
broadcast t m r q = do
  subs <- findSubs t
  pid <- liftSTM . nextPktID =<< asks pktID
  mapM_ (\ch' -> sendPacketIO ch' (pkt pid)) subs
  where pkt pid = T.PublishPkt T.PublishRequest{
          _pubDup=False,
          _pubRetain=r,
          _pubQoS=q,
          _pubTopic=textToBL t,
          _pubPktID=pid,
          _pubBody=m,
          _pubProps=mempty}
