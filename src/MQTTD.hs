{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent     (ThreadId, throwTo)
import           Control.Concurrent.STM (STM, TBQueue, TVar, atomically, isFullTBQueue, modifyTVar', newTBQueue,
                                         newTVarIO, readTVar, writeTBQueue, writeTVar)
import           Control.Monad          (unless)
import           Control.Monad.Catch    (Exception, MonadCatch (..), MonadMask (..), MonadThrow (..))
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (MonadLogger (..))
import           Control.Monad.Reader   (MonadReader, ReaderT (..), asks, runReaderT)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Text              (Text, pack)
import qualified Data.Text.Encoding     as TE
import           Data.Word              (Word16)
import qualified Network.MQTT.Topic     as T
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..))

data MQTTException = MQTTDuplicate deriving Show

instance Exception MQTTException

type PktQueue = TBQueue T.MQTTPkt
type ClientID = Int

data ConnectedClient = ConnectedClient {
  _clientConnReq :: T.ConnectRequest,
  _clientThread  :: ThreadId,
  _clientID      :: ClientID
  }

instance Show ConnectedClient where
  show ConnectedClient{..} = "ConnectedClient " <> show _clientConnReq

data Session = Session {
  _sessionClient :: Maybe ConnectedClient,
  _sessionChan   :: PktQueue
  }

data Env = Env {
  subs        :: TVar [(T.Filter, PktQueue)],
  sessions    :: TVar (Map BL.ByteString Session),
  pktID       :: TVar Word16,
  clientIDGen :: TVar ClientID
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
newEnv = liftIO $ Env <$> newTVarIO mempty <*> newTVarIO mempty <*> newTVarIO 1 <*> newTVarIO 0

nextID :: MonadIO m => MQTTD m Int
nextID = asks clientIDGen >>= \ig -> liftSTM $ modifyTVar' ig (+1) >> readTVar ig

findSubs :: MonadIO m => T.Topic -> MQTTD m [PktQueue]
findSubs t = asks subs >>= \st -> liftSTM $ fmap snd . filter (\(f,_) -> T.match f t) <$> readTVar st

subscribe :: MonadIO m => T.SubscribeRequest -> PktQueue -> MQTTD m ()
subscribe (T.SubscribeRequest _ topics _props) ch = do
  let new = map (\(sbs,_) -> (blToText sbs, ch)) topics
  st <- asks subs
  liftSTM $ modifyTVar' st (<> new)

registerClient :: MonadIO m => T.ConnectRequest -> ClientID -> ThreadId -> MQTTD m Session
registerClient req@T.ConnectRequest{..} i o = do
  c <- asks sessions
  let k = _connID
      nc = ConnectedClient req o i
  (o', ns) <- liftSTM $ do
    ch <- newTBQueue 1000
    m <- readTVar c
    let s = maybeClean ch $ Map.findWithDefault (Session Nothing ch) k m
        o' = _sessionClient =<< Map.lookup k m
        ns = s{_sessionClient=Just nc}
    writeTVar c (Map.insert k ns m)
    pure (o', ns)
  case o' of
    Nothing                  -> pure ()
    Just ConnectedClient{..} -> liftIO $ throwTo _clientThread MQTTDuplicate
  pure ns

    where
      maybeClean ch x
        | _cleanSession = Session Nothing ch
        | otherwise = x

unregisterClient :: MonadIO m => BL.ByteString -> ClientID -> MQTTD m ()
unregisterClient k mid = do
  c <- asks sessions
  liftSTM $ modifyTVar' c (Map.update up k)

    where
      up Session{_sessionClient=Just (ConnectedClient{_clientID=i})}
        | mid == i = Nothing
      up s = Just s

unSubAll :: MonadIO m => PktQueue -> MQTTD m ()
unSubAll ch = asks subs >>= \st -> liftSTM $ modifyTVar' st (filter ((/= ch) . snd))

sendPacket :: PktQueue -> T.MQTTPkt -> STM Bool
sendPacket ch p = do
  full <- isFullTBQueue ch
  unless full $ writeTBQueue ch p
  pure full

sendPacketIO :: MonadIO m => PktQueue -> T.MQTTPkt -> m Bool
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

broadcast :: MonadIO m => T.Topic -> BL.ByteString -> Bool -> T.QoS -> MQTTD m ()
broadcast t m r q = do
  subs <- findSubs t
  pid <- liftSTM . nextPktID =<< asks pktID
  mapM_ (flip sendPacketIO (pkt pid)) subs
  where pkt pid = T.PublishPkt T.PublishRequest{
          _pubDup=False,
          _pubRetain=r,
          _pubQoS=q,
          _pubTopic=textToBL t,
          _pubPktID=pid,
          _pubBody=m,
          _pubProps=mempty}
