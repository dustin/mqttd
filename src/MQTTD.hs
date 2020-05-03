{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE UndecidableInstances       #-}

module MQTTD where

import           Control.Concurrent.STM (TChan, atomically, writeTChan)
import           Control.Concurrent.STM (STM, TChan, TVar, atomically, modifyTVar', newTChanIO, newTVarIO, readTChan,
                                         readTVar, writeTChan)
import           Control.Monad.Catch    (MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException (..), bracket_,
                                         catch)
import           Control.Monad.IO.Class (MonadIO (..))
import           Control.Monad.Logger   (Loc (..), LogLevel (..), LogSource, LogStr, MonadLogger (..), ToLogStr (..),
                                         logDebugN, logErrorN, logInfoN, monadLoggerLog)
import           Control.Monad.Reader   (MonadReader, ReaderT (..), asks, runReaderT)
import qualified Data.ByteString.Lazy   as BL
import           Data.Map.Strict        (Map)
import qualified Data.Map.Strict        as Map
import           Data.Text              (Text)
import qualified Data.Text.Encoding     as TE
import qualified Network.MQTT.Types     as T
import           UnliftIO               (MonadUnliftIO (..))

data Env = Env {
  subs :: TVar (Map Text [TChan T.MQTTPkt])
  }

newtype MQTTD a = MQTTD
  { runMQTTD :: ReaderT Env IO a
  } deriving (Applicative, Functor, Monad, MonadIO, MonadUnliftIO,
              MonadCatch, MonadThrow, MonadMask, MonadReader Env, MonadFail)

liftSTM :: MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

runIO :: MonadIO m => Env -> MQTTD a -> m a
runIO e m = liftIO $ runReaderT (runMQTTD m) e

newEnv :: MonadIO m => m Env
newEnv = liftIO $ Env <$> newTVarIO mempty

runNew :: MonadIO m => MQTTD a -> m a
runNew a = newEnv >>= \e -> runIO e a

findSubs :: Text -> MQTTD [TChan T.MQTTPkt]
findSubs t = asks subs >>= \st -> liftSTM $ Map.findWithDefault [] t <$> readTVar st

subscribe :: T.SubscribeRequest -> TChan T.MQTTPkt -> MQTTD ()
subscribe (T.SubscribeRequest _ topics _props) ch = do
  let m = Map.fromList $ map (\(sbs,_) -> (blToText sbs, [ch])) topics
  st <- asks subs
  liftSTM $ modifyTVar' st (Map.unionWith (<>) m)

unSubAll :: TChan T.MQTTPkt -> MQTTD ()
unSubAll ch = asks subs >>= \st -> liftSTM $ modifyTVar' st (Map.map (filter (/= ch)))

sendPacket :: TChan T.MQTTPkt -> T.MQTTPkt -> STM ()
sendPacket ch p = writeTChan ch p

sendPacketIO :: MonadIO m => TChan T.MQTTPkt -> T.MQTTPkt -> m ()
sendPacketIO ch = liftSTM . sendPacket ch

textToBL :: Text -> BL.ByteString
textToBL = BL.fromStrict . TE.encodeUtf8

blToText :: BL.ByteString -> Text
blToText = TE.decodeUtf8 . BL.toStrict

broadcast :: Text -> BL.ByteString -> Bool -> T.QoS -> MQTTD ()
broadcast t m r q = do
  subs <- findSubs t
  mapM_ (\ch' -> sendPacketIO ch' pkt) subs
  where pkt = T.PublishPkt T.PublishRequest{
          _pubDup=False,
          _pubRetain=r,
          _pubQoS=q,
          _pubTopic=textToBL t,
          _pubPktID=13,
          _pubBody=m,
          _pubProps=mempty}
