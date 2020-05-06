{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent       (myThreadId)
import           Control.Concurrent.STM   (readTBQueue)
import           Control.Monad            (void)
import qualified Control.Monad.Catch      as E
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.Logger     (MonadLogger (..), logDebugN, logInfoN, runStderrLoggingT)
import           Control.Monad.Trans      (lift)
import qualified Data.ByteString.Lazy     as BL
import           Data.Conduit             (runConduit, (.|))
import           Data.Conduit.Attoparsec  (conduitParser, sinkParser)
import qualified Data.Conduit.Combinators as C
import           Data.Conduit.Network     (AppData, appSink, appSource, runTCPServer, serverSettings)
import qualified Data.UUID                as UUID
import           System.Random            (randomIO)
import           UnliftIO                 (Async (..), MonadUnliftIO (..), async, cancel, link)

import qualified Network.MQTT.Types       as T

import           MQTTD

dispatch :: (MonadLogger m, MonadFail m, MonadIO m) => Session -> T.MQTTPkt -> MQTTD m ()
dispatch Session{..} T.PingPkt = void $ sendPacketIO _sessionChan T.PongPkt
dispatch sess@Session{..} (T.SubscribePkt req@(T.SubscribeRequest pid subs props)) = do
  subscribe sess req
  void $ sendPacketIO _sessionChan (T.SubACKPkt (T.SubscribeResponse pid (map (const (Right T.QoS0)) subs) props))
dispatch sess (T.PublishPkt req) = do
  T.PublishRequest{..} <- resolveAliasIn sess req
  broadcast (blToText _pubTopic) _pubBody _pubRetain _pubQoS
dispatch _ x = fail ("unhandled: " <> show x)

handleConnection :: forall m. (MonadLogger m, MonadUnliftIO m, MonadFail m, E.MonadMask m, E.MonadThrow m) => AppData -> MQTTD m ()
handleConnection ad = runConduit $ do
  (cpkt@(T.ConnPkt _ pl), genedID) <- ensureID =<< appSource ad .| sinkParser T.parseConnect
  cid <- lift nextID

  lift $ run pl cid cpkt genedID

  where
    run :: T.ProtocolLevel -> ClientID -> T.MQTTPkt -> Maybe BL.ByteString -> MQTTD m ()
    run pl cid (T.ConnPkt req@T.ConnectRequest{..} _) nid = do
      logInfoN ("A connection is made " <> tshow req)
      -- Register and accept the connection
      tid <- liftIO myThreadId
      (sess@Session{_sessionChan}, existing) <- registerClient req cid tid
      let cprops = [ T.PropTopicAliasMaximum 100 ] <> [ T.PropAssignedClientIdentifier i | Just i <- [nid] ]
      sendPacketIO _sessionChan (T.ConnACKPkt $ T.ConnACKFlags existing T.ConnAccepted cprops)

      o <- async $ processOut pl _sessionChan
      link o

      E.finally (runIn sess pl) (teardown o cid req)

    runIn sess pl = runConduit $ appSource ad
        .| conduitParser (T.parsePacket pl)
        .| C.mapM_ (\(_,x) -> dispatch sess x)

    processOut pl ch = runConduit $
      C.repeatM (liftSTM $ readTBQueue ch)
      .| C.mapM (\x -> liftIO (print x) >> pure x)
      .| C.map (BL.toStrict . T.toByteString pl)
      .| appSink ad

    teardown :: Async a -> ClientID -> T.ConnectRequest -> MQTTD m ()
    teardown o cid c@T.ConnectRequest{..} = do
      cancel o
      logDebugN ("Tearing down ... " <> tshow c)
      unregisterClient _connID cid
      case _lastWill of
        Nothing               -> pure ()
        Just (T.LastWill{..}) -> broadcast (blToText _willTopic) _willMsg _willRetain _willQoS

    ensureID (T.ConnPkt c@T.ConnectRequest{_connID=""} pl) = do
      nid <- BL.fromStrict . UUID.toASCIIBytes <$> liftIO randomIO
      logDebugN ("Generating ID for anonymous client: " <> tshow nid)
      pure (T.ConnPkt c{T._connID=nid} pl, Just nid)
    ensureID x = pure (x, Nothing)

main :: IO ()
main = do
  e <- newEnv
  runTCPServer (serverSettings 1883 "*") (runStderrLoggingT . runIO e . handleConnection)
