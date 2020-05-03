{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Main where

import           Control.Concurrent.STM   (TChan, newTChanIO, readTChan)
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

dispatch :: (MonadFail m, MonadIO m) => TChan T.MQTTPkt -> T.MQTTPkt -> MQTTD m ()
dispatch ch T.PingPkt = sendPacketIO ch T.PongPkt
dispatch ch (T.SubscribePkt req@(T.SubscribeRequest pid subs props)) = do
  subscribe req ch
  sendPacketIO ch (T.SubACKPkt (T.SubscribeResponse pid (map (const (Right T.QoS0)) subs) props))
dispatch _ (T.PublishPkt T.PublishRequest{..}) =
  broadcast (blToText _pubTopic) _pubBody _pubRetain _pubQoS
dispatch _ x = fail ("unhandled: " <> show x)

handleConnection :: forall m. (MonadLogger m, MonadUnliftIO m, MonadFail m, E.MonadMask m, E.MonadThrow m) => AppData -> MQTTD m ()
handleConnection ad = runConduit $ do
  ch :: TChan T.MQTTPkt <- liftIO newTChanIO
  (cpkt@(T.ConnPkt _ pl), genedID) <- ensureID =<< appSource ad .| sinkParser T.parseConnect
  o <- lift . async $ processOut pl ch

  lift $ E.finally (runIn pl ch cpkt genedID o) (teardown o ch cpkt)

  where
    runIn :: T.ProtocolLevel -> TChan T.MQTTPkt -> T.MQTTPkt -> Maybe BL.ByteString -> Async () -> MQTTD m ()
    runIn pl ch (T.ConnPkt req@T.ConnectRequest{..} _) nid o = do
      logInfoN ("A connection is made " <> tshow req)
      link o
      -- Register and accept the connection
      _ <- registerClient req ch o
      let cprops = [ T.PropAssignedClientIdentifier i | Just i <- [nid] ]
      sendPacketIO ch (T.ConnACKPkt $ T.ConnACKFlags False T.ConnAccepted cprops)

      runConduit $ appSource ad
        .| conduitParser (T.parsePacket pl)
        .| C.mapM_ (\(_,x) -> dispatch ch x)

    processOut pl ch = runConduit $
      C.repeatM (liftSTM $ readTChan ch)
      .| C.map (BL.toStrict . T.toByteString pl)
      .| appSink ad

    teardown :: Async a -> TChan T.MQTTPkt -> T.MQTTPkt -> MQTTD m ()
    teardown o ch (T.ConnPkt c@T.ConnectRequest{..} _) = do
      cancel o
      logDebugN ("Tearing down ... " <> tshow c)
      unregisterClient _connID ch
      unSubAll ch
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
