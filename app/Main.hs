module Main where

import           Control.Monad            (void)
import           Control.Monad.Catch      (MonadMask (..))
import           Control.Monad.Logger     (LogLevel (..), MonadLogger (..), filterLogger, logInfoN, runStderrLoggingT)
import           Data.Conduit.Network     (runTCPServer, serverSettings)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import           Data.Maybe               (fromMaybe)
import qualified Network.WebSockets       as WS
import           UnliftIO                 (MonadUnliftIO (..), async, waitAnyCancel)

import           MQTTD
import           MQTTD.Conduit
import           MQTTD.Config
import           MQTTD.Util

applyListenerOptions :: ListenerOptions -> Authorizer -> Authorizer
applyListenerOptions ListenerOptions{..} a@Authorizer{..} =
  a{_authAnon=fromMaybe _authAnon _optAllowAnonymous}

runListener :: (MonadUnliftIO m, MonadLogger m, MonadFail m, MonadMask m) => Listener -> MQTTD m ()
runListener (MQTTListener a p o) = do
  logInfoN ("Starting mqtt service on " <> tshow a <> ":" <> tshow p)
  modifyAuthorizer (applyListenerOptions o) $
    withRunInIO $ \unl -> runTCPServer (serverSettings p a) (unl . tcpApp)
runListener (WSListener a p o) = do
  logInfoN ("Starting websocket service on " <> tshow a <> ":" <> tshow p)
  modifyAuthorizer (applyListenerOptions o) $
    withRunInIO $ \unl -> WS.runServer a p (unl . webSocketsApp)
runListener (MQTTSListener a p c k o) = do
  logInfoN ("Starting mqtts service on " <> tshow a <> ":" <> tshow p)
  modifyAuthorizer (applyListenerOptions o) $
    withRunInIO $ \unl -> runGeneralTCPServerTLS (tlsConfig a p c k) (unl . tcpApp)

main :: IO ()
main = do
  conf@Config{..} <- parseConfFile "mqttd.conf"

  let baseAuth = _confDefaults `applyListenerOptions` Authorizer{
        _authAnon = False,
        _authUsers = _confUsers
        }

  e <- newEnv baseAuth
  runStderrLoggingT . logfilt conf . runIO e $ do
    sc <- async sessionCleanup
    pc <- async persistenceCleanup

    ls <- traverse (async . runListener) _confListeners

    void $ waitAnyCancel (sc:pc:ls)

      where
        logfilt Config{..} = filterLogger (\_ -> flip (if _confDebug then (>=) else (>)) LevelDebug)
