module Main where

import           Control.Lens
import           Control.Monad            (void)
import           Control.Monad.Catch      (MonadMask (..))
import           Control.Monad.Logger     (LogLevel (..), MonadLogger (..), filterLogger, logInfoN, runStderrLoggingT)
import           Data.Conduit.Network     (runTCPServer, serverSettings)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import           Data.Maybe               (fromMaybe)
import           Database.SQLite.Simple   hiding (bind, close)
import qualified Network.WebSockets       as WS
import           UnliftIO                 (MonadUnliftIO (..), async, waitAnyCancel)

import           MQTTD
import           MQTTD.Conduit
import           MQTTD.Config
import           MQTTD.DB
import           MQTTD.Types
import           MQTTD.Util

runListener :: (MonadUnliftIO m, MonadLogger m, MonadFail m, MonadMask m) => Listener -> MQTTD m ()
runListener (MQTTListener a p _) = do
  logInfoN ("Starting mqtt service on " <> tshow a <> ":" <> tshow p)
  withRunInIO $ \unl -> runTCPServer (serverSettings p a) (unl . tcpApp)
runListener (WSListener a p _) = do
  logInfoN ("Starting websocket service on " <> tshow a <> ":" <> tshow p)
  withRunInIO $ \unl -> WS.runServer a p (unl . webSocketsApp)
runListener (MQTTSListener a p c k _) = do
  logInfoN ("Starting mqtts service on " <> tshow a <> ":" <> tshow p)
  withRunInIO $ \unl -> runGeneralTCPServerTLS (tlsConfig a p c k) (unl . tcpApp)

main :: IO ()
main = do
  conf@Config{..} <- parseConfFile "mqttd.conf"

  let baseAuth = _confDefaults `applyListenerOptions` Authorizer{
        _authAnon = False,
        _authUsers = _confUsers
        }

  withConnection "mqttd.db" $ \db -> do
    initDB db
    e <- newEnv baseAuth db
    runStderrLoggingT . logfilt conf . runIO e $ do
      sc <- async sessionCleanup
      pc <- async retainerCleanup
      restoreSessions
      restoreRetained

      ls <- traverse (async . runModified) _confListeners

      void $ waitAnyCancel (sc:pc:ls)

        where
          logfilt Config{..} = filterLogger (\_ -> flip (if _confDebug then (>=) else (>)) LevelDebug)

          runModified = modifyAuthorizer . applyListenerOptions . view listenerOpts <*> runListener

          applyListenerOptions ListenerOptions{..} a@Authorizer{..} =
            a{_authAnon=fromMaybe _authAnon _optAllowAnonymous}
