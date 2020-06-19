module MQTTD.Main where

import           Control.Lens
import           Control.Monad.Catch      (MonadMask (..))
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.Logger     (LogLevel (..), MonadLogger (..), filterLogger, logInfoN, runStderrLoggingT)
import           Data.Conduit.Network     (runGeneralTCPServer, serverSettings)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import           Data.Maybe               (fromMaybe)
import           Database.SQLite.Simple   hiding (bind, close)
import qualified Network.WebSockets       as WS
import           UnliftIO                 (Async (..), MonadUnliftIO (..), async, withRunInIO)

import           MQTTD
import           MQTTD.Conduit
import           MQTTD.Config
import           MQTTD.DB
import           MQTTD.Types
import           MQTTD.Util

runListener :: (MonadUnliftIO m, MonadLogger m, MonadFail m, MonadMask m) => Listener -> MQTTD m ()
runListener (MQTTListener a p _) = do
  logInfoN ("Starting mqtt service on " <> tshow a <> ":" <> tshow p)
  runGeneralTCPServer (serverSettings p a) tcpApp
runListener (WSListener a p _) = do
  logInfoN ("Starting websocket service on " <> tshow a <> ":" <> tshow p)
  withRunInIO $ \unl -> WS.runServer a p (unl . webSocketsApp)
runListener (MQTTSListener a p c k _) = do
  logInfoN ("Starting mqtts service on " <> tshow a <> ":" <> tshow p)
  runGeneralTCPServerTLS (tlsConfig a p c k) tcpApp


runServerLogging :: (MonadFail m, MonadMask m, MonadUnliftIO m, MonadIO m, MonadLogger m) => Config -> m [Async ()]
runServerLogging Config{..} = do
  let baseAuth = _confDefaults `applyListenerOptions` Authorizer{
        _authAnon = False,
        _authUsers = _confUsers
        }

  withRunInIO $ \unl -> withConnection (_persistenceDBPath _confPersist) $ \db -> do
    initDB db
    e <- newEnv baseAuth db
    unl . runIO e $ do
      sc <- async sessionCleanup
      pc <- async retainerCleanup
      dba <- async (runOperations $ statStore e)
      st <- async publishStats
      as <- async applyStats
      restoreSessions
      restoreRetained

      ls <- traverse (async . runModified) _confListeners

      pure (sc:pc:dba:st:as:ls)

        where
          runModified = modifyAuthorizer . applyListenerOptions . view listenerOpts <*> runListener

          applyListenerOptions ListenerOptions{..} a@Authorizer{..} =
            a{_authAnon=fromMaybe _authAnon _optAllowAnonymous}

runServer :: Config -> IO [Async ()]
runServer conf@Config{..} = runStderrLoggingT . logfilt conf . runServerLogging $ conf
  where
    logfilt Config{..} = filterLogger (\_ -> flip (if _confDebug then (>=) else (>)) LevelDebug)
