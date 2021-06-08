module MQTTD.Main where

import           Control.Concurrent       (newChan, newEmptyMVar, putMVar, readChan, takeMVar)
import           Control.Lens
import           Control.Monad.Catch      (MonadMask (..))
import           Control.Monad.IO.Class   (MonadIO (..))
import           Control.Monad.Logger     (LogLevel (..), MonadLogger (..), filterLogger, logDebugN, logInfoN,
                                           runStderrLoggingT)
import           Data.Conduit.Network     (runGeneralTCPServer, serverSettings, setAfterBind)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import           Data.Maybe               (fromMaybe)
import           Database.SQLite.Simple   hiding (bind)
import qualified Network.WebSockets       as WS
import           UnliftIO                 (Async (..), MonadUnliftIO (..), async, finally, withRunInIO)

import           MQTTD
import           MQTTD.Conduit
import           MQTTD.Config
import           MQTTD.DB
import           MQTTD.Stats              (applyStats)
import           MQTTD.Types
import           MQTTD.Util

runListener :: (MonadUnliftIO m, MonadLogger m, MonadFail m, MonadMask m) => Listener -> MQTTD m (Async ())
runListener (MQTTListener a p _) = do
  logInfoN ("Starting mqtt service on " <> tshow a <> ":" <> tshow p)
  -- The generic TCP listener is featureful enough to allow us to
  -- block until binding is done.
  bound <- liftIO newEmptyMVar
  rv <- async $ runGeneralTCPServer (serverSettings p a & setAfterBind (putMVar bound)) tcpApp
  _ <- liftIO $ takeMVar bound
  pure rv
runListener (WSListener a p _) = do
  logInfoN ("Starting websocket service on " <> tshow a <> ":" <> tshow p)
  withRunInIO $ \unl -> async $ WS.runServer a p (unl . webSocketsApp)
runListener (MQTTSListener a p c k _) = do
  logInfoN ("Starting mqtts service on " <> tshow a <> ":" <> tshow p)
  async $ runGeneralTCPServerTLS (tlsConfig a p c k) tcpApp

-- Block forever.
pause :: MonadIO m => m ()
pause = liftIO (newChan >>= readChan)

runServerLogging :: (MonadFail m, MonadMask m, MonadUnliftIO m, MonadIO m, MonadLogger m) => Config -> m [Async ()]
runServerLogging Config{..} = do
  let baseAuth = _confDefaults `applyListenerOptions` Authorizer{
        _authAnon = False,
        _authUsers = _confUsers
        }

  db <- liftIO $ open (_persistenceDBPath _confPersist)
  liftIO $ initDB db
  dbc <- async $ finally pause (logDebugN "Closing DB connection" >> liftIO (close db))

  withRunInIO $ \unl -> do
    e <- newEnv baseAuth db
    unl . runIO e $ do
      sc <- async sessionCleanup
      pc <- async retainerCleanup
      dba <- async runOperations
      st <- async publishStats
      as <- async (applyStats $ stats e)
      restoreSessions
      restoreRetained

      ls <- traverse runModified _confListeners

      pure (sc:pc:dba:st:as:dbc:ls)

        where
          runModified = modifyAuthorizer . applyListenerOptions . view listenerOpts <*> runListener

          applyListenerOptions ListenerOptions{..} a@Authorizer{..} =
            a{_authAnon=fromMaybe _authAnon _optAllowAnonymous}

runServer :: Config -> IO [Async ()]
runServer conf = runStderrLoggingT . logfilt conf . runServerLogging $ conf
  where
    logfilt Config{..} = filterLogger (\_ -> flip (if _confDebug then (>=) else (>)) LevelDebug)
