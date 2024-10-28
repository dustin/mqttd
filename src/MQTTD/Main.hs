module MQTTD.Main where

import           Cleff
import           Cleff.Fail
import           Control.Concurrent       (newChan, newEmptyMVar, putMVar, readChan, takeMVar)
import           Control.Lens
import           Data.Conduit.Network     (runGeneralTCPServer, serverSettings, setAfterBind)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import           Data.Maybe               (fromMaybe)
import           Database.SQLite.Simple   hiding (bind)
import qualified Network.WebSockets       as WS
import           UnliftIO                 (Async (..), async, finally)

import           MQTTD
import           MQTTD.Conduit
import           MQTTD.Config
import           MQTTD.DB
import           MQTTD.Logging
import           MQTTD.Stats
import           MQTTD.Types
import           MQTTD.Util

runListener :: [IOE, Fail, LogFX, MQTTD, Stats, DB] :>> es => Listener -> Eff es (Async ())
runListener (MQTTListener a p _) = do
  logInfoL ["Starting mqtt service on ", tshow a, ":", tshow p]
  -- The generic TCP listener is featureful enough to allow us to
  -- block until binding is done.
  bound <- liftIO newEmptyMVar
  rv <- async $ runGeneralTCPServer (serverSettings p a & setAfterBind (putMVar bound)) tcpApp
  _ <- liftIO $ takeMVar bound
  pure rv
runListener (WSListener a p _) = do
  logInfoL ["Starting websocket service on ", tshow a, ":", tshow p]
  withRunInIO $ \unl -> async $ WS.runServer a p (unl . webSocketsApp)
runListener (MQTTSListener a p c k _) = do
  logInfoL ["Starting mqtts service on ", tshow a, ":", tshow p]
  async $ runGeneralTCPServerTLS (tlsConfig a p c k) tcpApp

-- Block forever.
pause :: MonadIO m => m ()
pause = liftIO (newChan >>= readChan)

runServerLogging :: [IOE, Fail, LogFX, Stats] :>> es => Config -> Eff es [Async ()]
runServerLogging Config{..} = do
  let baseAuth = _confDefaults `applyListenerOptions` Authorizer{
        _authAnon = False,
        _authUsers = _confUsers
        }

  -- withConnection is not used here because this action spawns a
  -- bunch of Asyncs and returns a list of them.  withConnection would
  -- close the connection before it returns.  Instead, I hold the
  -- connection inside its own Async and add that to the list.
  db <- liftIO $ open (_persistenceDBPath _confPersist)
  liftIO $ initDB db
  dbc <- async $ finally pause (logDbg "Closing DB connection" >> liftIO (close db))

  e <- newEnv baseAuth db
  ss <- getStatStore
  fmap fst . runMQTTD e . runDB db (dbQ e) $ do
    sc <- async sessionCleanup
    pc <- async retainerCleanup
    dba <- async runOperations
    st <- async publishStats
    as <- async (applyStats ss)
    restoreSessions
    restoreRetained

    ls <- traverse runModified _confListeners

    pure (sc:pc:dba:st:as:dbc:ls)

        where
          runModified = modifyAuthorizer . applyListenerOptions . view listenerOpts <*> runListener

          applyListenerOptions ListenerOptions{..} a@Authorizer{..} =
            a{_authAnon=fromMaybe _authAnon _optAllowAnonymous}

runServer :: Config -> IO [Async ()]
runServer conf = runIOE . runFailIO . runLogFX (verbose conf) . runNewStats . runServerLogging $ conf
  where
    verbose Config{..} = _confDebug
