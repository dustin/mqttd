module Main where

import           Control.Monad            (void)
import           Control.Monad.Catch      (MonadMask (..))
import           Control.Monad.Logger     (MonadLogger (..), runStderrLoggingT)
import           Data.Conduit.Network     (runTCPServer, serverSettings)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import qualified Network.WebSockets       as WS
import           UnliftIO                 (MonadUnliftIO (..), async, waitAnyCancel)

import           MQTTD
import           MQTTD.Conduit
import           MQTTD.Config

runListener :: (MonadUnliftIO m, MonadLogger m, MonadFail m, MonadMask m) => Listener -> MQTTD m ()
runListener (MQTTListener a p) = withRunInIO $ \unl -> runTCPServer (serverSettings p a) (unl . tcpApp)
runListener (WSListener a p) = withRunInIO $ \unl -> WS.runServer a p (unl . webSocketsApp)
runListener (MQTTSListener a p c k) = withRunInIO $ \unl -> runGeneralTCPServerTLS (tlsConfig a p c k) (unl . tcpApp)

main :: IO ()
main = do
  Config{..} <- parseConfFile "mqttd.conf"

  e <- newEnv
  runStderrLoggingT . runIO e $ do
    sc <- async sessionCleanup
    pc <- async persistenceCleanup

    ls <- traverse (async . runListener) _confListeners

    void $ waitAnyCancel (sc:pc:ls)
