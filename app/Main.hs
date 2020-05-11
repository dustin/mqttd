module Main where

import           Control.Monad            (void)
import           Control.Monad.Logger     (runStderrLoggingT)
import           Data.Conduit.Network     (runTCPServer, serverSettings)
import           Data.Conduit.Network.TLS (runGeneralTCPServerTLS, tlsConfig)
import qualified Network.WebSockets       as WS
import           UnliftIO                 (MonadUnliftIO (..), async, waitAnyCancel)

import           MQTTD
import           MQTTD.Conduit

main :: IO ()
main = do
  e <- newEnv
  runStderrLoggingT . runIO e $ do
    sc <- async sessionCleanup
    pc <- async persistenceCleanup
    -- Plaintext server
    serv <- async (withRunInIO $ \unl -> runTCPServer (serverSettings 1883 "*") (unl . tcpApp))
    -- TLS Server
    let cfile = "certificate.pem"
        kfile = "key.pem"
    sserv <- async (withRunInIO $ \unl -> runGeneralTCPServerTLS (tlsConfig "*" 8883 cfile kfile) (unl . tcpApp))
    -- Websockets server
    ws <- async (withRunInIO $ \unl -> WS.runServer "0.0.0.0" 8080 (unl . webSocketsApp))

    void $ waitAnyCancel [sc, pc, serv, sserv, ws]
