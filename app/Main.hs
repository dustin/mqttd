module Main where

import           Control.Concurrent   (myThreadId, throwTo)
import           Control.Exception    (AsyncException (UserInterrupt))
import           Control.Monad        (void)
import           System.Environment   (getArgs)
import           System.Posix.Signals (Handler (Catch), installHandler, sigTERM)
import           UnliftIO             (waitAnyCancel)

import           MQTTD.Config
import           MQTTD.Main

configFilePath :: [String] -> String
configFilePath []        = "mqttd.conf"
configFilePath (path: _) = path

main :: IO ()
main = do
  -- SIGTERM shuts down like SIGINT: the services are cancelled, so
  -- pending DB writes are finished before the process exits.  (By
  -- default, SIGTERM ends the process on the spot.)
  tid <- myThreadId
  void $ installHandler sigTERM (Catch (throwTo tid UserInterrupt)) Nothing
  void . waitAnyCancel =<< runServer =<< parseConfFile . configFilePath =<< getArgs
