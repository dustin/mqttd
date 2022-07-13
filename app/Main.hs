module Main where

import           Control.Monad (void)
import           System.Environment (getArgs)
import           UnliftIO      (waitAnyCancel)

import           MQTTD.Config
import           MQTTD.Main

configFilePath :: [String] -> String
configFilePath [] = "mqttd.conf"
configFilePath (path: _) = path

main :: IO ()
main = void . waitAnyCancel =<< runServer =<< parseConfFile . configFilePath =<< getArgs
