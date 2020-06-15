module Main where

import           Control.Monad (void)
import           UnliftIO      (waitAnyCancel)

import           MQTTD.Config
import           MQTTD.Main

main :: IO ()
main = void . waitAnyCancel =<< runServer =<< parseConfFile "mqttd.conf"
