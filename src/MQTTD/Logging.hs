{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

module MQTTD.Logging where

import           Control.Monad                  (when)
import           Control.Monad.Logger           (Loc (..), LogLevel (..), LogLine, LogSource, LogStr, defaultLoc, fromLogStr)
import qualified Data.ByteString.Char8          as C8
import           Data.Foldable                  (fold)
import           Data.String                    (fromString)
import qualified Data.Text                      as T
import           Effectful
import           Effectful.Dispatch.Dynamic     (interpret, reinterpret)
import           Effectful.TH
import           Effectful.Writer.Static.Shared
import           System.IO                      (stderr)


data LogFX :: Effect where
  LogFX :: Loc -> LogSource -> LogLevel -> LogStr -> LogFX m ()

makeEffect ''LogFX

genericLog :: LogLevel -> LogFX :> es => T.Text -> Eff es ()
genericLog lvl = logFX defaultLoc "" lvl . fromString . T.unpack

logError, logInfo, logDbg :: LogFX :> es => T.Text -> Eff es ()

logErrorL, logInfoL, logDbgL :: (Foldable f, LogFX :> es) => f T.Text -> Eff es ()

logErrorL = logError . fold
logInfoL = logInfo . fold
logDbgL = logDbg . fold

logError = genericLog LevelError

logInfo = genericLog LevelInfo

logDbg = genericLog LevelDebug

baseLogger :: LogLevel -> Loc -> LogSource -> LogLevel -> LogStr -> IO ()
baseLogger minLvl _ _ lvl s = when (lvl >= minLvl) $ C8.hPutStrLn stderr (fromLogStr ls)
  where
    ls = prefix <> ": " <> s
    prefix = case lvl of
               LevelDebug   -> "D"
               LevelInfo    -> "I"
               LevelWarn    -> "W"
               LevelError   -> "E"
               LevelOther x -> fromString . T.unpack $ x


runLogFX :: (IOE :> es) => Bool -> Eff (LogFX : es) a -> Eff es a
runLogFX verbose = interpret \_ -> \case
  LogFX loc src lvl' msg -> liftIO $ baseLogger minLvl loc src lvl' msg
  where minLvl = if verbose then LevelDebug else LevelInfo

runNoLogFX :: Eff (LogFX : es) a -> Eff es a
runNoLogFX = interpret \_ -> \case
  LogFX{} -> pure ()

runLogWriter :: Eff (LogFX : es) a -> Eff es (a, [LogLine])
runLogWriter = reinterpret runWriter \_ -> \case
  LogFX a b c d -> tell [(a,b,c,d)]
