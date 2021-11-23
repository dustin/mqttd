module MQTTD.Logging where

import           Control.Monad.Logger (MonadLogger (..), logDebugN, logErrorN, logInfoN)
import           Data.Foldable        (fold)

import           Data.Text            (Text)

logError, logInfo, logDbg :: MonadLogger m => Text -> m ()

logError = logErrorN
logInfo = logInfoN
logDbg = logDebugN

logErrorL, logInfoL, logDbgL :: (Foldable f, MonadLogger m) => f Text-> m ()

logErrorL = logErrorN . fold
logInfoL = logInfoN . fold
logDbgL = logDebugN . fold
