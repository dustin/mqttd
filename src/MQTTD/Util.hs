module MQTTD.Util where

import           Control.Concurrent.STM (STM, atomically)
import           Control.Monad.IO.Class (MonadIO (..))

liftSTM :: MonadIO m => STM a -> m a
liftSTM = liftIO . atomically
