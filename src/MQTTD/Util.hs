module MQTTD.Util where

import           Control.Concurrent.STM (STM, atomically)
import           Control.Monad.IO.Class (MonadIO (..))
import qualified Data.ByteString.Lazy   as BL
import           Data.Text              (Text, pack)
import qualified Data.Text.Encoding     as TE

liftSTM :: MonadIO m => STM a -> m a
liftSTM = liftIO . atomically

textToBL :: Text -> BL.ByteString
textToBL = BL.fromStrict . TE.encodeUtf8

blToText :: BL.ByteString -> Text
blToText = TE.decodeUtf8 . BL.toStrict

tshow :: Show a => a -> Text
tshow = pack . show
