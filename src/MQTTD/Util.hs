module MQTTD.Util where

import           Control.Concurrent.STM     (STM, TVar, modifyTVar', readTVar)
import           Control.Monad              (when)
import qualified Data.ByteString.Lazy       as BL
import           Data.Foldable              (traverse_)
import           Data.Text                  (Text, pack)
import qualified Data.Text.Encoding         as TE
import           Effectful
import           Effectful.Dispatch.Dynamic (interpret)
import           Effectful.Fail

{-# INLINE textToBL #-}
textToBL :: Text -> BL.ByteString
textToBL = BL.fromStrict . TE.encodeUtf8

{-# INLINE blToText #-}
blToText :: BL.ByteString -> Text
blToText = TE.decodeUtf8 . BL.toStrict

{-# INLINE tshow #-}
tshow :: Show a => a -> Text
tshow = pack . show

{-# INLINE justM #-}
justM :: Monad m => (a -> m ()) -> Maybe a -> m ()
justM = traverse_

-- | Handle Fail by throwing an IOError, so a failure only takes down
-- the thread it occurs in.
runFailIO :: IOE :> es => Eff (Fail : es) a -> Eff es a
runFailIO = interpret \_ -> \case
  Fail msg -> liftIO (ioError (userError msg))

modifyTVarRet :: TVar a -> (a -> a) -> STM a
modifyTVarRet v f = modifyTVar' v f *> readTVar v

{-# INLINE whenM #-}
whenM :: Monad m => m Bool -> m () -> m ()
whenM mb m = mb >>= \b -> when b m
