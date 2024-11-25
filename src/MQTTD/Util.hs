module MQTTD.Util where

import           Control.Concurrent.STM (STM, TVar, modifyTVar', readTVar)
import qualified Data.ByteString.Lazy   as BL
import           Data.Foldable          (traverse_)
import           Data.Text              (Text, pack)
import qualified Data.Text.Encoding     as TE

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

modifyTVarRet :: TVar a -> (a -> a) -> STM a
modifyTVarRet v f = modifyTVar' v f *> readTVar v

{-# INLINE whenM #-}
whenM :: Monad m => m Bool -> m () -> m ()
whenM mb m = mb >>= \b -> if b then m else pure ()
