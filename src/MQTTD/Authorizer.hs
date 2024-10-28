{-# LANGUAGE TemplateHaskell #-}

module MQTTD.Authorizer where

import           Cleff
import           Control.Monad        (unless, when)
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map.Strict      as Map
import           Data.Maybe           (fromMaybe)
import           Data.Password.Bcrypt (PasswordCheck (..), checkPassword, mkPassword)
import qualified Data.Text.Encoding   as TE
import           Network.MQTT.Types

import           MQTTD.Config
import           MQTTD.Types

data AuthFX :: Effect where
  Authorize :: ConnectRequest -> AuthFX m (Either String ())
  GetACL :: Maybe BL.ByteString -> AuthFX m [ACL]

makeEffect ''AuthFX

runAuth :: Authorizer -> Eff (AuthFX : es) a -> Eff es a
runAuth Authorizer{..} = interpret \case
    Authorize ConnectRequest{..} ->
      pure . unless _authAnon $ do
        uname <- maybe (Left "anonymous clients are not allowed") Right _username
        (User _ want _) <- maybe inv Right (Map.lookup uname _authUsers)
        pass <- maybe (Right "") Right _password
        case want of
          Plaintext p   -> when (pass /= p) inv
          HashedPass hp -> when (checkPassword (topw pass) hp == PasswordCheckFail) inv

      where inv = Left "invalid username or password"
            topw = mkPassword . TE.decodeUtf8 . BL.toStrict

    GetACL un -> pure $ fromMaybe [] (fmap (\(User _ _ a) -> a) . (`Map.lookup` _authUsers) =<< un)
