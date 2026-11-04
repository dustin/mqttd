{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}

-- | What build of mqttd this is.
module MQTTD.Version (versionString, gitRevision) where

import           Control.Exception          (SomeException, try)
import           Data.Version               (showVersion)
import           Language.Haskell.TH        (runIO)
import           Language.Haskell.TH.Syntax (addDependentFile, lift)
import           System.Directory           (doesFileExist, makeAbsolute)
import           System.Environment         (lookupEnv)
import           System.Process             (readProcess)

import           Paths_mqttd                (version)

-- | The git revision this was built from: $MQTTD_GIT_REV if it's set
-- (for building without git), or else the short hash of git's HEAD (with
-- jj, the last commit).  Nothing if neither is available.
gitRevision :: Maybe String
gitRevision = $(do
  -- Recompile when HEAD moves.  (With jj, or any detached HEAD, it holds
  -- the commit hash itself.)
  headFile <- runIO (makeAbsolute ".git/HEAD")
  hasHead <- runIO (doesFileExist headFile)
  if hasHead then addDependentFile headFile else pure ()
  rev <- runIO (lookupEnv "MQTTD_GIT_REV") >>= \case
    Just r | not (null r) -> pure (Just r)
    _ -> runIO $ either (\(_ :: SomeException) -> Nothing) (Just . takeWhile (/= '\n'))
                   <$> try (readProcess "git" ["rev-parse", "--short", "HEAD"] "")
  lift rev)

-- | e.g. "mqttd 0.9.6.0 (0632df7)"
versionString :: String
versionString = "mqttd " <> showVersion version <> maybe "" (\r -> " (" <> r <> ")") gitRevision
