module Main where

import           Control.Exception    (bracket_)
import           Data.Password.Bcrypt
import           Data.Text            (Text)
import qualified Data.Text            as T
import           Options.Applicative
import           System.IO            (hFlush, hGetEcho, hSetEcho, stdin, stdout)

data Options = Options { optArgs :: [Text] }

options :: Parser Options
options = Options <$> many (argument str (metavar "[password ...]"))

withEcho :: Bool -> IO a -> IO a
withEcho echo action = do
  putStr "Enter password: " >> hFlush stdout
  old <- hGetEcho stdin
  bracket_ (hSetEcho stdin echo) (hSetEcho stdin old >> putStrLn "") action

getPass :: IO String
getPass = withEcho False getLine

genPassInteractive :: IO ()
genPassInteractive = do
  p <- T.pack <$> getPass
  hp <- unPasswordHash <$> hashPassword (mkPassword p)
  print hp

genPasses :: [Text] -> IO ()
genPasses [] = genPassInteractive
genPasses xs = mapM_ en xs
  where
    en p = do
      hp <- unPasswordHash <$> hashPassword (mkPassword p)
      putStr (show p)
      putStr " → "
      print hp

main :: IO ()
main = genPasses . optArgs =<< execParser opts
  where
    opts = info (options <**> helper)
           ( fullDesc <> progDesc "mqttd password hash utility")
