module MQTTD.Config (Config(..), Listener(..), parseConfFile) where

import           Control.Applicative        ((<|>))
import           Data.Conduit.Network       (HostPreference)
import           Data.String                (IsString (..))
import           Data.Text                  (Text, pack)
import           Data.Void                  (Void)
import           Text.Megaparsec            (Parsec, between, endBy1, noneOf, parse, some, try)
import           Text.Megaparsec.Char       (space)
import qualified Text.Megaparsec.Char.Lexer as L
import           Text.Megaparsec.Error      (errorBundlePretty)


type Parser = Parsec Void Text

type ListenAddress = String
type PortNumber = Int

data Listener = MQTTListener HostPreference PortNumber
              | MQTTSListener HostPreference PortNumber FilePath FilePath
              | WSListener ListenAddress PortNumber
              deriving Show

data Config = Config {
  _confListeners :: [Listener]
  } deriving Show

spacey :: Parser a -> Parser a
spacey f = space *> f <* space

qstr :: IsString a => Parser a
qstr = fromString <$> (between "\"" "\"" (some $ noneOf ['"'])  <|> between "'" "'" (some $ noneOf ['\'']))

parseListener :: Parser Listener
parseListener = spacey "listener" *> (try mqtt <|> try mqtts <|> ws)
  where
    mqtt =  spacey "mqtt"  *> (MQTTListener <$> spacey qstr <*> (space *> L.decimal))
    mqtts = spacey "mqtts" *> (MQTTSListener <$> spacey qstr <*> spacey L.decimal <*> spacey qstr <*> (space *> qstr))
    ws =    spacey "ws"    *> (WSListener <$> spacey qstr <*> (space *> L.decimal))

parseConfig :: Parser Config
parseConfig = Config <$> (parseListener `endBy1` "\n")

parseFile :: Parser a -> String -> IO a
parseFile f s = pack <$> readFile s >>= either (fail.errorBundlePretty) pure . parse f s

parseConfFile :: String -> IO Config
parseConfFile = parseFile parseConfig
