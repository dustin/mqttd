module MQTTD.Config (Config(..), Listener(..), parseConfFile) where

import           Control.Applicative        ((<|>))
import           Control.Monad              (void)
import           Data.Conduit.Network       (HostPreference)
import           Data.String                (IsString (..))
import           Data.Text                  (Text, pack)
import           Data.Void                  (Void)
import           Text.Megaparsec            (Parsec, endBy1, manyTill, parse, some, takeWhile1P, try)
import           Text.Megaparsec.Char       (char, space)
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
  _confDebug     :: Bool,
  _confListeners :: [Listener]
  } deriving Show

sc :: Parser ()
sc = L.space s (L.skipLineComment "#" <* space) (L.skipBlockComment "/*" "*/")
  where s = void $ takeWhile1P (Just "white space") (`elem` [' ', '\t'])

lexeme :: Parser a -> Parser a
lexeme = L.lexeme sc

symbol :: Text -> Parser Text
symbol = L.symbol sc

qstr :: IsString a => Parser a
qstr = fromString <$> (char '"' >> manyTill L.charLiteral (char '"'))

parseListener :: Parser Listener
parseListener = symbol "listener" *> (try mqtt <|> try mqtts <|> ws)
  where
    mqtt =  symbol "mqtt"  *> (MQTTListener <$> lexeme qstr <*> lexeme L.decimal)
    mqtts = symbol "mqtts" *> (MQTTSListener <$> lexeme qstr <*> lexeme L.decimal <*> lexeme qstr <*> lexeme qstr)
    ws =    symbol "ws"    *> (WSListener <$> lexeme qstr <*> lexeme L.decimal)

parseConfig :: Parser Config
parseConfig = Config True <$> endBy1 (sc *> lexeme parseListener) (some "\n")

parseFile :: Parser a -> String -> IO a
parseFile f s = pack <$> readFile s >>= either (fail.errorBundlePretty) pure . parse f s

parseConfFile :: String -> IO Config
parseConfFile = parseFile parseConfig
