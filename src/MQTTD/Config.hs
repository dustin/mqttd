module MQTTD.Config (Config(..), User(..), Listener(..), parseConfFile) where

import           Control.Applicative        ((<|>))
import           Data.Conduit.Network       (HostPreference)
import           Data.Foldable              (asum)
import           Data.String                (IsString (..))
import           Data.Text                  (Text, pack)
import           Data.Void                  (Void)
import           Text.Megaparsec            (Parsec, between, manyTill, option, parse, some, try)
import           Text.Megaparsec.Char       (char, space, space1)
import qualified Text.Megaparsec.Char.Lexer as L
import           Text.Megaparsec.Error      (errorBundlePretty)


type Parser = Parsec Void Text

type ListenAddress = String
type PortNumber = Int

data User = User Text Text deriving (Show, Eq)

data Listener = MQTTListener HostPreference PortNumber
              | MQTTSListener HostPreference PortNumber FilePath FilePath
              | WSListener ListenAddress PortNumber
              deriving (Show, Eq)

data Config = Config {
  _confDebug     :: Bool,
  _confUsers     :: [User],
  _confListeners :: [Listener]
  } deriving (Show, Eq)

sc :: Parser ()
sc = L.space space1 (L.skipLineComment "#" <* space) (L.skipBlockComment "/*" "*/")

sc' :: Parser a -> Parser a
sc' = (sc *>)

lexeme :: Parser a -> Parser a
lexeme = L.lexeme sc

symbol :: Text -> Parser Text
symbol = L.symbol sc

symbeq :: Text -> Parser Text
symbeq x = symbol x <* symbol "="

qstr :: IsString a => Parser a
qstr = fromString <$> (char '"' >> manyTill L.charLiteral (char '"'))

parseListener :: Parser Listener
parseListener = symbol "listener" *> (asum . map (sc' . try)) [mqtt, mqtts, ws]
  where
    mqtt =  symbol "mqtt"  *> (MQTTListener <$> lexeme qstr <*> lexeme L.decimal)
    mqtts = symbol "mqtts" *> (MQTTSListener <$> lexeme qstr <*> lexeme L.decimal <*> lexeme qstr <*> lexeme qstr)
    ws =    symbol "ws"    *> (WSListener <$> lexeme qstr <*> lexeme L.decimal)

parseUser :: Parser User
parseUser = User <$> (symbol "user" *> lexeme qstr) <*> (symbol "password" *> lexeme qstr)

namedList :: Text -> Parser p -> Parser [p]
namedList s p = namedValue s $ between "[" "]" (some (sc *> lexeme p))

namedValue :: Text -> Parser p -> Parser p
namedValue s p = symbeq s *> p

parseListeners :: Parser [Listener]
parseListeners = namedList "listeners" parseListener

parseConfig :: Parser Config
parseConfig = Config
              <$> sc' (option False $ namedValue "debug" parseBool)
              <*> sc' (option [] $ namedList "users" parseUser)
              <*> sc' parseListeners

parseBool :: Parser Bool
parseBool = True <$ lexeme "true" <|> False <$ lexeme "false"

parseFile :: Parser a -> String -> IO a
parseFile f s = pack <$> readFile s >>= either (fail.errorBundlePretty) pure . parse f s

parseConfFile :: String -> IO Config
parseConfFile = parseFile parseConfig
