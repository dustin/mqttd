module MQTTD.Config (Config(..), User(..), ACL(..),
                     Listener(..), ListenerOptions(..), listenerOpts,
                     parseConfFile) where

import           Control.Applicative        ((<|>))
import           Control.Lens
import qualified Data.ByteString.Lazy       as BL
import           Data.Conduit.Network       (HostPreference)
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.String                (IsString (..))
import           Data.Text                  (Text, pack)
import           Data.Void                  (Void)
import qualified Network.MQTT.Topic         as T
import           Text.Megaparsec            (Parsec, between, choice, manyTill, option, parse, some, try)
import           Text.Megaparsec.Char       (char, space, space1)
import qualified Text.Megaparsec.Char.Lexer as L
import           Text.Megaparsec.Error      (errorBundlePretty)

type Parser = Parsec Void Text

type ListenAddress = String
type PortNumber = Int

data ACL = Allow T.Filter | Deny T.Filter deriving (Show, Eq)

data User = User BL.ByteString BL.ByteString [ACL] deriving (Show, Eq)

data ListenerOptions = ListenerOptions {
  _optAllowAnonymous :: Maybe Bool
  } deriving (Eq, Show)

instance Semigroup ListenerOptions where
  (ListenerOptions a) <> (ListenerOptions b) = ListenerOptions (a <|> b)

instance Monoid ListenerOptions where
  mempty = ListenerOptions Nothing

data Listener = MQTTListener HostPreference PortNumber ListenerOptions
              | MQTTSListener HostPreference PortNumber FilePath FilePath ListenerOptions
              | WSListener ListenAddress PortNumber ListenerOptions
              deriving (Show, Eq)

listenerOpts :: Lens' Listener ListenerOptions
listenerOpts = lens r w
  where
    r (MQTTListener _ _ o) = o
    r (MQTTSListener _ _ _ _ o) = o
    r (WSListener _ _ o) = o
    w (MQTTListener a b _) o = MQTTListener a b o
    w (MQTTSListener a b c d _) o = MQTTSListener a b c d o
    w (WSListener a b _) o = WSListener a b o

data Config = Config {
  _confDebug     :: Bool,
  _confUsers     :: Map BL.ByteString User,
  _confListeners :: [Listener],
  _confDefaults  :: ListenerOptions
  } deriving (Show, Eq)

data Section = DebugSection Bool
             | UserSection [User]
             | DefaultsSection ListenerOptions
             | ListenerSection [Listener]
             deriving Show

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
parseListener = symbol "listener" *> (choice . map (sc' . try)) [mqtt, mqtts, ws] <*> o
  where
    mqtt =  symbol "mqtt"  *> (MQTTListener <$> lexeme qstr <*> lexeme L.decimal)
    mqtts = symbol "mqtts" *> (MQTTSListener <$> lexeme qstr <*> lexeme L.decimal <*> lexeme qstr <*> lexeme qstr)
    ws =    symbol "ws"    *> (WSListener <$> lexeme qstr <*> lexeme L.decimal)

    o = option mempty (lexeme parseListenOpts)

parseUser :: Parser User
parseUser = User <$> (symbol "user" *> lexeme qstr) <*> (symbol "password" *> lexeme qstr)
            <*> option [] (lexeme parseACL)
  where parseACL = symbol "acls" *> between "[" "]" (some (sc' (lexeme aclEntry)))
        aclEntry = Allow <$> (symbol "allow" *> lexeme qstr)
                   <|> Deny <$> (symbol "deny" *> lexeme qstr)

namedList :: Text -> Parser p -> Parser [p]
namedList s p = namedValue s $ between "[" "]" (some (sc *> lexeme p))

namedValue :: Text -> Parser p -> Parser p
namedValue s p = symbeq s *> lexeme p

parseListenOpts :: Parser ListenerOptions
parseListenOpts = between "{" "}" (ListenerOptions <$> sc' (lexeme aListenOpt))
  where
    aListenOpt = namedValue "allow_anonymous" (Just <$> parseBool)

parseSection :: Parser Section
parseSection = (choice . map sc') [
  try (DebugSection <$> namedValue "debug" parseBool),
  DefaultsSection <$> namedValue "defaults" parseListenOpts,
  UserSection <$> namedList "users" parseUser,
  ListenerSection <$> namedList "listeners" parseListener
  ]

parseConfig :: Parser Config
parseConfig = foldr up (Config False mempty mempty mempty) <$> sc' (some parseSection)

    where
      up (DebugSection d) c = c{_confDebug=d}
      up (UserSection l) c@Config{..} =
        c{_confUsers=Map.union _confUsers . Map.fromList . map (\u@(User n _ _) -> (n,u)) $ l}
      up (ListenerSection l) c@Config{..} = c{_confListeners=_confListeners <> l}
      up (DefaultsSection l) c = c{_confDefaults=l}

parseBool :: Parser Bool
parseBool = True <$ lexeme "true" <|> False <$ lexeme "false"

parseFile :: Parser a -> String -> IO a
parseFile f s = readFile s >>= (either (fail.errorBundlePretty) pure . parse f s) . pack

parseConfFile :: String -> IO Config
parseConfFile = parseFile parseConfig
