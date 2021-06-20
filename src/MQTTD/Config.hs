module MQTTD.Config (Config(..), Creds(..), User(..), ACLAction(..), ACL(..), PersistenceConfig(..),
                     Listener(..), ListenerOptions(..), listenerOpts,
                     parseConfFile) where

import           Control.Applicative        ((<|>))
import           Control.Lens
import qualified Data.ByteString.Lazy       as BL
import           Data.Conduit.Network       (HostPreference)
import           Data.Foldable              (asum)
import           Data.Map.Strict            (Map)
import qualified Data.Map.Strict            as Map
import           Data.Password.Bcrypt       (Bcrypt, PasswordHash (..))
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

data ACLAction = ACLSub | ACLPubSub deriving (Show, Eq)

data ACL = Allow ACLAction T.Filter | Deny T.Filter deriving (Show, Eq)

data Creds = Plaintext BL.ByteString | HashedPass (PasswordHash Bcrypt) deriving (Show, Eq)

data User = User BL.ByteString Creds [ACL] deriving (Show, Eq)

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
    r (MQTTListener _ _ o)      = o
    r (MQTTSListener _ _ _ _ o) = o
    r (WSListener _ _ o)        = o
    w (MQTTListener a b _) o      = MQTTListener a b o
    w (MQTTSListener a b c d _) o = MQTTSListener a b c d o
    w (WSListener a b _) o        = WSListener a b o

newtype PersistenceConfig = PersistenceConfig { _persistenceDBPath :: FilePath } deriving (Show, Eq)

data Config = Config {
  _confDebug     :: Bool,
  _confUsers     :: Map BL.ByteString User,
  _confListeners :: [Listener],
  _confDefaults  :: ListenerOptions,
  _confPersist   :: PersistenceConfig
  } deriving (Show, Eq)

data Section = DebugSection Bool
             | UserSection [User]
             | DefaultsSection ListenerOptions
             | ListenerSection [Listener]
             | PersistenceSection PersistenceConfig
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
parseListener = lexeme "listener" *> (choice . map (sc' . try)) [mqtt, mqtts, ws] <*> o
  where
    mqtt =  lexeme "mqtt"  *> (MQTTListener <$> lexeme qstr <*> lexeme L.decimal)
    mqtts = lexeme "mqtts" *> (MQTTSListener <$> lexeme qstr <*> lexeme L.decimal <*> lexeme qstr <*> lexeme qstr)
    ws =    lexeme "ws"    *> (WSListener <$> lexeme qstr <*> lexeme L.decimal)

    o = option mempty (lexeme parseListenOpts)

parseUser :: Parser User
parseUser = User <$> (lexeme "user" *> lexeme qstr) <*> passwd
            <*> option [] (lexeme parseACL)
  where parseACL = lexeme "acls" *> between "[" "]" (some (sc' (lexeme aclEntry)))
        aclEntry = asum . map try $ [Allow ACLPubSub <$> (lexeme "allow" *> lexeme qstr),
                                     Allow ACLSub <$> (lexeme "allow" *> lexeme "read" *> lexeme qstr),
                                     Deny <$> (lexeme "deny" *> lexeme qstr)]
        passwd = Plaintext <$> (lexeme "password" *> lexeme qstr)
                 <|> HashedPass . PasswordHash <$> (lexeme "hashedpass" *> lexeme qstr)

namedList :: Text -> Parser p -> Parser [p]
namedList s p = namedValue s $ between "[" "]" (some (sc *> lexeme p))

namedValue :: Text -> Parser p -> Parser p
namedValue s p = symbeq s *> lexeme p

parseListenOpts :: Parser ListenerOptions
parseListenOpts = between "{" "}" (ListenerOptions <$> sc' (lexeme aListenOpt))
  where
    aListenOpt = namedValue "allow_anonymous" (Just <$> parseBool)

parsePersistence :: Parser PersistenceConfig
parsePersistence = between "{" "}" (PersistenceConfig <$> sc' (lexeme aListenOpt))
  where
    aListenOpt = namedValue "db" (lexeme qstr)

parseSection :: Parser Section
parseSection = (choice . map sc') [
  try (DebugSection <$> namedValue "debug" parseBool),
  DefaultsSection <$> namedValue "defaults" parseListenOpts,
  UserSection <$> namedList "users" parseUser,
  ListenerSection <$> namedList "listeners" parseListener,
  PersistenceSection <$> namedValue "persistence" parsePersistence
  ]

parseConfig :: Parser Config
parseConfig = foldr up (Config False mempty mempty mempty (PersistenceConfig ":memory:")) <$> sc' (some parseSection)

    where
      up (DebugSection d) c = c{_confDebug=d}
      up (UserSection l) c@Config{..} =
        c{_confUsers=Map.union _confUsers . Map.fromList . map (\u@(User n _ _) -> (n,u)) $ l}
      up (ListenerSection l) c@Config{..} = c{_confListeners=_confListeners <> l}
      up (DefaultsSection l) c = c{_confDefaults=l}
      up (PersistenceSection l) c = c{_confPersist=l}

parseBool :: Parser Bool
parseBool = True <$ lexeme "true" <|> False <$ lexeme "false"

parseFile :: Parser a -> String -> IO a
parseFile f s = readFile s >>= (either (fail.errorBundlePretty) pure . parse f s) . pack

parseConfFile :: String -> IO Config
parseConfFile = parseFile parseConfig
