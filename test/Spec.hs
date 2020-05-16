import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck as QC

import           MQTTD.Config

testConfigParser :: Assertion
testConfigParser = do
  got <- parseConfFile "test/test.conf"
  let want = Config {_confDebug = True,
                     _confListeners = [MQTTListener "*" 1883,
                                       WSListener "*" 8080,
                                       MQTTSListener "*" 8883 "certificate.pem" "key.pem"]}
  assertEqual "" want got

tests :: [TestTree]
tests = [
  testCase "config parsing" testConfigParser
    ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
