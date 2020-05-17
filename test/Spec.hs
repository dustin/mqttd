import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck as QC

import           MQTTD.Config

testConfigFiles :: Assertion
testConfigFiles =
  mapM_ aTest [
      ("test.conf", Config {_confDebug = True,
                            _confUsers = [
                               User "myuser" "mypw",
                               User "otheruser" "otherpw"
                               ],
                            _confListeners = [MQTTListener "*" 1883,
                                              WSListener "*" 8080,
                                              MQTTSListener "*" 8883 "certificate.pem" "key.pem"]}),
      ("test2.conf", Config {_confDebug = False,
                             _confUsers = [],
                             _confListeners = [MQTTListener "*" 1883,
                                               MQTTSListener "*" 8883 "certificate.pem" "key.pem"]})
      ]
  where
    aTest (f,w) = assertEqual f w =<< parseConfFile ("test/" <> f)

tests :: [TestTree]
tests = [
  testCase "config files" testConfigFiles
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
