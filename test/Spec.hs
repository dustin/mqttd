import           Test.QuickCheck.Checkers
import           Test.QuickCheck.Classes
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck    as QC

import           MQTTD.Config

testConfigFiles :: Assertion
testConfigFiles =
  mapM_ aTest [
      ("test.conf", Config {_confDebug = True,
                            _confUsers = [
                               User "myuser" "mypw",
                               User "otheruser" "otherpw"
                               ],
                            _confListeners = [MQTTListener "*" 1883 mempty,
                                              WSListener "*" 8080 mempty,
                                              MQTTSListener "*" 8883 "certificate.pem" "key.pem"
                                              (ListenerOptions (Just False) )]}),
      ("test2.conf", Config {_confDebug = False,
                             _confUsers = [],
                             _confListeners = [MQTTListener "*" 1883 mempty,
                                               MQTTSListener "*" 8883 "certificate.pem" "key.pem" mempty]})
      ]
  where
    aTest (f,w) = assertEqual f w =<< parseConfFile ("test/" <> f)
    noops = ListenerOptions Nothing

instance EqProp ListenerOptions where (=-=) = eq

instance Arbitrary ListenerOptions where
  arbitrary = ListenerOptions <$> arbitrary

tests :: [TestTree]
tests = [
  testCase "config files" testConfigFiles,

  testGroup "listener properties" [
      testProperties "semigroup" (unbatch $ semigroup (undefined :: ListenerOptions, undefined :: Int)),
      testProperties "monoid" (unbatch $ monoid (undefined :: ListenerOptions))
      ]
  ]

main :: IO ()
main = defaultMain $ testGroup "All Tests" tests
