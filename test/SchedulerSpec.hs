{-# LANGUAGE ScopedTypeVariables #-}

module SchedulerSpec where

import           Cleff
import           Control.Concurrent.Async (withAsync)
import           Control.Concurrent.STM   (modifyTVar', newTVarIO, readTVar, readTVarIO, registerDelay)
import           Control.Monad.Logger     (LogLine)
import           Data.Foldable            (traverse_)
import           Data.List                (sort)
import           Data.Maybe               (listToMaybe)
import qualified Data.Set                 as Set
import           Data.String              (IsString (..))
import           Data.Time
import           UnliftIO                 (atomically, writeTVar)

import           Hedgehog
import qualified Hedgehog.Gen             as Gen
import qualified Hedgehog.Range           as Range

import           MQTTD.Logging
import           MQTTD.Stats
import           Scheduler

-- https://github.com/hedgehogqa/haskell-hedgehog/issues/215
genUTCTime :: MonadGen m => m UTCTime
genUTCTime = do
    y <- toInteger <$> Gen.int (Range.constant 2000 2019)
    m <- Gen.int (Range.constant 1 12)
    d <- Gen.int (Range.constant 1 28)
    secs <- toInteger <$> Gen.int (Range.constant 0 86401)
    pure $ UTCTime (fromGregorian y m d) (secondsToDiffTime secs)

genCollidingQueueID :: MonadGen m => m QueueID
genCollidingQueueID = do
    t <- Gen.element [UTCTime (fromGregorian 2019 1 1) 0, UTCTime (fromGregorian 2019 1 1) 1, UTCTime (fromGregorian 2020 2 1) 2]
    i <- Gen.int (Range.linear 0 5)
    pure $ QueueID t i

genDates :: MonadGen m => m [UTCTime]
genDates = Gen.list (Range.linear 1 100) genUTCTime

populate :: [UTCTime] -> TimedQueue Int
populate = foldr (\t -> add (QueueID t 0) 0) mempty

zeroDate :: UTCTime
zeroDate = UTCTime (fromGregorian 1970 1 1) 0

future :: UTCTime
future = UTCTime (fromGregorian 2023 1 1) 0

hprop_queueNotReady :: Property
hprop_queueNotReady = property $ do
    dates <- forAll genDates
    let tq = populate dates
        (todo, tq') = ready zeroDate tq
    annotateShow (todo, tq')
    length todo === 0

hprop_queueAllReady :: Property
hprop_queueAllReady = property $ do
    dates <- forAll genDates
    let tq = populate dates
        (todo, tq') = ready future tq
    annotateShow (todo, tq')
    length todo === length dates

hprop_queueAllReadyColliding :: Property
hprop_queueAllReadyColliding = property $ do
    items <- zipWith (\n q -> q{_qidI = n}) [0..] <$> forAll (Gen.list (Range.linear 1 100) genCollidingQueueID)
    let tq = foldr (uncurry add) (mempty :: TimedQueue Int) (zip items [0..])
        (todo, tq') = ready future tq
    annotateShow (todo, tq')
    sort todo === sort [0.. length items - 1]

hprop_doAllTimings :: Property
hprop_doAllTimings = property $ do
    items <- zipWith (\n q -> q{_qidI = n}) [0..] <$> forAll (Gen.list (Range.linear 1 100) genCollidingQueueID)
    let tq = foldr (uncurry add) (mempty :: TimedQueue Int) (zip items [0..])
        timestamps = Set.toList . Set.fromList $ [addUTCTime (-1), id, addUTCTime 1] <*> (_qidT <$> items)
        done = foldMap fst $ scanl (\(_, tq') t -> ready t tq') (mempty, tq) timestamps
    annotateShow (done, tq)
    sort done === [0 .. length items - 1]

hprop_queueNextIsLowest :: Property
hprop_queueNextIsLowest = property $ do
    dates <- forAll genDates
    next (populate dates) === listToMaybe (sort dates)

runTestOnce ::  Eff [Stats, LogFX, IOE] a -> IO [LogLine]
runTestOnce a = newStatStore >>= \ss -> withAsync (applyStats ss) (\_ -> (fmap snd . runIOE . runLogWriter . runStats ss) a)

nr :: MonadIO m => m (QueueRunner Int)
nr = newRunner

hprop_runnerRuns :: Property
hprop_runnerRuns = property $ do
    runner <- nr
    ndates <- zip [1..] <$> forAll genDates
    nvar <- liftIO $ newTVarIO 0
    clock <- liftIO $ fixedClock future
    logs <- liftIO $ runTestOnce $ do
        traverse_ (\(n,t) -> enqueue t n runner) ndates
        runOnce clock (\x -> atomically $ modifyTVar' nvar (+ x)) runner
    annotateShow logs
    [fromString ("Running " <> show (length ndates) <> " actions")] === fmap (\(_, _, _, s) -> s) logs
    tsum <- liftIO $ readTVarIO nvar
    tsum === sum (fst <$> ndates)

fixedClock :: UTCTime -> IO Clock
fixedClock t = pure $ Clock (pure t) (\_ -> registerDelay 1000)

testClock :: [UTCTime] -> IO Clock
testClock timesIn = do
    times <- newTVarIO $ sort $ [before zeroDate, after future] <> foldMap (\t -> [before t, t, after t]) timesIn
    let nextTime = atomically $ do
            t <- readTVar times
            case t of
                [] -> error "OUTATIME"
                (x:xs) -> do
                    writeTVar times xs
                    pure x
        reg _ = registerDelay 1000
    pure $ Clock nextTime reg
  where
    before = addUTCTime (-1)
    after = addUTCTime 1

hprop_runnerCancels :: Property
hprop_runnerCancels = property $ do
    runner <- nr
    ndates <- zip [1..] <$> forAll genDates
    clock <- liftIO $ fixedClock future
    nvar <- liftIO $ newTVarIO 0
    logs <- liftIO $ runTestOnce $ do
        ids <- traverse (\(n,t) -> enqueue t n runner) ndates
        ss <- getStatStore
        traverse_ (\i -> atomically $ cancelSTM ss i runner) ids
        _ <- enqueue zeroDate 0 runner
        runOnce clock (\x -> atomically $ modifyTVar' nvar (+ x)) runner
    annotateShow logs
    ["Running 1 actions"] === fmap (\(_, _, _, s) -> s) logs
    tsum <- liftIO $ readTVarIO nvar
    tsum === 0
