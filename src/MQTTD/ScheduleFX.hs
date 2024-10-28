{-# LANGUAGE TemplateHaskell #-}

module MQTTD.ScheduleFX where

import           Cleff
import           Data.Time.Clock (UTCTime)

import           MQTTD.Stats
import           Scheduler       as S

data ScheduleFX a :: Effect where
  Enqueue :: UTCTime -> a -> ScheduleFX a m QueueID

makeEffect ''ScheduleFX

runSchedule :: ([Stats, IOE] :>> es, Ord a) => QueueRunner a -> Eff (ScheduleFX a : es) b -> Eff es b
runSchedule qr = interpret \case
    Enqueue t a -> S.enqueue t a qr
