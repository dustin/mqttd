{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell     #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

module MQTTD.ScheduleFX where

import           Data.Time.Clock            (UTCTime)
import           Effectful
import           Effectful.Dispatch.Dynamic (interpret)
import           Effectful.TH

import           MQTTD.Stats
import           Scheduler                  (QueueID, QueueRunner)
import qualified Scheduler                  as S

data ScheduleFX a :: Effect where
  Enqueue :: UTCTime -> a -> ScheduleFX a m QueueID

makeEffect ''ScheduleFX

runSchedule :: (Stats :> es, IOE :> es, Ord a) => QueueRunner a -> Eff (ScheduleFX a : es) b -> Eff es b
runSchedule qr = interpret \_ -> \case
    Enqueue t a -> S.enqueue t a qr
