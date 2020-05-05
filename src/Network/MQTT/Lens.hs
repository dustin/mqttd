{-# LANGUAGE TemplateHaskell #-}

module Network.MQTT.Lens where

import           Control.Lens
import           Network.MQTT.Types

makeLenses ''ConnectRequest

makePrisms ''MQTTPkt

makePrisms ''ProtocolLevel

makePrisms ''Property

makeLenses ''LastWill

makeLenses ''ConnACKFlags

makeLenses ''PublishRequest

makeLenses ''SubOptions

makeLenses ''SubscribeRequest

makeLenses ''PubACK

makeLenses ''PubREC

makeLenses ''PubREL

makeLenses ''PubCOMP

makeLenses ''SubscribeRequest

makeLenses ''SubscribeResponse

makePrisms ''QoS

makePrisms ''ConnACKRC

makePrisms ''RetainHandling

makePrisms ''SubErr

makePrisms ''UnsubStatus

makePrisms ''DiscoReason
