# Changelog for mqttd

## 0.9.5.0

Using cleff for effect management.

## 0.9.0.0

I've been running in production for over a year with no issues to
speak of.  I've not kept any useful changelog, so to get this far,
here's a slightly filtered log from inception:

* 2020-05-02 Kind of working.
* 2020-05-02 MQTTD Monad can wrap more stuff and use MonadLogger.
* 2020-05-02 Trying to keep a little client state.
* 2020-05-03 Register/unregister and logging.
* 2020-05-03 Generate client IDs when clients don't bother.
* 2020-05-03 Track sessions instead of connections.
* 2020-05-03 Generate new pkt IDs for broadcasting.
* 2020-05-03 Pass the client info around when dispatching.
* 2020-05-03 Implement subscriptions as a list.
* 2020-05-03 Use a TBQueue instead of a TChan
* 2020-05-03 Move the message queue to the session.
* 2020-05-03 Move subscriptions into the sessions themselves.
* 2020-05-03 I can tell whether I'm resuming a session or not now.
* 2020-05-04 Update net-mqttd and use SessionReuse instead of Bool
* 2020-05-04 Expire detached sessions.
* 2020-05-04 Clean up sessions around the right time.
* 2020-05-04 Remove session expiry when establishing a new client.
* 2020-05-05 Resolve incoming aliases.
* 2020-05-05 Properly useful debugging for incoming and outgoing packets.
* 2020-05-05 Basic QoS 1 handling.
* 2020-05-06 Cleaner as a toplevel in MQTTD
* 2020-05-06 Move the lastwill into the session and fire it when the session dies.
* 2020-05-06 Expire sessions at approximately the right time.
* 2020-05-07 Work scheduler.
* 2020-05-07 Use the scheduler thing to run cleanups.
* 2020-05-07 Slight cleanup
* 2020-05-07 Handle subscriber options.
* 2020-05-08 Disconnect clients if we haven't heard from them in a while.
* 2020-05-08 Need to remember to dedup subscriptions.
* 2020-05-08 Store subscriptions in a Map.
* 2020-05-08 Implemented unsubscribe.
* 2020-05-09 Stop copying and pasting liftSTM everywhere.
* 2020-05-09 Beginning retained messages.
* 2020-05-09 Don't remember the TopicAlias property after resolving aliases.
* 2020-05-09 Pass a (more or less) complete message through broadcast.
* 2020-05-09 Basic retention support.
* 2020-05-09 Put Retention in a more sensible place.
* 2020-05-09 TLS support with hard-coded cert paths.
* 2020-05-09 Adjust message expiry interval property when publishing retained messages.
* 2020-05-09 QoS 1 is fine.
* 2020-05-09 Use .mapped instead of %~ fmap
* 2020-05-09 I don't need partsof and %~ and stuff to update a value.
* 2020-05-09 Yeah, QoS 1 and 2 will need a bit more work on the transmission side.
* 2020-05-09 Basic websockets support.
* 2020-05-10 liftSTM is UnliftIO.
* 2020-05-10 Handle retransmits on outbound publishes.
* 2020-05-10 Using ConstraintKinds to have not-so-significantly-long conastraints.
* 2020-05-10 QoS 2 handling.
* 2020-05-11 Simplify 'maybe (pure ()) ...' with justM
* 2020-05-11 Refactor so that everything isn't in Main
* 2020-05-11 Remove the remaining conduit helper from Main
* 2020-05-11 Note about how to deal with retransmits better
* 2020-05-11 Greatly simplified qos1/qos2 handling by adding correctness.
* 2020-05-11 Flush publishes from the session outbound queue.
* 2020-05-11 Should we allow clients to reuse sessions without explicit expiration?
* 2020-05-11 Remove TODO about STM broadcast.
* 2020-05-11 Added default session expiration.
* 2020-05-11 Get all the machinery running before running retransmits.
* 2020-05-11 Rename runIn to processIn to be more consistent.
* 2020-05-11 Allow clients to request disconnect with will.
* 2020-05-11 Replace the session queue on each connect.
* 2020-05-12 I don't sleep anymore.
* 2020-05-12 Replace a bunch of small declarations that make an empty session with emptySession
* 2020-05-12 Use sendPacket*_ instead of void .
* 2020-05-12 Support outbound aliases.
* 2020-05-15 Use published net-mqtt and net-mqtt-lens
* 2020-05-16 Made a config file for listeners and stuff.
* 2020-05-16 Use more normal sc/symbol/lexeme patterns while parsing.
* 2020-05-16 Log listeners on startup.
* 2020-05-16 Adding some debug control to the config.
* 2020-05-16 Added a test for config parsing.
* 2020-05-16 I started on the config file thing.
* 2020-05-16 Make a list of listeners in the config.
* 2020-05-16 Add debug flag to config file.
* 2020-05-16 Add some users to the config file.
* 2020-05-16 Add default options section and make the ordering not matter.
* 2020-05-17 Allow options on listeners.
* 2020-05-17 Rename _opt_allow_anonymous to _optAllowAnonymous
* 2020-05-17 Move the emptiness of an option into the option.
* 2020-05-17 Small hlint suggestion in parsing.
* 2020-05-17 Run connections through an authorizer.
* 2020-05-17 Wired up toplevel defaults for anonymous authorization.
* 2020-05-17 Honor per-listener anonymous filter.
* 2020-05-17 More informative unauthorized connection refusal.
* 2020-05-17 Config users should be a Map by username.
* 2020-05-17 Simple user/pass authentication.
* 2020-05-17 Simplify running listener by consolidating modification into one place
* 2020-05-17 ACL parsing.
* 2020-05-17 Enforce ACLs on publish and subscribe.
* 2020-05-17 A bit easier-to-follow subscribe authorization.
* 2020-05-17 Tests for ACL evaluation.
* 2020-05-17 Adjusting some config comments.
* 2020-05-17 Created SubTree to efficiently manage subscriptions.
* 2020-05-17 Make SubTree Foldable and Functor at least look similar.
* 2020-05-18 Property tests for SubTree.
* 2020-05-18 Semigroup and Monoid for SubTree
* 2020-05-18 Simpler modify
* 2020-05-18 SubTree flatten and fromList
* 2020-05-18 Made SubTree a bit more generic by holding any monoidal type.
* 2020-05-18 Document and be clearer on types.
* 2020-05-18 Render some docs.
* 2020-05-18 subs explicitly having or not having a value makes things make more sense
* 2020-05-18 modify no longer requires monoidal a.
* 2020-05-18 Oh look, I can just derive Functor and Traversable
* 2020-05-18 Use SubTree for tracking subscriptions globally.
* 2020-05-18 I don't know why I was thinking maps weren't monoidal.
* 2020-05-20 Split out basic type definitions from MQTTD.hs
* 2020-05-20 Rename Persistence to Retainer
* 2020-05-20 Helper for modifying an stm var and returning the new value.
* 2020-05-24 Store and retrieve sessions and retained messages.
* 2020-05-25 Config for persistence.
* 2020-05-25 Replace BL.ByteString with readable types.
* 2020-05-25 Batch up storage requests.
* 2020-05-26 Remember LastWill for sessions across restart.
* 2020-05-26 Deliver will properties.
* 2020-05-26 On MQTTDuplicate, include the session ID in the error.
* 2020-05-26 Don't match $-prefix with wildcards.
* 2020-05-26 Don't wait for publishing retained messages before sending sub ack.
* 2020-05-26 Remove some extraneous parens.
* 2020-05-26 Don't persist $SYS/-prefixed topics.
* 2020-05-26 Doing some basic stats.
* 2020-05-26 Publish some message counters.
* 2020-05-26 net-mqtt update
* 2020-05-27 Add $SYS/broker/subscriptions/count
* 2020-05-27 Add some storage stats.
* 2020-05-27 Don't just drop the session from the map when unregistering.
* 2020-05-27 Reuse modifySession to modify the session in unregisterClient
* 2020-05-27 Immediately expire sessions that don't have any subscriptions.
* 2020-05-28 Immediately expire more sessions on disconnect.
* 2020-05-28 Don't log retention about $SYS/ things.
* 2020-05-28 Do not allow clients to publish to empty topics.
* 2020-05-28 Clean up connection logging a bit.
* 2020-05-28 Even more conn log cleanup.
* 2020-05-28 Nicer looking property logging on connect
* 2020-05-28 Added some RTS stats under $SYS
* 2020-05-29 Don't transform gc elapsed timestamp.
* 2020-05-29 Generate arbitrary topics with some arbitrary matches for SubTree testing.
* 2020-05-28 Remove some redundant imports I got from moving things around.
* 2020-05-29 I apparently had two arbitrary topic instances.
* 2020-05-30 Speed up tests and document collisions in subtree testing.
* 2020-05-30 Adapt the new arbitraryMatchingTopic from net-mqtt
* 2020-05-30 A bit of documentation updates.
* 2020-05-30 Honor client "receive maximum"
* 2020-05-30 Drop packets destined for backlog if the queue is full.
* 2020-05-31 Log the connection source.
* 2020-06-14 Update to the latest stack resolver.
* 2020-06-15 Moved the main loop of main into a reusable module.
* 2020-06-15 A basic operational integration test.
* 2020-06-15 Don't log to stderr from tests.
* 2020-06-15 Some more coverage in basic tests.
* 2020-06-15 Write a test for nextPktID
* 2020-06-15 Run through the aliases code in tests.
* 2020-06-19 use "Generic" conduit adaptors for TCP
* 2020-06-19 MQTTListener doesn't return until binding is complete.
* 2020-06-19 Simplify blocking in MQTTListener a bit.
* 2020-06-19 Send "dontcare" in QoS1 to prevent race.
* 2020-06-19 Connect pubber and subber concurrently in tests.
* 2020-06-19 Log connect and disconnect similarly
* 2020-06-19 Maintain the persistence connection with at thread in the async list
* 2020-06-20 Add -Wall to testing.
* 2020-06-21 Support shared subscriptions.
* 2020-06-21 Tests for ACLs and logins and stuff.
* 2020-08-16 Added default.nix
* 2020-10-01 I spelled subscribe incorrectly in DB restoration.
* 2020-10-03 Trying a test workflow with cachix.
* 2020-10-03 Remove stack setup thing from CI
* 2020-10-06 Added integration test to verify RetainAsPublished
* 2020-10-06 Don't bother cleaning up retained sys topics
* 2020-11-14 Turns out I can derive Traversable as well.
* 2020-11-14 Default config to in-memory DB.
* 2020-11-14 HasStats class to simplify the stat access.
* 2020-11-14 Added stats for action execution
* 2020-11-14 Increment the action executed stat by the number of stats executed
* 2021-04-15 More lexeme, less symbol
* 2021-06-14 One sqlite-simple should be enough.
* 2021-06-19 Add a comment as to why withConnection isn't used
* 2021-06-19 More granular ACLs.
* 2021-06-20 Use Network.MQTT.Arbitrary for arbitrary topics
* 2021-06-20 Don't reinvent Arbitrary sizing.
* 2021-06-20 Support bcrypt hashed passwords.
* 2021-06-20 Wrote a property test for ACLs.
* 2021-06-20 Updating my nix/cachix workflow.
* 2021-06-20 Rename test.yml to nix-build.yml
* 2021-06-20 Include mqttdpass in artifacts
* 2021-06-20 Comment in the toplevel config about the mqttdpass command.
