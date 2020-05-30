# mqttd

This is an MQTT 5.0 (and 3.1.1) broker.

I'd been using [mosquitto](https://mosquitto.org/) for a while and
it's worked pretty well, but I wanted a bit more functionality and
had already written much of the packet processing in [my client
library](///github.com/dustin/mqtt-hs), so putting together
something that suits me well was straightforward.

This is currently my production MQTT server with a variety of clients
connected with two sites bridging messages with
[mqtt-bridge](///github.com/dustin/mqtt-bridge) and a listener on the internet with
TLS using a [Let's Encrypt](https://letsencrypt.org/) cert.

## Configuring

See the [example configuration](mqttd.conf)
for an complete example of getting things up and running.
