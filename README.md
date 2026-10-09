# Wayseer module: MQTT

An external module for Wayseer Desktop that reads an MQTT broker. It shows the broker's topics as
a tree, the numbers in their payloads as metrics with units, and whether devices say they are
online. It understands Home Assistant MQTT discovery, as zigbee2mqtt, ESPHome, Zwave JS UI,
ebusd and many others send it, and zigbee2mqtt's device list and mesh.

It is not built into the app. It runs as its own program from a signed package, as
`wayseer-labs/mqtt` in the namespace `mqtt`. Like every Wayseer module it is a lens, not a store.
It keeps only a bounded working set in memory and forgets what it no longer hears.

It subscribes to the topic filters you name and publishes nothing, with one exception you opt into:
`zigbee2mqtt_networkmap` (below).

## Configuration

```yaml
modules:
  - kind: external
    name: home
    options:
      module: wayseer-labs/mqtt
      options:
        url: mqtts://broker.lan
        topics: [zigbee2mqtt/#, homeassistant/#, tele/#]
        username: wayseer
        secret_keyring: mqtt/wayseer
```

| Option | Default | Meaning |
|---|---|---|
| `url` | | The broker: `mqtt://`, `mqtts://`, `ws://` or `wss://`, with a host. A missing port is the scheme's own: 1883, 8883, 80 or 443. It may not hold credentials. |
| `topics` | | The topic filters to subscribe to, such as `zigbee2mqtt/#`, or `'#'` for everything. At most 64. |
| `qos` | `0` | The subscription's QoS, 0 or 1. With 1 and a fixed `client_id`, the broker may keep what was missed while the module was away. |
| `client_id` | random | The client identifier, 1 to 64 letters, digits, `.`, `_` or `-`. |
| `username` | | The username. Its password comes from `secret_keyring`, `secret_file` or `secret_env`. |
| `ca_file` | the system's | PEM roots to trust, for `mqtts://` and `wss://`. |
| `cert_file`, `key_file` | | A PEM client certificate and its key, together. |
| `interval` | `2s` | How often changes and series points are sent, from 1s to 1m. |
| `stale_after` | `10m` | How long a topic that has had live messages may be silent before it shows as stale, from `interval` to 168h. |
| `max_topics` | `5000` | The most topics kept, from 1 to 100000. The least recently heard go first. |
| `zigbee2mqtt_networkmap` | off | How often to ask each zigbee2mqtt bridge for its network map, from 1m to 24h. |

The module connects with TLS 1.2 or later. It retries a broker that doesn't answer after 1s, then
twice as long each time up to a minute, and once connected reconnects by itself, subscribing again
each time. The first snapshot waits for the broker's retained messages: it is sent a quarter of
a second after the broker answers the subscription, and within 3s or an interval regardless. A
wrong password or an untrusted certificate shows as the module's error, never with the
password in it. A broker that refuses some of the filters is a note naming them. One that refuses
them all is an error.

Public test brokers often refuse `'#'`; name narrower filters for them. Go refuses a server
certificate that has no subject alternative names, as `test.mosquitto.org`'s on port 8883 has none,
so read that one over `mqtt://` or `wss://test.mosquitto.org:8081`.

## What it shows

| Kind | Native ID | Is |
|---|---|---|
| `mqtt/broker` | `broker.lan:8883` | The broker. |
| `mqtt/topic` | `zigbee2mqtt/kitchen` | A topic heard, or a level above one. |
| `mqtt/device` | `zigbee2mqtt_0x00124b0000000003` | A device that Home Assistant discovery describes, by its first identifier. |

**The tree.** The broker is the `parent_of` each top level, and each level the `parent_of` the
levels below it, so `zigbee2mqtt/kitchen` sits under `zigbee2mqtt`.

**Payloads.** A plain number is read as the topic's `value`, or as the field its last level names
when that is a known measure, so `garden/temperature` reads `temperature`. A JSON object is read
three levels deep as dotted fields, such as `ENERGY.Power`, up to 32 of them. Numbers become
metrics. Text and booleans become attributes, clipped to 120 bytes. Lists are skipped. Each topic
also has its `topic`, `bytes` and whether its last message was `retained`.

**Units.** A field is measured by the last part of its name: `temperature` in °C, `humidity` and
`battery` in %, `pressure` in Pa (from hPa when below 2000), `power` in W, `energy`, `total`,
`today` and `yesterday` in Wh (from kWh), `voltage` in V (from mV when above 1000), `current` in A,
`illuminance` and `lux` in lx, `co2` in ppm, `pm25` and the like in µg/m³, `rssi` in dBm and
`frequency` in Hz. A unit a discovery config declares wins over the name, so an ESPHome sensor in
°F is charted as a temperature in °C.

**Status of a topic.** A topic is `down` when its device's availability topic says it is offline.
That is a last level of `availability`, `state`, `status`, `LWT`, `online` or `connected` that says
online or offline, or true or false under the last two. A topic that has had live messages is
`warn` once it is silent for longer than `stale_after` and three of its usual gaps. A topic only
ever heard retained is never stale. The broker is `down` while not connected.

**Home Assistant discovery.** With `homeassistant/#` among the topics, each discovery config's
device becomes an `mqtt/device` with its `model`, `manufacturer`, `sw_version`, `hw_version` and
`area`. Device configs with components and abbreviated keys are read too. A device `owns` the
state topics its configs name, and `talks_to` the device it says it is reached through, such as
a zigbee2mqtt bridge. It is `down` when one of its availability topics says it is not available. A
value template that picks a field, such as `{{ value_json.temperature }}`, reads that field. Other
templates are left unread.

**zigbee2mqtt.** From each bridge's retained `bridge/devices`, a device's topic gains its
`ieee_address`, `role`, `power_source`, `model` and `vendor`. Bridge topics are not read as
readings. With `zigbee2mqtt_networkmap` set, the module asks each bridge for a raw network map
that often. Each link becomes a `talks_to` edge toward the coordinator, carrying its best
`linkquality`. A failed map is ignored and the last mesh kept.

That request is all the module ever publishes. zigbee2mqtt scans every router to answer it, so ask
rarely. If the broker drops the connection right after a request, as HiveMQ does when the
credentials may not publish there, the module stops asking that bridge and says so in a note.

**Series**, one point per interval, kept for 256 points:

| Metric | Unit | Kinds |
|---|---|---|
| `messages` | per second | broker |
| each numeric field, such as `temperature` | its unit, above | topic |

Fields of a known unit show with every topic. Others are offered by name. The catalogue grows as
new fields are heard, to at most 200 metrics and 4096 series.

**Events:**

| Kind | Severity | When |
|---|---|---|
| `offline` | warn | A device's availability topic says it went offline. |
| `online` | info | It came back. |

**Bounds.** Between sends the working set may reach twice `max_topics`, and then new topics are
dropped and counted in the broker's `dropped`. Each send trims it back to `max_topics`, the least
recently heard first, with discovery configs last. A discovery config is read for at most 64
components and 8 availability topics, and a network map for at most 10000 links.

## Working on it

```
make check   # what CI runs: tests with the conformance suite, vet and lint for every platform, a key scan
make help    # every target
```

Tests never reach the network unless you ask. They run a broker in the test's process with
[mochi-mqtt](https://github.com/mochi-mqtt/server), over TCP, TLS and WebSocket. The fixtures in
`testdata/` are made up, following what zigbee2mqtt, ESPHome, ebusd and Tasmota send.
`testdata/house.txt` and the others are what they build, and `testdata/broker/retained.txt` is
the broker the conformance suite reads. Run the tests with `UPDATE_SNAPSHOTS=1`
to write them again, then read the diff.

To read a real broker for five seconds and print what the module made of it:

```
MQTT_TEST_URL=mqtt://test.mosquitto.org go test -run TestAgainstARealBroker -v
```

`MQTT_TEST_TOPICS` narrows it from `'#'`, and `MQTT_TEST_OPTIONS` adds options, such as
`ca_file: ca.pem`. The test only subscribes.

The module imports the SDK (`wayseer.dev/sdk`), the standard library and the Eclipse Paho MQTT
client. `TestImportsOnlyTheSDK` keeps it free of the app. golangci-lint is pinned in
`tools/go.mod`, and gitleaks runs at a pinned version through `go run`.
`TestMakeSignPackagesTheModule` signs a package with throwaway keys when Wayseer's source is in
`../../core` or `../core`, and skips otherwise.

To sign a package for yourself, see Wayseer's guide, "Writing a module": `make sign` with
`WAYSEER_DEV_KEY` and `WAYSEER_DEV_CERT` set.

For a marketplace submission, the sandbox serves `testdata/broker` as an MQTT broker holding the
messages in its `retained.txt`, as `TestConformance` does here:

```yaml
conformance:
  options: |
    url: mqtt://127.0.0.1:1883
    topics: ['#']
    interval: 1s
  failing: |
    url: mqtt://127.0.0.1:1
    topics: ['#']
  fixture: testdata/broker
  server: mqtt
```

## Licence

MIT; see `LICENSE`. The program links the [Eclipse Paho MQTT Go client](https://github.com/eclipse/paho.mqtt.golang),
used under the Eclipse Distribution License 1.0 (BSD-3-Clause).
