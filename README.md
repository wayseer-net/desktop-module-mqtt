# Wayseer MQTT module

A module for [Wayseer Desktop](https://wayseer.app) that reads an MQTT broker: its topics as a
tree, the numbers in their payloads as metrics with units, and whether devices say they are
online, with zigbee2mqtt's devices and mesh. It subscribes to the topic filters you name and
publishes nothing, unless you set `zigbee2mqtt_networkmap`: then it asks each zigbee2mqtt bridge
for its network map that often, and that request is all it ever publishes. If the broker drops the
connection for it, as HiveMQ does when the credentials may not publish there, it stops asking
and says so on the module's health.

With `homeassistant/#` among the topics it reads Home Assistant MQTT discovery, as zigbee2mqtt,
ESPHome, Zwave JS UI and many others send it: each device becomes an entity, with its model and
maker, that owns its state topics, talks to the hub it is reached through, and is down when its
availability topic says so. The units the configs declare name and convert the readings, so an
ESPHome sensor in °F is charted as a temperature in °C.

Work in progress: it connects and reads, but is not yet in the marketplace.

```yaml
modules:
  - kind: external
    name: home
    options:
      module: wayseer-labs/mqtt
      options:
        url: mqtts://broker.lan          # mqtt://, mqtts://, ws:// or wss://
        topics: [zigbee2mqtt/#, homeassistant/#, tele/#]  # or '#' for everything, up to max_topics
        username: wayseer
        secret_keyring: mqtt/wayseer     # or secret_file, secret_env
        zigbee2mqtt_networkmap: 15m      # optional; zigbee2mqtt scans every router to answer
```

## Licence

MIT; see `LICENSE`. The program links the [Eclipse Paho MQTT Go client](https://github.com/eclipse/paho.mqtt.golang),
used under the Eclipse Distribution License 1.0 (BSD-3-Clause).
