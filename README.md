# Wayseer MQTT module

A module for [Wayseer Desktop](https://wayseer.app) that reads an MQTT broker: its topics as a
tree, the numbers in their payloads as metrics with units, and whether devices say they are
online. It subscribes to the topic filters you name and never publishes.

Work in progress: it connects and reads, but is not yet in the marketplace.

```yaml
modules:
  - kind: external
    name: home
    options:
      module: wayseer-labs/mqtt
      options:
        url: mqtts://broker.lan          # mqtt://, mqtts://, ws:// or wss://
        topics: [zigbee2mqtt/#, tele/#]  # or '#' for everything, up to max_topics
        username: wayseer
        secret_keyring: mqtt/wayseer     # or secret_file, secret_env
```

## Licence

MIT; see `LICENSE`. The program links the [Eclipse Paho MQTT Go client](https://github.com/eclipse/paho.mqtt.golang),
used under the Eclipse Distribution License 1.0 (BSD-3-Clause).
