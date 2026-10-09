// Package mqtt shows what an MQTT broker carries in Wayseer: its topic tree as entities, numeric
// payloads as series, devices that go quiet or announce they are offline as status, and
// zigbee2mqtt's devices and mesh.
//
// It subscribes only to the topic filters the owner names, and publishes only zigbee2mqtt network
// map requests, when zigbee2mqtt_networkmap is set. It is a lens: it keeps only a bounded, ageing
// working set in memory. See README.md for options and the data it sends.
package mqtt
