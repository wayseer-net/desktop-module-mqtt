// Package mqtt shows what an MQTT broker carries in Wayseer: its topic tree as entities, numeric
// payloads as series, and devices that go quiet or announce they are offline as status.
//
// It subscribes only to the topic filters the owner names, never to a bare "#" on its own, and
// never publishes. It is a lens: it keeps only a bounded, ageing working set in memory. See
// README.md for options and the data it sends.
package mqtt
