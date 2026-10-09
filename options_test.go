package mqtt

import (
	"strings"
	"testing"
	"time"

	"wayseer.dev/sdk/sdktest"
)

// parsed decodes and checks options as Configure does.
func parsed(t *testing.T, yaml string) (options, error) {
	t.Helper()
	cfg, err := sdktest.Config("mqtt", yaml)
	if err != nil {
		t.Fatal(err)
	}
	o := defaults()
	if err := cfg.Decode(&o); err != nil {
		return o, err
	}
	return o, o.validate()
}

func TestDefaultsSubscribeAtQoS0(t *testing.T) {
	o, err := parsed(t, "url: mqtt://broker.lan\ntopics: [zigbee2mqtt/#]")
	if err != nil {
		t.Fatal(err)
	}
	if o.QoS != 0 || o.Interval != 2*time.Second || o.StaleAfter != 10*time.Minute || o.MaxTopics != 5000 {
		t.Errorf("defaults %+v", o)
	}
}

func TestEveryTopicIsAllowed(t *testing.T) {
	if _, err := parsed(t, "url: mqtt://broker.lan\ntopics: ['#', '$SYS/#']"); err != nil {
		t.Error(err)
	}
}

func TestGoodOptions(t *testing.T) {
	for _, yaml := range []string{
		"url: mqtts://broker.lan:8883\ntopics: [home/+/temp, a/+/b/#, plain/topic]\nqos: 1",
		"url: wss://broker.lan/mqtt\ntopics: [x]\nca_file: ca.pem\ncert_file: c.pem\nkey_file: k.pem",
		"url: ws://broker.lan:9001\ntopics: [x]\nclient_id: wayseer-desk_1.a",
		"url: mqtt://broker.lan\ntopics: [x]\nusername: me\nsecret_env: MQTT_PASSWORD",
		"url: mqtt://broker.lan\ntopics: [zigbee2mqtt/#]\nzigbee2mqtt_networkmap: 15m",
	} {
		if _, err := parsed(t, yaml); err != nil {
			t.Errorf("%q: %v", yaml, err)
		}
	}
}

func TestBadOptions(t *testing.T) {
	ok := "url: mqtt://broker.lan\n"
	for _, c := range []struct{ yaml, want string }{
		{"topics: [x]", "url is required"},
		{"url: http://broker.lan\ntopics: [x]", "mqtt://"},
		{"url: mqtt://\ntopics: [x]", "mqtt://"},
		{"url: mqtt://me:pw@broker.lan\ntopics: [x]", "credentials"},
		{ok, "topics"},
		{ok + "topics: ['']", "empty"},
		{ok + "topics: [a/#/b]", "'#'"},
		{ok + "topics: [a#]", "'#'"},
		{ok + "topics: [a/b+]", "'+'"},
		{ok + "topics: [+a]", "'+'"},
		{ok + "topics: [\"a\\0b\"]", "NUL"},
		{ok + "topics: [x, x]", "twice"},
		{ok + "topics: [" + strings.Repeat("t,", maxTopicFilters) + "t]", "at most"},
		{ok + "topics: [x]\nqos: 2", "qos"},
		{ok + "topics: [x]\nca_file: ca.pem", "mqtts://"},
		{ok + "topics: [x]\ncert_file: c.pem", "together"},
		{ok + "topics: [x]\nsecret_env: P", "username"},
		{ok + "topics: [x]\nclient_id: 'a b'", "client_id"},
		{ok + "topics: [x]\ninterval: 100ms", "interval"},
		{ok + "topics: [x]\nstale_after: 1s", "stale_after"},
		{ok + "topics: [x]\nmax_topics: 0", "max_topics"},
		{ok + "topics: [x]\nzigbee2mqtt_networkmap: 30s", "zigbee2mqtt_networkmap"},
		{ok + "topics: [x]\nzigbee2mqtt_networkmap: 25h", "zigbee2mqtt_networkmap"},
	} {
		if _, err := parsed(t, c.yaml); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%q: error %v, want one mentioning %q", c.yaml, err, c.want)
		}
	}
}

func TestTheBrokerAddressHasItsSchemesDefaultPort(t *testing.T) {
	for url, want := range map[string]string{
		"mqtt://broker.lan": "broker.lan:1883", "mqtts://broker.lan": "broker.lan:8883",
		"ws://broker.lan/mqtt": "broker.lan:80", "wss://broker.lan/mqtt": "broker.lan:443",
		"mqtt://broker.lan:1884": "broker.lan:1884", "mqtt://[::1]": "[::1]:1883",
	} {
		if got := address(url); got != want {
			t.Errorf("address(%q) = %q, want %q", url, got, want)
		}
	}
}
