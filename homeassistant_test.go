package mqtt

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"wayseer.dev/sdk"
	"wayseer.dev/sdk/sdktest"
)

// homeAssistant is the house with the discovery configs and state of the fixture, retained.
func homeAssistant(t *testing.T, s *state) {
	t.Helper()
	house(s)
	sc := bufio.NewScanner(bytes.NewReader(fixtureIn(t, "homeassistant", "configs.txt")))
	for sc.Scan() {
		if line := sc.Text(); line != "" && line[0] != '#' {
			topic, payload, _ := strings.Cut(line, " ")
			s.receive(topic, []byte(payload), true, t0)
		}
	}
}

// number is a reading of the topic in its unit, by field.
func number(s *state, topic, field string) (float64, bool) {
	if t := s.topics[topic]; t != nil {
		for _, r := range s.converted(topic, t.read) {
			if r.field == field {
				return r.v, true
			}
		}
	}
	return 0, false
}

func TestDiscoveryMakesDevicesThatOwnTheirStateTopics(t *testing.T) {
	s := newTestState(t, "")
	homeAssistant(t, s)
	sdktest.Golden(t, "testdata/homeassistant/house.txt", render(s, t0.Add(2*time.Second)))
}

func TestADiscoveredUnitNamesAndConvertsTheReading(t *testing.T) {
	s := newTestState(t, "")
	homeAssistant(t, s)
	for _, c := range []struct {
		topic, field string
		want         float64
	}{
		{"garden/sensor/garden_temperature/state", "temperature", 20}, // °F, by its device class
		{"heating/boiler/flow", "temperature", 55.5},                  // a JSON field, by its device class
		{"heating/boiler/power", "power", 1200},                       // kW
		{"heating/boiler/pressure", "pressure", 150000},               // kPa, by its unit alone
		{"zigbee2mqtt/kitchen", "temperature", 21.5},                  // already named so
	} {
		if v, ok := number(s, c.topic, c.field); !ok || math.Abs(v-c.want) > 1e-9 {
			t.Errorf("%s %s = %v (%v), want %v; raw %+v", c.topic, c.field, v, ok, c.want, s.topics[c.topic].read.nums)
		}
	}
}

func TestAKnownFieldTakesTheDiscoveredUnitOverItsGuess(t *testing.T) {
	s := newTestState(t, "")
	s.receive("homeassistant/sensor/x/config", []byte(`{"stat_t":"weather","val_tpl":"{{ value_json.pressure }}","unit_of_meas":"kPa","dev_cla":"pressure"}`), true, t0)
	s.receive("weather", []byte(`{"pressure":101.3}`), false, t0)
	if v, _ := number(s, "weather", "pressure"); math.Abs(v-101300) > 1e-6 {
		t.Errorf("pressure %v Pa, want 101300", v)
	}
}

func TestAnUnknownUnitLeavesTheReadingAsHeard(t *testing.T) {
	s := newTestState(t, "")
	s.receive("homeassistant/sensor/x/config", []byte(`{"stat_t":"tank","unit_of_meas":"L","dev_cla":"volume"}`), true, t0)
	s.receive("tank", []byte(`420`), false, t0)
	if v, ok := number(s, "tank", fieldValue); !ok || v != 420 {
		t.Errorf("readings %+v", s.topics["tank"].read.nums)
	}
}

func TestADiscoveryConfigIsNotReadAsAReading(t *testing.T) {
	s := newTestState(t, "")
	homeAssistant(t, s)
	ents, _ := s.world(t0)
	if e := ents[s.ref(kindTopic, "homeassistant/device/boiler/config")]; len(e.Attrs) != 3 {
		t.Errorf("config has %v", e.Attrs)
	}
}

func TestAnEmptyConfigRemovesTheDevice(t *testing.T) {
	s := newTestState(t, "")
	homeAssistant(t, s)
	s.receive("homeassistant/device/boiler/config", nil, true, t0.Add(time.Minute))
	ents, _ := s.world(t0.Add(time.Minute))
	if _, ok := ents[s.ref(kindDevice, "boiler-1")]; ok {
		t.Error("the boiler stays")
	}
	s.receive("heating/boiler/power", []byte("1.2"), false, t0.Add(time.Minute))
	if v, _ := number(s, "heating/boiler/power", "power"); v == 1200 {
		t.Error("a forgotten config still converts")
	}
}

func TestADeviceIsDownWhenItsAvailabilityPayloadSaysSo(t *testing.T) {
	s := newTestState(t, "")
	homeAssistant(t, s)
	s.receive("heating/boiler/online", []byte("0"), true, t0.Add(time.Minute))
	s.receive("zigbee2mqtt/kitchen/availability", []byte(`{"state":"offline"}`), true, t0.Add(time.Minute))
	ents, _ := s.world(t0.Add(time.Minute))
	for native, says := range map[string]string{"boiler-1": "heating/boiler/online", "zigbee2mqtt_0x00124b0000000003": "zigbee2mqtt/kitchen/availability"} {
		if st := ents[s.ref(kindDevice, native)].Status; st.Level != sdk.StatusDown || !strings.Contains(st.Reason, says) {
			t.Errorf("%s: %+v", native, st)
		}
	}
	if st := ents[s.ref(kindDevice, "a4cf12b3c4d5")].Status; st.Level != sdk.StatusOK {
		t.Errorf("garden: %+v", st)
	}
}

func TestAViaDeviceIsTalkedTo(t *testing.T) {
	s := newTestState(t, "")
	homeAssistant(t, s)
	_, edges := s.world(t0)
	k := sdk.EdgeKey{From: s.ref(kindDevice, "zigbee2mqtt_0x00124b0000000003"), To: s.ref(kindDevice, "zigbee2mqtt_bridge_0x00124b0000000001"), Rel: sdk.RelTalksTo}
	if _, ok := edges[k]; !ok {
		t.Error("the kitchen sensor doesn't talk to the bridge")
	}
}

func TestAbbreviationsReadAsTheFullNames(t *testing.T) {
	short, okS := discovery("homeassistant/sensor/n/o/config", []byte(`{"~":"b","stat_t":"~/s","unit_of_meas":"°C","dev_cla":"temperature","val_tpl":"{{ value_json.t }}",
		"avty":[{"t":"~/a","pl_avail":"up","pl_not_avail":"down"}],"dev":{"ids":"i","name":"n","mdl":"m","mf":"f","sw":"1","hw":"2","sa":"room","via_dev":"hub"}}`))
	full, okF := discovery("homeassistant/sensor/n/o/config", []byte(`{"state_topic":"b/s","unit_of_measurement":"°C","device_class":"temperature","value_template":"{{ value_json.t }}",
		"availability":[{"topic":"b/a","payload_available":"up","payload_not_available":"down"}],
		"device":{"identifiers":["i"],"name":"n","model":"m","manufacturer":"f","sw_version":"1","hw_version":"2","suggested_area":"room","via_device":"hub"}}`))
	if !okS || !okF || !reflect.DeepEqual(short, full) {
		t.Errorf("abbreviated %+v\nfull %+v", short, full)
	}
}

func TestValueTemplatesNameAField(t *testing.T) {
	for tpl, want := range map[string]string{
		"": "", "{{ value }}": "", "{{value|float}}": "", "{{ value_json.temperature }}": "temperature",
		"{{ value_json.ENERGY.Power | round(1) }}": "ENERGY.Power", `{{ value_json['a b'] }}`: "a b", `{{ value_json["x"].y }}`: "x.y",
	} {
		if got, ok := templateField(tpl); !ok || got != want {
			t.Errorf("%q: %q %v, want %q", tpl, got, ok, want)
		}
	}
	for _, tpl := range []string{"{{ value_json.a if value_json.b else 0 }}", "{% if value %}1{% endif %}", "{{ float(value) * 2 }}", "value_json.a }}"} {
		if got, ok := templateField(tpl); ok {
			t.Errorf("%q names %q", tpl, got)
		}
	}
}

func TestOnlyDiscoveryTopicsAreConfigs(t *testing.T) {
	for _, topic := range []string{"homeassistant/config", "homeassistant/sensor/config", "homeassistant/a/b/c/d/config", "other/sensor/x/config", "homeassistant/sensor/x/state"} {
		if _, ok := discovery(topic, []byte(`{"stat_t":"x"}`)); ok {
			t.Errorf("%s read as a config", topic)
		}
	}
}

func TestAnAvailabilityTopicIsNotAReading(t *testing.T) {
	s := newTestState(t, "")
	homeAssistant(t, s)
	ents, _ := s.world(t0)
	if a := ents[s.ref(kindTopic, "heating/boiler/online")].Attrs; len(a) != 3 {
		t.Errorf("attrs %v", a)
	}
}

func TestAConfigAfterItsTopicsStillApplies(t *testing.T) {
	s := newTestState(t, "")
	s.receive("heating/boiler/online", []byte("0"), true, t0)
	s.receive("heating/boiler/power", []byte("1.2"), true, t0)
	s.receive("homeassistant/device/boiler/config", []byte(`{"~":"heating/boiler","dev":{"ids":"boiler-1"},"avty_t":"~/online","pl_avail":"1","pl_not_avail":"0",
		"cmps":{"power":{"p":"sensor","dev_cla":"power","unit_of_meas":"kW","stat_t":"~/power"}}}`), true, t0)
	ents, _ := s.world(t0)
	if st := ents[s.ref(kindDevice, "boiler-1")].Status; st.Level != sdk.StatusDown {
		t.Errorf("boiler %+v", st)
	}
	if p := ents[s.ref(kindTopic, "heating/boiler/power")].Attrs["power"]; p.Num() != 1200 {
		t.Errorf("power %v", p)
	}
	if a := ents[s.ref(kindTopic, "heating/boiler/online")].Attrs; len(a) != 3 {
		t.Errorf("the availability topic reads %v", a)
	}
}

func TestTrimmingKeepsDiscoveryConfigsTillLast(t *testing.T) {
	s := newTestState(t, "max_topics: 8")
	s.receive("homeassistant/sensor/x/config", []byte(`{"stat_t":"x","dev":{"ids":"x"}}`), true, t0)
	for i := range 10 {
		s.receive(fmt.Sprintf("busy/%d", i), []byte("1"), false, t0.Add(time.Duration(i+1)*time.Second))
	}
	s.flush(t0.Add(time.Minute))
	if _, ok := s.ha.configs["homeassistant/sensor/x/config"]; !ok {
		t.Errorf("trimmed the config; kept %v", slices.Sorted(maps.Keys(s.topics)))
	}
}
