package mqtt

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"wayseer.dev/sdk"
	"wayseer.dev/sdk/sdktest"
)

var t0 = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)

func newTestState(t *testing.T, yaml string) *state {
	t.Helper()
	o, err := parsed(t, "url: mqtt://broker.lan:1883\ntopics: ['#']\n"+yaml)
	if err != nil {
		t.Fatal(err)
	}
	return newState("home", &o)
}

// house is a zigbee2mqtt network, a Tasmota plug and a plain sensor, as their retained
// messages arrive on connecting and a few live ones after.
func house(s *state) {
	s.receive("zigbee2mqtt/bridge/state", []byte(`{"state":"online"}`), true, t0)
	s.receive("zigbee2mqtt/kitchen", []byte(`{"temperature":21.5,"humidity":40,"battery":90,"linkquality":120}`), true, t0)
	s.receive("zigbee2mqtt/kitchen/availability", []byte(`{"state":"online"}`), true, t0)
	s.receive("zigbee2mqtt/hall_light", []byte(`{"state":"ON","brightness":200}`), true, t0)
	s.receive("tele/plug/LWT", []byte("Online"), true, t0)
	s.receive("tele/plug/SENSOR", []byte(`{"ENERGY":{"Total":12.5,"Power":95,"Voltage":231}}`), false, t0.Add(time.Second))
	s.receive("garden/temperature", []byte("14.2"), false, t0.Add(time.Second))
}

func render(s *state, now time.Time) string {
	ents, edges := s.world(now)
	var b strings.Builder
	for _, ref := range slices.Sorted(maps.Keys(ents)) {
		e := ents[ref]
		attrs := make([]string, 0, len(e.Attrs))
		for k, v := range e.Attrs {
			attrs = append(attrs, k+"="+v.String())
		}
		slices.Sort(attrs)
		fmt.Fprintf(&b, "%s %q %s %q %s\n", ref, e.Name, e.Status.Level, e.Status.Reason, strings.Join(attrs, " "))
	}
	var lines []string
	for k := range edges {
		lines = append(lines, fmt.Sprintf("%s -%s-> %s", k.From, k.Rel, k.To))
	}
	slices.Sort(lines)
	b.WriteString(strings.Join(lines, "\n") + "\n")
	return b.String()
}

func TestTopicsMakeATreeUnderTheBroker(t *testing.T) {
	s := newTestState(t, "")
	house(s)
	sdktest.Golden(t, "testdata/house.txt", render(s, t0.Add(2*time.Second)))
}

func TestAnAvailabilityTopicSetsItsDevicesStatusAndRaisesAnEvent(t *testing.T) {
	s := newTestState(t, "")
	house(s)
	s.flush(t0.Add(2 * time.Second))
	s.receive("zigbee2mqtt/kitchen/availability", []byte("offline"), true, t0.Add(3*time.Second))
	s.receive("tele/plug/LWT", []byte("Offline"), true, t0.Add(3*time.Second))
	evs := s.flush(t0.Add(4 * time.Second))
	ents, _ := s.world(t0.Add(4 * time.Second))
	for _, native := range []string{"zigbee2mqtt/kitchen", "tele/plug"} {
		e := ents[s.ref(kindTopic, native)]
		if e.Status.Level != sdk.StatusDown || !strings.Contains(e.Status.Reason, "offline") {
			t.Errorf("%s: %s %q", native, e.Status.Level, e.Status.Reason)
		}
	}
	if len(evs) != 2 || evs[0].Kind != "offline" || evs[0].Severity != sdk.SevWarn {
		t.Errorf("events %+v", evs)
	}
	if _, ok := ents[s.ref(kindTopic, "zigbee2mqtt/kitchen/availability")]; ok {
		t.Error("an availability topic is its device's status, not an entity")
	}
}

func TestAStateThatIsNotAvailabilityIsAReading(t *testing.T) {
	s := newTestState(t, "")
	s.receive("home/light/state", []byte("ON"), true, t0)
	ents, _ := s.world(t0)
	if e, ok := ents[s.ref(kindTopic, "home/light/state")]; !ok || e.Attrs["payload"] != sdk.String("ON") {
		t.Errorf("home/light/state: %v %+v", ok, e)
	}
}

func TestARetainedEmptyMessageRemovesTheTopic(t *testing.T) {
	s := newTestState(t, "")
	house(s)
	s.receive("garden/temperature", nil, true, t0.Add(3*time.Second))
	ents, _ := s.world(t0.Add(3 * time.Second))
	for _, native := range []string{"garden/temperature", "garden"} {
		if _, ok := ents[s.ref(kindTopic, native)]; ok {
			t.Errorf("%s is still there", native)
		}
	}
}

func TestATopicThatGoesQuietIsStaleAfterThriceItsGapOrStaleAfter(t *testing.T) {
	s := newTestState(t, "stale_after: 1m")
	for i := range 3 {
		s.receive("meter/power", []byte("100"), false, t0.Add(time.Duration(i)*10*time.Minute))
	}
	last := t0.Add(20 * time.Minute)
	status := func(now time.Time) sdk.Status {
		ents, _ := s.world(now)
		return ents[s.ref(kindTopic, "meter/power")].Status
	}
	if st := status(last.Add(29 * time.Minute)); st.Level != sdk.StatusOK {
		t.Errorf("within three gaps: %s %q", st.Level, st.Reason)
	}
	if st := status(last.Add(31 * time.Minute)); st.Level != sdk.StatusWarn || !strings.Contains(st.Reason, "31m") {
		t.Errorf("past three gaps: %s %q", st.Level, st.Reason)
	}
	s.receive("retained/only", []byte("1"), true, t0)
	ents, _ := s.world(t0.Add(48 * time.Hour))
	if st := ents[s.ref(kindTopic, "retained/only")].Status; st.Level != sdk.StatusOK {
		t.Errorf("a topic only ever retained goes stale: %s %q", st.Level, st.Reason)
	}
}

func TestTheLeastRecentlyHeardTopicsGoFirstPastMaxTopics(t *testing.T) {
	s := newTestState(t, "max_topics: 4")
	for i := range 6 {
		s.receive(fmt.Sprintf("s/%d", i), []byte("1"), false, t0.Add(time.Duration(i)*time.Second))
	}
	s.flush(t0.Add(10 * time.Second))
	ents, _ := s.world(t0.Add(10 * time.Second))
	var got []string
	for _, e := range ents {
		if e.Kind == kindTopic {
			got = append(got, e.Name)
		}
	}
	slices.Sort(got)
	if want := []string{"3", "4", "5", "s"}; !slices.Equal(got, want) {
		t.Errorf("kept %q, want %q", got, want)
	}
	if s.dropped != 3 {
		t.Errorf("dropped %d, want 3", s.dropped)
	}
}

func TestTheCatalogueGrowsWithNewFields(t *testing.T) {
	s := newTestState(t, "")
	gen := s.cat.gen
	house(s)
	names := map[string]sdk.Metric{}
	for _, m := range s.cat.metrics {
		names[m.Name] = m
	}
	for name, unit := range map[string]sdk.Unit{
		"temperature": sdk.UnitCelsius, "humidity": sdk.UnitPercent, "ENERGY.Power": sdk.UnitWatts,
		"ENERGY.Total": sdk.UnitWattHours, "brightness": sdk.UnitNone, metricMessages: sdk.UnitPerSec,
	} {
		if m, ok := names[name]; !ok || m.Unit != unit {
			t.Errorf("%s: %v %+v", name, ok, m)
		}
	}
	if names["temperature"].Extra || !names["brightness"].Extra {
		t.Error("known fields should show with their topic, others be chosen by name")
	}
	if s.cat.gen == gen {
		t.Error("the catalogue grew without a new generation")
	}
	gen = s.cat.gen
	house(s)
	if s.cat.gen != gen {
		t.Error("the same fields again made a new generation")
	}
}

func TestReadingsBecomeSeriesAtEachFlush(t *testing.T) {
	s := newTestState(t, "")
	house(s)
	s.flush(t0.Add(2 * time.Second))
	s.receive("garden/temperature", []byte("14.6"), false, t0.Add(3*time.Second))
	s.flush(t0.Add(4 * time.Second))
	r := s.series[sdk.SeriesRef{Entity: s.ref(kindTopic, "garden/temperature"), Metric: "temperature"}]
	if r == nil {
		t.Fatal("no series for garden/temperature")
	}
	pts := r.In(sdk.TimeWindow{From: t0, To: t0.Add(time.Minute)})
	if len(pts) != 2 || pts[0].V != 14.2 || pts[1].V != 14.6 {
		t.Errorf("points %+v", pts)
	}
	rate := s.series[sdk.SeriesRef{Entity: s.brokerRef(), Metric: metricMessages}]
	if rate == nil || rate.Len() != 2 {
		t.Errorf("message rate %+v", rate)
	}
}
