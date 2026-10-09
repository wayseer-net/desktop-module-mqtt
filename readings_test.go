package mqtt

import (
	"strings"
	"testing"

	"wayseer.dev/sdk"
)

// readingsOf reads a payload as a topic with no discovery config does.
func readingsOf(level string, payload []byte) readings {
	r := rawReadingsOf(level, payload)
	r.nums = new(state).converted(level, r)
	return r
}

func TestAPlainNumberIsTheTopicsValue(t *testing.T) {
	r := readingsOf("abc123", []byte(" 21.5\n"))
	if len(r.nums) != 1 || r.nums[0] != (reading{field: fieldValue, v: 21.5}) || len(r.attrs) != 0 {
		t.Errorf("readings %+v", r)
	}
}

func TestAPlainNumberOnAKnownLevelIsThatField(t *testing.T) {
	r := readingsOf("pressure", []byte("1013"))
	if len(r.nums) != 1 || r.nums[0] != (reading{field: "pressure", v: 101300}) {
		t.Errorf("readings %+v", r)
	}
}

func TestJSONFieldsAreFlattenedAndScaledToTheirUnits(t *testing.T) {
	r := readingsOf("x", []byte(`{"temperature":21.46,"humidity":40,"pressure":1013.2,"energy":1.5,"voltage":3005,
		"linkquality":120,"state":"ON","child_lock":false,"update":{"state":"idle","progress":12},"list":[1,2]}`))
	nums := map[string]float64{}
	for _, n := range r.nums {
		nums[n.field] = n.v
	}
	want := map[string]float64{
		"temperature": 21.46, "humidity": 40, "pressure": 101320, "energy": 1500, "voltage": 3.005,
		"linkquality": 120, "update.progress": 12,
	}
	for f, v := range want {
		if got, ok := nums[f]; !ok || got-v > 1e-9 || v-got > 1e-9 {
			t.Errorf("%s = %v (%v), want %v", f, got, ok, v)
		}
	}
	if len(nums) != len(want) {
		t.Errorf("numbers %v", nums)
	}
	if r.attrs["state"] != sdk.String("ON") || r.attrs["child_lock"] != sdk.Bool(false) || r.attrs["update.state"] != sdk.String("idle") {
		t.Errorf("attrs %v", r.attrs)
	}
}

func TestOtherPayloadsAreKeptAsText(t *testing.T) {
	if r := readingsOf("x", []byte("ON")); r.attrs["payload"] != sdk.String("ON") || len(r.nums) != 0 {
		t.Errorf("text: %+v", r)
	}
	long := readingsOf("x", []byte(strings.Repeat("x", 500)))
	if s := long.attrs["payload"].Str(); len(s) > maxText+len("…") {
		t.Errorf("long text kept whole: %d bytes", len(s))
	}
	if r := readingsOf("x", []byte{0xff, 0x00, 0x01}); r.attrs["payload_bytes"] != sdk.Number(3).In(sdk.UnitBytes) {
		t.Errorf("binary: %+v", r)
	}
}

func TestReadingsAreBounded(t *testing.T) {
	var b strings.Builder
	b.WriteString("{")
	for i := range 3 * maxReadings {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`"f` + strings.Repeat("a", i%7) + string(rune('a'+i%26)) + `":` + "1")
	}
	b.WriteString("}")
	r := readingsOf("x", []byte(b.String()))
	if len(r.nums)+len(r.attrs) > maxReadings {
		t.Errorf("%d readings, at most %d", len(r.nums)+len(r.attrs), maxReadings)
	}
}

func TestKnownFieldsHaveUnits(t *testing.T) {
	for field, want := range map[string]sdk.Unit{
		"temperature": sdk.UnitCelsius, "device_temperature": sdk.UnitCelsius, "humidity": sdk.UnitPercent,
		"power": sdk.UnitWatts, "energy": sdk.UnitWattHours, "illuminance_lux": sdk.UnitLux, "co2": sdk.UnitPPM,
		"pm25": sdk.UnitMicrogramsPerM3, "battery": sdk.UnitPercent, "current": sdk.UnitAmperes,
		"update.progress": sdk.UnitNone, fieldValue: sdk.UnitNone,
	} {
		if got := unitOf(field); got != want {
			t.Errorf("unitOf(%q) = %q, want %q", field, got, want)
		}
	}
}
