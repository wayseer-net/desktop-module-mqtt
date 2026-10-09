package mqtt

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"wayseer.dev/sdk"
)

// Bounds on what one payload contributes.
const (
	maxReadings = 32  // fields read from one payload
	maxDepth    = 3   // levels of nested JSON objects flattened
	maxText     = 120 // bytes of text kept
)

// fieldValue names the reading of a payload that is a plain number.
const fieldValue = "value"

// reading is one number in a payload, in its field's unit.
type reading struct {
	field string
	v     float64
}

// readings is what a payload says: numbers, and anything else as attributes.
type readings struct {
	nums  []reading
	attrs map[string]sdk.Value
}

// readingsOf reads a payload on a topic whose last level is level: a plain number, a JSON
// object flattened to dotted fields, or text. A plain number is the level's field when its
// unit is known, such as a topic ending /temperature, and otherwise the topic's value.
func readingsOf(level string, payload []byte) readings {
	r := readings{attrs: map[string]sdk.Value{}}
	p := bytes.TrimSpace(payload)
	if v, err := strconv.ParseFloat(string(p), 64); err == nil {
		field := fieldValue
		if unitFor(level) != nil {
			field = strings.ToLower(level)
		}
		r.nums = append(r.nums, reading{field, scaled(field, v)})
		return r
	}
	if len(p) > 0 && p[0] == '{' && json.Valid(p) {
		d := json.NewDecoder(bytes.NewReader(p))
		d.UseNumber()
		_, _ = d.Token() // the opening brace
		r.object(d, "", 1)
		return r
	}
	if utf8.Valid(p) && !bytes.ContainsFunc(p, func(c rune) bool { return unicode.IsControl(c) && c != '\t' }) {
		r.attrs["payload"] = sdk.String(clip(string(p)))
		return r
	}
	r.attrs["payload_bytes"] = sdk.Number(float64(len(payload))).In(sdk.UnitBytes)
	return r
}

// object reads the fields of a JSON object whose opening brace d has read, up to its close.
func (r *readings) object(d *json.Decoder, prefix string, depth int) {
	for d.More() {
		t, _ := d.Token()
		key, _ := t.(string)
		r.value(d, prefix+key, depth)
	}
	_, _ = d.Token() // the closing brace
}

// value reads one field's value: a number, a bool or text kept, an object flattened, a list skipped.
func (r *readings) value(d *json.Decoder, field string, depth int) {
	t, _ := d.Token()
	full := len(r.nums)+len(r.attrs) >= maxReadings
	switch v := t.(type) {
	case json.Number:
		if f, err := v.Float64(); err == nil && !full {
			r.nums = append(r.nums, reading{field, scaled(field, f)})
		}
	case string:
		if !full {
			r.attrs[field] = sdk.String(clip(v))
		}
	case bool:
		if !full {
			r.attrs[field] = sdk.Bool(v)
		}
	case json.Delim:
		if v == '{' && depth < maxDepth {
			r.object(d, field+".", depth+1)
			return
		}
		skip(d)
	}
}

// skip reads past the rest of an object or list whose opening d has read.
func skip(d *json.Decoder) {
	for n := 1; n > 0; {
		t, err := d.Token()
		if err != nil {
			return
		}
		if delim, ok := t.(json.Delim); ok {
			if delim == '{' || delim == '[' {
				n++
			} else {
				n--
			}
		}
	}
}

func clip(s string) string {
	if len(s) <= maxText {
		return s
	}
	cut := maxText
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}

// fieldUnit is how a field named like this is measured, and how a sensor's number becomes
// the unit's: zigbee2mqtt and Home Assistant send pressure in hPa, energy in kWh, and a
// battery's voltage in mV.
type fieldUnit struct {
	names []string
	unit  sdk.Unit
	scale func(float64) float64
}

var fieldUnits = []fieldUnit{
	{[]string{"temperature", "temp"}, sdk.UnitCelsius, nil},
	{[]string{"humidity"}, sdk.UnitPercent, nil},
	{[]string{"battery"}, sdk.UnitPercent, nil},
	{[]string{"pressure"}, sdk.UnitPascals, func(v float64) float64 {
		if v < 2000 {
			return v * 100 // hPa
		}
		return v
	}},
	{[]string{"power"}, sdk.UnitWatts, nil},
	{[]string{"energy", "total", "today", "yesterday"}, sdk.UnitWattHours, func(v float64) float64 { return v * 1000 }},
	{[]string{"voltage"}, sdk.UnitVolts, func(v float64) float64 {
		if v > 1000 {
			return v / 1000 // mV
		}
		return v
	}},
	{[]string{"current"}, sdk.UnitAmperes, nil},
	{[]string{"illuminance", "illuminance_lux", "lux"}, sdk.UnitLux, nil},
	{[]string{"co2", "eco2"}, sdk.UnitPPM, nil},
	{[]string{"pm1", "pm10", "pm25", "pm2_5"}, sdk.UnitMicrogramsPerM3, nil},
	{[]string{"rssi", "signal"}, sdk.UnitDBm, nil},
	{[]string{"frequency"}, sdk.UnitHertz, nil},
}

// unitFor finds the unit of a field by its last part, lower-cased: "device_temperature" and
// "ENERGY.Total" count; a counter's total, today or yesterday only under "energy".
func unitFor(field string) *fieldUnit {
	f := strings.ToLower(field)
	last := f[strings.LastIndexByte(f, '.')+1:]
	for i := range fieldUnits {
		u := &fieldUnits[i]
		for _, n := range u.names {
			energyPart := n == "total" || n == "today" || n == "yesterday"
			switch {
			case energyPart && last == n && strings.Contains(f, "energy."):
				return u
			case !energyPart && (last == n || strings.HasSuffix(last, "_"+n)):
				return u
			}
		}
	}
	return nil
}

// unitOf is a field's unit; none for a field it does not know.
func unitOf(field string) sdk.Unit {
	if u := unitFor(field); u != nil {
		return u.unit
	}
	return sdk.UnitNone
}

// scaled is v in its field's unit.
func scaled(field string, v float64) float64 {
	if u := unitFor(field); u != nil && u.scale != nil {
		return u.scale(v)
	}
	return v
}
