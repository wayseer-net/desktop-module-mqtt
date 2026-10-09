package mqtt

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"wayseer.dev/sdk"
)

// kindDevice is a device that Home Assistant discovery describes.
const kindDevice sdk.Kind = "mqtt/device"

// discovered is what Home Assistant discovery has said: configs by topic, indexed by the state
// and availability topics they name, and the last short payload on each topic that may say
// whether a device is available.
type discovered struct {
	configs map[string]haConfig
	stale   bool // the index needs rebuilding
	states  map[string][]haEntity
	avails  map[string]bool
	said    map[string]string
}

// homeAssistant folds a message on a discovery topic into the configs, reporting whether it
// was one; an empty or unreadable payload forgets the config.
func (s *state) homeAssistant(name string, payload []byte) bool {
	if !discoveryTopic(name) {
		return false
	}
	s.forgetConfig(name)
	if c, ok := discovery(name, payload); ok {
		if s.ha.configs == nil {
			s.ha.configs = map[string]haConfig{}
		}
		s.ha.configs[name], s.ha.stale = c, true
	}
	return true
}

// forgetConfig forgets the config on name, and what its availability topic said.
func (s *state) forgetConfig(name string) {
	if _, ok := s.ha.configs[name]; ok {
		delete(s.ha.configs, name)
		s.ha.stale = true
	}
	delete(s.ha.said, name)
}

// index rebuilds the entities by state topic and the availability topics, if a config changed.
func (s *state) index() {
	if !s.ha.stale {
		return
	}
	s.ha.stale = false
	s.ha.states, s.ha.avails = map[string][]haEntity{}, map[string]bool{}
	for _, c := range s.ha.configs {
		for _, e := range c.entities {
			if e.state != "" {
				s.ha.states[e.state] = append(s.ha.states[e.state], e)
			}
			for _, a := range e.avail {
				s.ha.avails[a.topic] = true
			}
		}
	}
}

// keepAvailability keeps a short payload on a topic a config names for availability, or that
// may come to, up to max_topics of them.
func (s *state) keepAvailability(name string, payload []byte) {
	s.index()
	_, kept := s.ha.said[name]
	level := strings.ToLower(name[strings.LastIndexByte(name, '/')+1:])
	switch {
	case len(payload) > maxText:
	case kept, s.ha.avails[name], len(s.ha.said) < s.opts.MaxTopics && slices.Contains(availLevels, level):
		if s.ha.said == nil {
			s.ha.said = map[string]string{}
		}
		s.ha.said[name] = string(bytes.TrimSpace(payload))
	}
}

// onlyAvailability reports whether a config names topic for availability and nothing else.
func (s *state) onlyAvailability(topic string) bool {
	s.index()
	return s.ha.avails[topic] && len(s.ha.states[topic]) == 0
}

// haUnit is one of discovery's units: the unit it is in Wayseer, the field a reading in it is
// named when its own name says nothing, and how to convert to it.
type haUnit struct {
	unit  sdk.Unit
	field string
	conv  func(float64) float64
}

func times(f float64) func(float64) float64 { return func(v float64) float64 { return v * f } }

var haUnits = map[string]haUnit{
	"°C": {sdk.UnitCelsius, "temperature", nil}, "°F": {sdk.UnitCelsius, "temperature", func(v float64) float64 { return (v - 32) * 5 / 9 }},
	"K": {sdk.UnitCelsius, "temperature", func(v float64) float64 { return v - 273.15 }},
	"%": {sdk.UnitPercent, "", nil},
	"W": {sdk.UnitWatts, "power", nil}, "kW": {sdk.UnitWatts, "power", times(1e3)},
	"Wh": {sdk.UnitWattHours, "energy", nil}, "kWh": {sdk.UnitWattHours, "energy", times(1e3)}, "MWh": {sdk.UnitWattHours, "energy", times(1e6)},
	"V": {sdk.UnitVolts, "voltage", nil}, "mV": {sdk.UnitVolts, "voltage", times(1e-3)},
	"A": {sdk.UnitAmperes, "current", nil}, "mA": {sdk.UnitAmperes, "current", times(1e-3)},
	"Pa": {sdk.UnitPascals, "pressure", nil}, "hPa": {sdk.UnitPascals, "pressure", times(100)}, "kPa": {sdk.UnitPascals, "pressure", times(1e3)},
	"mbar": {sdk.UnitPascals, "pressure", times(100)}, "bar": {sdk.UnitPascals, "pressure", times(1e5)},
	"lx": {sdk.UnitLux, "illuminance", nil}, "ppm": {sdk.UnitPPM, "", nil},
	"µg/m³": {sdk.UnitMicrogramsPerM3, "", nil}, "μg/m³": {sdk.UnitMicrogramsPerM3, "", nil},
	"dBm": {sdk.UnitDBm, "rssi", nil}, "Hz": {sdk.UnitHertz, "frequency", nil}, "kHz": {sdk.UnitHertz, "frequency", times(1e3)},
}

// converted are a topic's raw numbers each in its unit: by the unit a config declares for it
// when there is one, else by its name. A topic only for availability has none.
func (s *state) converted(name string, r readings) []reading {
	if s.onlyAvailability(name) {
		return nil
	}
	out := make([]reading, len(r.nums))
	for i, n := range r.nums {
		if d, ok := s.declared(name, n, r.plain); ok {
			out[i] = d
			continue
		}
		out[i] = reading{n.field, scaled(n.field, n.v)}
	}
	return out
}

// declared is raw reading n of topic name in the unit a config declares for it, named by the
// field if that names the unit, else by the device class, else by the unit.
func (s *state) declared(name string, n reading, plain bool) (reading, bool) {
	for _, e := range s.ha.states[name] {
		hu, ok := haUnits[e.unit]
		if !e.read || !ok || e.field != n.field && (e.field != "" || !plain) {
			continue
		}
		v := n.v
		if hu.conv != nil {
			v = hu.conv(v)
		}
		switch {
		case unitOf(n.field) == hu.unit:
			return reading{n.field, v}, true
		case e.class != "" && unitOf(e.class) == hu.unit:
			return reading{e.class, v}, true
		case unitFor(n.field) == nil && hu.field != "":
			return reading{hu.field, v}, true
		}
	}
	return n, false
}

// found is a discovered device as its configs together say it: the first config, by topic,
// to give a detail gives it.
type found struct {
	haDevice
	states map[string]bool
	avail  []haAvail
}

// devices are the discovered devices by key.
func (s *state) devices() map[string]*found {
	out := map[string]*found{}
	for _, topic := range slices.Sorted(maps.Keys(s.ha.configs)) {
		c := s.ha.configs[topic]
		if c.device.key == "" {
			continue
		}
		d := out[c.device.key]
		if d == nil {
			d = &found{haDevice: c.device, states: map[string]bool{}}
			out[c.device.key] = d
		}
		d.merge(c.device)
		for _, e := range c.entities {
			if e.state != "" {
				d.states[e.state] = true
			}
			for _, a := range e.avail {
				if !slices.Contains(d.avail, a) {
					d.avail = append(d.avail, a)
				}
			}
		}
	}
	return out
}

func (d *found) merge(o haDevice) {
	for _, f := range []struct {
		to   *string
		from string
	}{
		{&d.name, o.name}, {&d.model, o.model}, {&d.maker, o.maker}, {&d.sw, o.sw}, {&d.hw, o.hw}, {&d.area, o.area}, {&d.via, o.via},
	} {
		if *f.to == "" {
			*f.to = f.from
		}
	}
	for _, id := range o.ids {
		if !slices.Contains(d.ids, id) {
			d.ids = append(d.ids, id)
		}
	}
}

// deviceWorld adds each discovered device, owning the state topics it names, and talking to
// the device it says it is reached through.
func (s *state) deviceWorld(ents map[sdk.EntityRef]sdk.Entity, edges map[sdk.EdgeKey]sdk.Edge) {
	devs := s.devices()
	byID := map[string]string{}
	for key, d := range devs {
		for _, id := range d.ids {
			byID[id] = key
		}
	}
	for key, d := range devs {
		ref := s.ref(kindDevice, key)
		ents[ref] = sdk.Entity{Ref: ref, Kind: kindDevice, Name: cmp.Or(d.name, key), Status: s.availability(d), Attrs: d.attrs(), Source: s.src}
		for topic := range d.states {
			if s.topics[topic] != nil {
				link(edges, ref, s.ref(kindTopic, topic), sdk.RelOwns, s.src)
			}
		}
		if via, ok := byID[d.via]; ok && via != key {
			link(edges, ref, s.ref(kindDevice, via), sdk.RelTalksTo, s.src)
		}
	}
}

func link(edges map[sdk.EdgeKey]sdk.Edge, from, to sdk.EntityRef, rel sdk.Relation, src sdk.ModuleID) {
	edges[sdk.EdgeKey{From: from, To: to, Rel: rel}] = sdk.Edge{From: from, To: to, Rel: rel, Weight: 1, Source: src}
}

func (d *found) attrs() map[string]sdk.Value {
	a := map[string]sdk.Value{}
	for k, v := range map[string]string{"model": d.model, "manufacturer": d.maker, "sw_version": d.sw, "hw_version": d.hw, "area": d.area} {
		if v != "" {
			a[k] = sdk.String(v)
		}
	}
	return a
}

// availability is down when one of the device's availability topics last said it was not available.
func (s *state) availability(d *found) sdk.Status {
	for _, a := range slices.SortedFunc(slices.Values(d.avail), func(x, y haAvail) int { return strings.Compare(x.topic, y.topic) }) {
		if said := s.ha.said[a.topic]; said != "" && field(said, a.field) == a.down {
			return sdk.Status{Level: sdk.StatusDown, Reason: "offline, says " + a.topic}
		}
	}
	return sdk.Status{Level: sdk.StatusOK}
}

// field is the payload's field at a dotted path, as text; "" is the whole payload.
func field(payload, path string) string {
	if path == "" {
		return payload
	}
	var v any
	d := json.NewDecoder(strings.NewReader(payload))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return ""
	}
	for _, key := range strings.Split(path, ".") {
		m, _ := v.(map[string]any)
		v = m[key]
	}
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
