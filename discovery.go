package mqtt

import (
	"cmp"
	"encoding/json"
	"maps"
	"slices"
	"strings"
)

// Bounds on what one discovery config contributes.
const (
	haPrefix        = "homeassistant"
	maxHAComponents = 64
	maxHAAvail      = 8
)

// haConfig is what one discovery config says: a device and its entities.
type haConfig struct {
	device   haDevice
	entities []haEntity
}

// haDevice is a discovered device; the first identifier, or connection, is its key.
type haDevice struct {
	key, name, model, maker, sw, hw, area, via string
	ids                                        []string
}

// haEntity is one entity of a config: the topic its state comes on, the field its value
// template reads ("" for the whole payload; read is false if the template is beyond us), its
// unit and device class, and the topics that say whether it is available.
type haEntity struct {
	state, field, unit, class string
	read                      bool
	avail                     []haAvail
}

// haAvail is an availability topic, the field its template reads, and what it says when up or down.
type haAvail struct{ topic, field, up, down string }

// haAbbrev are the abbreviations of the keys read, as discovery allows them.
var haAbbrev = map[string]string{
	"stat_t": "state_topic", "unit_of_meas": "unit_of_measurement", "dev_cla": "device_class", "val_tpl": "value_template",
	"avty": "availability", "avty_t": "availability_topic", "avty_tpl": "availability_template",
	"pl_avail": "payload_available", "pl_not_avail": "payload_not_available", "t": "topic",
	"dev": "device", "cmps": "components", "o": "origin", "ids": "identifiers", "cns": "connections",
	"mf": "manufacturer", "mdl": "model", "sw": "sw_version", "hw": "hw_version", "sa": "suggested_area", "via_dev": "via_device",
}

// expand renames m's abbreviated keys to their full names, unless the full name is there too.
func expand(m map[string]any) map[string]any {
	for short, full := range haAbbrev {
		if v, ok := m[short]; ok {
			if _, dup := m[full]; !dup {
				m[full] = v
			}
			delete(m, short)
		}
	}
	return m
}

// discoveryTopic reports whether name is <prefix>/<component>/[<node_id>/]<object_id>/config.
func discoveryTopic(name string) bool {
	parts := strings.Split(name, "/")
	return (len(parts) == 4 || len(parts) == 5) && parts[0] == haPrefix && parts[len(parts)-1] == "config" && !slices.Contains(parts, "")
}

// discovery reads a config on a discovery topic: one entity, or a device's components with
// the options they share.
func discovery(name string, payload []byte) (haConfig, bool) {
	var top map[string]any
	if !discoveryTopic(name) || json.Unmarshal(payload, &top) != nil || top == nil {
		return haConfig{}, false
	}
	expand(top)
	var c haConfig
	if dev, ok := top["device"].(map[string]any); ok {
		c.device = deviceOf(expand(dev))
	}
	cmps, ok := top["components"].(map[string]any)
	if strings.Split(name, "/")[1] != "device" || !ok {
		cmps = map[string]any{"": top}
	}
	for _, id := range slices.Sorted(maps.Keys(cmps))[:min(len(cmps), maxHAComponents)] {
		m, ok := cmps[id].(map[string]any)
		if !ok {
			continue
		}
		merged := maps.Clone(top)
		maps.Copy(merged, expand(maps.Clone(m)))
		c.entities = append(c.entities, entityOf(merged))
	}
	return c, true
}

func deviceOf(m map[string]any) haDevice {
	d := haDevice{
		name: str(m["name"]), model: str(m["model"]), maker: str(m["manufacturer"]), sw: str(m["sw_version"]),
		hw: str(m["hw_version"]), area: str(m["suggested_area"]), via: str(m["via_device"]), ids: strs(m["identifiers"]),
	}
	if len(d.ids) > 0 {
		d.key = d.ids[0]
	} else if cns, _ := m["connections"].([]any); len(cns) > 0 {
		d.key = strings.Join(strs(cns[0]), ":")
	}
	return d
}

func entityOf(m map[string]any) haEntity {
	base := str(m["~"])
	e := haEntity{state: tilde(str(m["state_topic"]), base), unit: str(m["unit_of_measurement"]), class: str(m["device_class"])}
	e.field, e.read = templateField(str(m["value_template"]))
	if list, ok := m["availability"].([]any); ok {
		for _, item := range list[:min(len(list), maxHAAvail)] {
			if a, ok := item.(map[string]any); ok {
				e.addAvail(base, expand(maps.Clone(a)), "value_template")
			}
		}
	} else if str(m["availability_topic"]) != "" {
		e.addAvail(base, map[string]any{
			"topic": m["availability_topic"], "availability_template": m["availability_template"],
			"payload_available": m["payload_available"], "payload_not_available": m["payload_not_available"],
		}, "availability_template")
	}
	return e
}

// addAvail adds the availability topic a names, if its template is one read; payloads default
// to online and offline.
func (e *haEntity) addAvail(base string, a map[string]any, template string) {
	field, ok := templateField(str(a[template]))
	topic := tilde(str(a["topic"]), base)
	if !ok || topic == "" {
		return
	}
	up, down := str(a["payload_available"]), str(a["payload_not_available"])
	e.avail = append(e.avail, haAvail{topic, field, cmp.Or(up, "online"), cmp.Or(down, "offline")})
}

// tilde puts base in place of a topic's leading or trailing "~".
func tilde(topic, base string) string {
	if rest, ok := strings.CutPrefix(topic, "~"); ok {
		return base + rest
	}
	if rest, ok := strings.CutSuffix(topic, "~"); ok {
		return rest + base
	}
	return topic
}

func str(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// strs is a string, or the strings of a list.
func strs(v any) []string {
	if s, ok := v.(string); ok && s != "" {
		return []string{s}
	}
	list, _ := v.([]any)
	var out []string
	for _, e := range list {
		if s := str(e); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// templateField reads a value template that picks a field of the payload, such as
// "{{ value_json.temperature | float }}", as the field; "" is the whole payload. It is false
// for any other template.
func templateField(tpl string) (string, bool) {
	tpl = strings.TrimSpace(tpl)
	if tpl == "" {
		return "", true
	}
	inner, open := strings.CutPrefix(tpl, "{{")
	inner, closed := strings.CutSuffix(inner, "}}")
	if !open || !closed {
		return "", false
	}
	expr, _, _ := strings.Cut(inner, "|")
	expr = strings.TrimSpace(expr)
	if expr == "value" {
		return "", true
	}
	rest, ok := strings.CutPrefix(expr, "value_json")
	var path []string
	for ok && rest != "" {
		var key string
		key, rest, ok = jsonKey(rest)
		path = append(path, key)
	}
	if !ok || len(path) == 0 {
		return "", false
	}
	return strings.Join(path, "."), true
}

// jsonKey reads one step of a value_json path, ".key" or "['key']", and what follows it.
func jsonKey(s string) (key, rest string, ok bool) {
	if s[0] == '.' {
		end := 1
		for end < len(s) && identByte(s[end]) {
			end++
		}
		return s[1:end], s[end:], end > 1
	}
	if len(s) < 4 || s[0] != '[' || s[1] != '\'' && s[1] != '"' {
		return "", "", false
	}
	end := strings.IndexByte(s[2:], s[1])
	if end < 0 || !strings.HasPrefix(s[2+end+1:], "]") {
		return "", "", false
	}
	return s[2 : 2+end], s[2+end+2:], true
}

func identByte(c byte) bool {
	return c == '_' || 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9'
}
