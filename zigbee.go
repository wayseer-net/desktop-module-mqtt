package mqtt

import (
	"encoding/json"
	"slices"
	"strings"
	"time"

	"wayseer.dev/sdk"
)

// zigbee2mqtt's bridge topics, under its base topic: the retained device list, and the network
// map it sends when asked for one.
const (
	z2mDevices    = "/bridge/devices"
	z2mNetworkMap = "/bridge/response/networkmap"
	z2mAskForMap  = "/bridge/request/networkmap"
	z2mMapRequest = `{"type":"raw","routes":false}`
	refusalWindow = 3 * time.Second // a disconnect this soon after asking for a map is taken as a refusal
	maxMeshLinks  = 10000
)

// device is what zigbee2mqtt's bridge says of one device.
type device struct {
	IEEE  string `json:"ieee_address"`
	Role  string `json:"type"` // Coordinator, Router or EndDevice
	Name  string `json:"friendly_name"`
	Power string `json:"power_source"`
	Def   *struct {
		Model  string `json:"model"`
		Vendor string `json:"vendor"`
	} `json:"definition"`
}

// mesh is what one zigbee2mqtt bridge has said: its devices by topic, and the best link
// quality between each pair of devices, by IEEE address, the lower first.
type mesh struct {
	asked   time.Time // when a network map was last asked for
	refused bool      // the broker disconnected us for asking; not asked again
	devices map[string]device
	topicOf map[string]string // by IEEE address
	links   map[[2]string]float64
}

// zigbee2mqtt folds a message on one of a bridge's topics into its mesh, reporting whether it
// was one; a bridge topic is not read as readings.
func (s *state) zigbee2mqtt(name string, payload []byte) bool {
	switch {
	case strings.HasSuffix(name, z2mDevices):
		s.bridgeDevices(strings.TrimSuffix(name, z2mDevices), payload)
	case strings.HasSuffix(name, z2mNetworkMap):
		s.bridgeNetworkMap(strings.TrimSuffix(name, z2mNetworkMap), payload)
	default:
		return false
	}
	return true
}

// meshOf is base's mesh, made if new.
func (s *state) meshOf(base string) *mesh {
	if s.meshes == nil {
		s.meshes = map[string]*mesh{}
	}
	m := s.meshes[base]
	if m == nil {
		m = &mesh{}
		s.meshes[base] = m
	}
	return m
}

// bridgeDevices replaces base's device list, adding a topic for each device not yet heard.
func (s *state) bridgeDevices(base string, payload []byte) {
	var list []device
	if json.Unmarshal(payload, &list) != nil {
		return
	}
	m := s.meshOf(base)
	m.devices, m.topicOf = map[string]device{}, map[string]string{}
	for _, d := range list[:min(len(list), s.opts.MaxTopics)] {
		if d.Name == "" || d.IEEE == "" {
			continue
		}
		topic := base + "/" + d.Name
		m.devices[topic], m.topicOf[d.IEEE] = d, topic
		s.ensure(topic)
	}
}

// bridgeNetworkMap replaces base's links with those of a raw network map that succeeded.
func (s *state) bridgeNetworkMap(base string, payload []byte) {
	var r struct {
		Status string `json:"status"`
		Data   struct {
			Type  string          `json:"type"`
			Value json.RawMessage `json:"value"`
		} `json:"data"`
	}
	if json.Unmarshal(payload, &r) != nil || r.Status != "ok" || r.Data.Type != "raw" {
		return
	}
	var v struct {
		Links []struct {
			Source, Target struct {
				IEEE string `json:"ieeeAddr"`
			}
			Quality float64 `json:"linkquality"`
		} `json:"links"`
	}
	if json.Unmarshal(r.Data.Value, &v) != nil {
		return
	}
	m := s.meshOf(base)
	m.links = map[[2]string]float64{}
	for _, l := range v.Links[:min(len(v.Links), maxMeshLinks)] {
		a, b := l.Source.IEEE, l.Target.IEEE
		if a == b || a == "" || b == "" {
			continue
		}
		if b < a {
			a, b = b, a
		}
		m.links[[2]string{a, b}] = max(m.links[[2]string{a, b}], l.Quality)
	}
}

// deviceAttrs adds what the bridge says of the device on topic, if it is one.
func (s *state) deviceAttrs(topic string, a map[string]sdk.Value) {
	for _, m := range s.meshes {
		d, ok := m.devices[topic]
		if !ok {
			continue
		}
		a["ieee_address"], a["role"] = sdk.String(d.IEEE), sdk.String(d.Role)
		if d.Power != "" {
			a["power_source"] = sdk.String(d.Power)
		}
		if d.Def != nil {
			a["model"], a["vendor"] = sdk.String(d.Def.Model), sdk.String(d.Def.Vendor)
		}
		return
	}
}

// roleRank orders roles so a link points towards the coordinator.
var roleRank = map[string]int{"EndDevice": 0, "Router": 1, "Coordinator": 2}

// meshEdges adds a talks_to edge for each link whose devices both have topics, pointing
// towards the coordinator, with its link quality.
func (s *state) meshEdges(edges map[sdk.EdgeKey]sdk.Edge) {
	for _, m := range s.meshes {
		for pair, q := range m.links {
			from, to := m.topicOf[pair[0]], m.topicOf[pair[1]]
			if s.topics[from] == nil || s.topics[to] == nil {
				continue
			}
			if rf, rt := roleRank[m.devices[from].Role], roleRank[m.devices[to].Role]; rf > rt || rf == rt && from > to {
				from, to = to, from
			}
			k := sdk.EdgeKey{From: s.ref(kindTopic, from), To: s.ref(kindTopic, to), Rel: sdk.RelTalksTo}
			edges[k] = sdk.Edge{
				From: k.From, To: k.To, Rel: k.Rel, Weight: 1, Source: s.src,
				Attrs: map[string]sdk.Value{"linkquality": sdk.Number(q)},
			}
		}
	}
}

// mapsDue are the request topics of the bridges not asked for a network map within every,
// sorted, marking them asked at now.
func (s *state) mapsDue(now time.Time, every time.Duration) []string {
	var due []string
	for base, m := range s.meshes {
		if !m.refused && (m.asked.IsZero() || now.Sub(m.asked) >= every) {
			m.asked = now
			due = append(due, base+z2mAskForMap)
		}
	}
	slices.Sort(due)
	return due
}

// mapRefused marks the bridges asked for a map within refusalWindow before the connection was
// lost at lost as refused, returning their request topics.
func (s *state) mapRefused(lost time.Time) []string {
	for _, m := range s.meshes {
		if !m.asked.IsZero() && lost.Sub(m.asked) <= refusalWindow {
			m.refused = true
		}
	}
	return s.refusedTopics()
}

// refusedTopics are the request topics of the bridges marked refused, sorted.
func (s *state) refusedTopics() []string {
	var topics []string
	for base, m := range s.meshes {
		if m.refused {
			topics = append(topics, base+z2mAskForMap)
		}
	}
	slices.Sort(topics)
	return topics
}

// mapNote says which network map requests cost the connection, or "" if none did.
func (s *state) mapNote() string {
	topics := s.refusedTopics()
	if len(topics) == 0 {
		return ""
	}
	return "the broker disconnected us after a network map request, so no more are sent; " +
		"check the credentials may publish to " + strings.Join(topics, ", ")
}
