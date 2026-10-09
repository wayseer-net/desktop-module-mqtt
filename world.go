package mqtt

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"wayseer.dev/sdk"
)

// world builds the broker and each topic as entities, each topic a child of the level above
// it, or of the broker.
func (s *state) world(now time.Time) (map[sdk.EntityRef]sdk.Entity, map[sdk.EdgeKey]sdk.Edge) {
	ents := map[sdk.EntityRef]sdk.Entity{}
	edges := map[sdk.EdgeKey]sdk.Edge{}
	broker := s.brokerRef()
	ents[broker] = sdk.Entity{
		Ref: broker, Kind: kindBroker, Name: s.broker, Status: s.brokerStatus(), Source: s.src,
		Attrs: map[string]sdk.Value{"topics": sdk.Number(float64(len(s.topics))).In(sdk.UnitCount), "dropped": sdk.Number(float64(s.dropped)).In(sdk.UnitCount)},
	}
	for name, t := range s.topics {
		ref := s.ref(kindTopic, name)
		ents[ref] = sdk.Entity{Ref: ref, Kind: kindTopic, Name: levelName(name), Status: t.status(s.opts.StaleAfter, now), Attrs: s.attrs(name, t), Source: s.src}
		parent := broker
		if p := parentOf(name); p != "" {
			parent = s.ref(kindTopic, p)
		}
		k := sdk.EdgeKey{From: parent, To: ref, Rel: sdk.RelParentOf}
		edges[k] = sdk.Edge{From: parent, To: ref, Rel: sdk.RelParentOf, Weight: 1, Source: s.src}
	}
	s.meshEdges(edges)
	s.deviceWorld(ents, edges)
	return ents, edges
}

// levelName is a topic's last level, or the whole topic when that level is empty.
func levelName(name string) string {
	if l := name[strings.LastIndexByte(name, '/')+1:]; l != "" {
		return l
	}
	return name
}

func (s *state) brokerStatus() sdk.Status {
	if s.connected {
		return sdk.Status{Level: sdk.StatusOK}
	}
	return sdk.Status{Level: sdk.StatusDown, Reason: "not connected"}
}

// status is down when the device's availability topic says offline, and stale when a topic
// that has had live messages goes quiet for longer than three of its gaps and staleAfter.
func (t *topic) status(staleAfter time.Duration, now time.Time) sdk.Status {
	switch {
	case t.avail == availDown:
		return sdk.Status{Level: sdk.StatusDown, Reason: "offline, says " + t.availOn}
	case t.live && now.Sub(t.heard) > max(staleAfter, 3*t.gap):
		return sdk.Status{Level: sdk.StatusWarn, Reason: "nothing heard for " + span(now.Sub(t.heard))}
	}
	return sdk.Status{Level: sdk.StatusOK}
}

// span is d in whole minutes, or hours past two.
func span(d time.Duration) string {
	if d >= 2*time.Hour {
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dm", int(d.Minutes()))
}

// attrs are a topic's readings, numbers in their units, then what the broker and any bridge
// said of it.
func (s *state) attrs(name string, t *topic) map[string]sdk.Value {
	a := make(map[string]sdk.Value, len(t.read.attrs)+len(t.read.nums)+3)
	if !s.onlyAvailability(name) {
		maps.Copy(a, t.read.attrs)
	}
	for _, r := range s.converted(name, t.read) {
		a[r.field] = sdk.Number(r.v).In(unitOf(r.field))
	}
	a["topic"] = sdk.String(name)
	if !t.heard.IsZero() {
		a["retained"] = sdk.Bool(t.retained)
		a["bytes"] = sdk.Number(float64(t.bytes)).In(sdk.UnitBytes)
	}
	if t.availOn != "" {
		a["availability"] = sdk.String(t.availOn)
	}
	s.deviceAttrs(name, a)
	return a
}
