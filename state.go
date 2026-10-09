package mqtt

import (
	"bytes"
	"container/heap"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"wayseer.dev/sdk"
)

// Entity kinds.
const (
	kindBroker sdk.Kind = "mqtt/broker"
	kindTopic  sdk.Kind = "mqtt/topic"
)

// avail is what a device's availability topic last said.
type avail uint8

const (
	availUnknown avail = iota
	availUp
	availDown
)

// topic is what the broker last said on one topic, or a level that only holds others.
type topic struct {
	heard    time.Time // the last message; zero for a level no message named
	gap      time.Duration
	retained bool // the last message was retained
	live     bool // a message has come as it was published, not only retained
	bytes    int
	read     readings           // as heard, before units
	fresh    map[string]float64 // numbers heard since the last flush, by field
	avail    avail
	availOn  string // the topic that says it
	kids     int
}

// state is the module's working set: the topics heard, bounded.
type state struct {
	src       sdk.ModuleID
	broker    string // host:port, the broker entity's native ID
	opts      options
	topics    map[string]*topic
	cat       catalogue
	series    map[sdk.SeriesRef]*sdk.Ring
	pending   []sdk.Event
	messages  int // since the last flush
	flushed   time.Time
	dropped   int // topics left out by max_topics since starting
	connected bool
	seq       uint64
	meshes    map[string]*mesh // zigbee2mqtt bridges, by base topic
	ha        discovered
}

func newState(src sdk.ModuleID, o *options) *state {
	return &state{
		src: src, broker: address(o.URL), opts: *o, topics: map[string]*topic{}, cat: newCatalogue(),
		series: map[sdk.SeriesRef]*sdk.Ring{},
	}
}

// receive folds one message into the working set.
func (s *state) receive(name string, payload []byte, retained bool, now time.Time) {
	s.messages++
	s.keepAvailability(name, payload)
	if dev, up, ok := availability(name, payload); ok {
		s.setAvail(dev, name, up, now)
		return
	}
	if len(payload) == 0 && retained {
		if dev := s.topics[parentOf(name)]; dev != nil && dev.availOn == name {
			dev.avail, dev.availOn = availUnknown, ""
		}
		s.remove(name)
		return
	}
	t := s.ensure(name)
	if t == nil {
		return
	}
	if !retained {
		if t.live {
			t.gap = now.Sub(t.heard)
		}
		t.live = true
	}
	t.heard, t.retained, t.bytes = now, retained, len(payload)
	if s.zigbee2mqtt(name, payload) || s.homeAssistant(name, payload) {
		t.read = readings{}
		return
	}
	t.read = rawReadingsOf(name[strings.LastIndexByte(name, '/')+1:], payload)
	for _, r := range s.converted(name, t.read) {
		if s.cat.note(r.field) {
			if t.fresh == nil {
				t.fresh = map[string]float64{}
			}
			t.fresh[r.field] = r.v
		}
	}
}

// availLevels are the last levels of a topic that may say whether its device is up.
var availLevels = []string{"availability", "state", "status", "lwt", "online", "connected"}

// availability reads name as a device's availability topic: the device is the level above, and
// the payload says online or offline (true or false under online or connected).
func availability(name string, payload []byte) (dev string, up, ok bool) {
	i := strings.LastIndexByte(name, '/')
	level := strings.ToLower(name[i+1:])
	if i <= 0 || !slices.Contains(availLevels, level) {
		return "", false, false
	}
	p := bytes.TrimSpace(payload)
	if len(p) > 0 && p[0] == '{' {
		var v struct{ State string }
		if json.Unmarshal(p, &v) != nil {
			return "", false, false
		}
		p = []byte(v.State)
	}
	switch strings.ToLower(string(p)) {
	case "online", "connected":
		return name[:i], true, true
	case "offline", "disconnected":
		return name[:i], false, true
	case "true":
		return name[:i], true, level == "online" || level == "connected"
	case "false":
		return name[:i], false, level == "online" || level == "connected"
	}
	return "", false, false
}

// setAvail records what dev's availability topic says, raising an event when it changes.
func (s *state) setAvail(dev, on string, up bool, now time.Time) {
	t := s.ensure(dev)
	if t == nil {
		return
	}
	a := availDown
	if up {
		a = availUp
	}
	if t.avail != availUnknown && t.avail != a {
		kind, sev := "offline", sdk.SevWarn
		if up {
			kind, sev = "online", sdk.SevInfo
		}
		s.seq++
		s.pending = append(s.pending, sdk.Event{
			ID: fmt.Sprintf("%s/%d", kind, s.seq), Entity: s.ref(kindTopic, dev), At: now, Severity: sev, Kind: kind,
			Message: fmt.Sprintf("%s went %s, says %s", dev, kind, on), Source: s.src,
		})
	}
	t.avail, t.availOn = a, on
}

// ensure is name's topic, made with the levels above it if new; nil when the working set is
// twice max_topics, which flush trims back.
func (s *state) ensure(name string) *topic {
	if t := s.topics[name]; t != nil {
		return t
	}
	if len(s.topics) >= 2*s.opts.MaxTopics {
		s.dropped++
		return nil
	}
	t := &topic{}
	s.topics[name] = t
	if p := parentOf(name); p != "" {
		if up := s.ensure(p); up != nil {
			up.kids++
		}
	}
	return t
}

// parentOf is the level above name, or "" for a top level.
func parentOf(name string) string {
	if i := strings.LastIndexByte(name, '/'); i > 0 {
		return name[:i]
	}
	return ""
}

// remove forgets name; a level that holds others stays as a level. A level above left holding
// nothing, that nothing was heard on, goes too.
func (s *state) remove(name string) {
	s.forgetConfig(name)
	t := s.topics[name]
	if t == nil {
		return
	}
	if t.kids > 0 {
		t.heard, t.read, t.fresh, t.live, t.retained, t.bytes = time.Time{}, readings{}, nil, false, false, 0
		return
	}
	delete(s.topics, name)
	p := parentOf(name)
	if up := s.topics[p]; up != nil {
		up.kids--
		if up.kids == 0 && up.heard.IsZero() && up.avail == availUnknown {
			s.remove(p)
		}
	}
}

// flush closes one interval: it trims the working set to max_topics, records series points,
// and returns the events to send.
func (s *state) flush(now time.Time) []sdk.Event {
	s.trim()
	s.record(now)
	evs := s.pending
	s.pending = nil
	return evs
}

// trim drops the least recently heard topics that hold no others until max_topics are left;
// discovery configs, heard once on connecting, go last.
func (s *state) trim() {
	if len(s.topics) <= s.opts.MaxTopics {
		return
	}
	var q leaves
	for name, t := range s.topics {
		if t.kids == 0 {
			q = append(q, s.leaf(name, t))
		}
	}
	heap.Init(&q)
	for len(s.topics) > s.opts.MaxTopics && q.Len() > 0 {
		name := heap.Pop(&q).(leaf).name
		s.remove(name)
		s.dropped++
		if up, t := s.lowestLeft(name); t != nil && t.kids == 0 {
			heap.Push(&q, s.leaf(up, t))
		}
	}
}

// lowestLeft is the nearest level above name that is still kept, which remove may leave empty.
func (s *state) lowestLeft(name string) (string, *topic) {
	for p := parentOf(name); p != ""; p = parentOf(p) {
		if t := s.topics[p]; t != nil {
			return p, t
		}
	}
	return "", nil
}

func (s *state) leaf(name string, t *topic) leaf {
	_, config := s.ha.configs[name]
	return leaf{name, t.heard, config}
}

// leaf is a topic that holds no others; leaves order the first to drop first.
type leaf struct {
	name   string
	heard  time.Time
	config bool
}

type leaves []leaf

func (q leaves) Len() int      { return len(q) }
func (q leaves) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q leaves) Less(i, j int) bool {
	a, b := q[i], q[j]
	switch {
	case a.config != b.config:
		return b.config
	case !a.heard.Equal(b.heard):
		return a.heard.Before(b.heard)
	}
	return a.name < b.name
}
func (q *leaves) Push(x any) { *q = append(*q, x.(leaf)) }
func (q *leaves) Pop() any {
	old := *q
	x := old[len(old)-1]
	*q = old[:len(old)-1]
	return x
}

// ref is the entity ref of a native ID; the instance name was checked in Configure.
func (s *state) ref(kind sdk.Kind, native string) sdk.EntityRef {
	r, _ := sdk.NewEntityRef(string(s.src), kind, native)
	return r
}

func (s *state) brokerRef() sdk.EntityRef { return s.ref(kindBroker, s.broker) }
