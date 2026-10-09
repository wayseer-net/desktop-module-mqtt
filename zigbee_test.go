package mqtt

import (
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	server "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/packets"

	"wayseer.dev/sdk"
	"wayseer.dev/sdk/sdktest"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	return fixtureIn(t, "zigbee2mqtt", name)
}

func fixtureIn(t *testing.T, dir, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/" + dir + "/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// zigbee is the house with the bridge's retained device list and a network map someone asked for.
func zigbee(t *testing.T, s *state) {
	house(s)
	s.receive("zigbee2mqtt/bridge/devices", fixture(t, "devices.json"), true, t0)
	s.receive("zigbee2mqtt/bridge/response/networkmap", fixture(t, "networkmap.json"), false, t0.Add(time.Second))
}

func TestTheBridgesDevicesAndNetworkMapMakeTheMesh(t *testing.T) {
	s := newTestState(t, "")
	zigbee(t, s)
	sdktest.Golden(t, "testdata/zigbee2mqtt/house.txt", render(s, t0.Add(2*time.Second)))
}

func TestABridgeTopicIsNotReadAsAReading(t *testing.T) {
	s := newTestState(t, "")
	zigbee(t, s)
	ents, _ := s.world(t0)
	for _, name := range []string{"zigbee2mqtt/bridge/devices", "zigbee2mqtt/bridge/response/networkmap"} {
		e := ents[s.ref(kindTopic, name)]
		if len(e.Attrs) != 3 { // topic, retained and bytes
			t.Errorf("%s has %v", name, e.Attrs)
		}
	}
}

func TestALinkIsOneEdgeTowardsTheCoordinatorWithItsBestQuality(t *testing.T) {
	s := newTestState(t, "")
	zigbee(t, s)
	_, edges := s.world(t0)
	light, coord := s.ref(kindTopic, "zigbee2mqtt/hall_light"), s.ref(kindTopic, "zigbee2mqtt/Coordinator")
	e, ok := edges[sdk.EdgeKey{From: light, To: coord, Rel: sdk.RelTalksTo}]
	if !ok {
		t.Fatal("no link from the light to the coordinator")
	}
	if q := e.Attrs["linkquality"]; q.Num() != 180 {
		t.Errorf("linkquality %v, want the better of 180 and 160", q)
	}
	if _, ok := edges[sdk.EdgeKey{From: coord, To: light, Rel: sdk.RelTalksTo}]; ok {
		t.Error("the link is drawn both ways")
	}
}

func TestANewDeviceListReplacesTheOld(t *testing.T) {
	s := newTestState(t, "")
	zigbee(t, s)
	without := strings.Replace(string(fixture(t, "devices.json")), `"friendly_name":"kitchen"`, `"friendly_name":"pantry"`, 1)
	s.receive("zigbee2mqtt/bridge/devices", []byte(without), true, t0.Add(time.Minute))
	ents, edges := s.world(t0.Add(time.Minute))
	if m := ents[s.ref(kindTopic, "zigbee2mqtt/kitchen")].Attrs["model"]; m.Type() != sdk.TypeNone {
		t.Errorf("the renamed kitchen keeps model %v", m)
	}
	pantry := s.ref(kindTopic, "zigbee2mqtt/pantry")
	if ents[pantry].Attrs["model"].Str() != "WSDCGQ11LM" {
		t.Error("the pantry has no model")
	}
	if _, ok := edges[sdk.EdgeKey{From: pantry, To: s.ref(kindTopic, "zigbee2mqtt/hall_light"), Rel: sdk.RelTalksTo}]; !ok {
		t.Error("the mesh doesn't follow the rename")
	}
}

func TestAFailedNetworkMapIsIgnored(t *testing.T) {
	s := newTestState(t, "")
	zigbee(t, s)
	s.receive("zigbee2mqtt/bridge/response/networkmap", []byte(`{"data":{},"status":"error","error":"timed out"}`), false, t0.Add(time.Minute))
	if _, edges := s.world(t0); !hasRel(edges, sdk.RelTalksTo) {
		t.Error("a failed request forgot the mesh")
	}
}

func hasRel(edges map[sdk.EdgeKey]sdk.Edge, rel sdk.Relation) bool {
	for k := range edges {
		if k.Rel == rel {
			return true
		}
	}
	return false
}

// mapRequests answers each network map request on b with the fixture, counting them.
func mapRequests(t *testing.T, b *testBroker) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	answer := fixture(t, "networkmap.json")
	err := b.Subscribe("zigbee2mqtt/bridge/request/networkmap", 1, func(_ *server.Client, _ packets.Subscription, pk packets.Packet) {
		if string(pk.Payload) == `{"type":"raw","routes":false}` {
			n.Add(1)
			go func() { _ = b.Publish("zigbee2mqtt/bridge/response/networkmap", answer, false, 0) }()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return &n
}

func (m *Module) meshLinks() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, edges := m.state.world(time.Now())
	n := 0
	for k := range edges {
		if k.Rel == sdk.RelTalksTo {
			n++
		}
	}
	return n
}

func TestAsksEachBridgeForItsNetworkMapWhenSetTo(t *testing.T) {
	b := startBroker(t, false, new(auth.AllowHook), nil)
	n := mapRequests(t, b)
	b.publish(t, "zigbee2mqtt/bridge/devices", string(fixture(t, "devices.json")), true)
	m := running(t, "url: mqtt://"+b.addr+"\ntopics: [zigbee2mqtt/#]\nzigbee2mqtt_networkmap: 1m\n")
	sdktest.Eventually(t, func() bool { return m.meshLinks() == 3 })
	if got := n.Load(); got != 1 {
		t.Errorf("asked %d times, want once until a minute passes", got)
	}
}

func TestNeverPublishesUnlessAskedTo(t *testing.T) {
	b := startBroker(t, false, new(auth.AllowHook), nil)
	n := mapRequests(t, b)
	b.publish(t, "zigbee2mqtt/bridge/devices", string(fixture(t, "devices.json")), true)
	m := running(t, "url: mqtt://"+b.addr+"\ntopics: [zigbee2mqtt/#]\n")
	sdktest.Eventually(t, func() bool { return m.heard("zigbee2mqtt/bridge/devices") != nil })
	time.Sleep(2 * mapCheck)
	if n.Load() != 0 {
		t.Error("asked for a network map without zigbee2mqtt_networkmap")
	}
}

// refusePublish disconnects a client that asks for a network map, as HiveMQ does to an MQTT
// 3.1.1 client that publishes where it may not.
type refusePublish struct{ server.HookBase }

func (h *refusePublish) ID() string           { return "refuse-publish" }
func (h *refusePublish) Provides(b byte) bool { return b == server.OnPublish }
func (h *refusePublish) OnPublish(cl *server.Client, pk packets.Packet) (packets.Packet, error) {
	if strings.HasSuffix(pk.TopicName, z2mAskForMap) {
		cl.Stop(packets.ErrNotAuthorized)
		return pk, packets.ErrRejectPacket
	}
	return pk, nil
}

func (m *Module) note() string {
	if h := m.health.Load(); h != nil {
		return h.Note
	}
	return ""
}

func TestADisconnectAfterAskingForAMapStopsTheAskingWithANote(t *testing.T) {
	b := startBroker(t, false, new(auth.AllowHook), nil)
	if err := b.AddHook(new(refusePublish), nil); err != nil {
		t.Fatal(err)
	}
	b.publish(t, "zigbee2mqtt/bridge/devices", string(fixture(t, "devices.json")), true)
	m := running(t, "url: mqtt://"+b.addr+"\ntopics: [zigbee2mqtt/#]\nzigbee2mqtt_networkmap: 1m\n")
	sdktest.Eventually(t, func() bool { return strings.Contains(m.note(), "zigbee2mqtt/bridge/request/networkmap") })
	sdktest.Eventually(t, m.isConnected)
}

func TestABridgeThatCostTheConnectionIsNotAskedAgain(t *testing.T) {
	s := newTestState(t, "")
	zigbee(t, s)
	if due := s.mapsDue(t0, time.Minute); len(due) != 1 {
		t.Fatalf("due %v", due)
	}
	if refused := s.mapRefused(t0.Add(time.Second)); len(refused) != 1 {
		t.Fatalf("refused %v", refused)
	}
	if due := s.mapsDue(t0.Add(time.Hour), time.Minute); len(due) != 0 {
		t.Errorf("asks again: %v", due)
	}
}

func TestADisconnectLongAfterAskingIsNotARefusal(t *testing.T) {
	s := newTestState(t, "")
	zigbee(t, s)
	s.mapsDue(t0, time.Minute)
	if refused := s.mapRefused(t0.Add(time.Minute)); len(refused) != 0 {
		t.Errorf("refused %v", refused)
	}
}
