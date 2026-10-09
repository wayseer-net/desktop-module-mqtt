package mqtt

import (
	"os"
	"testing"
	"time"
)

// TestAgainstARealBroker reads the broker at MQTT_TEST_URL for a while and logs what it saw;
// MQTT_TEST_TOPICS narrows it from '#', and MQTT_TEST_OPTIONS adds options, such as ca_file.
func TestAgainstARealBroker(t *testing.T) {
	url := os.Getenv("MQTT_TEST_URL")
	if url == "" {
		t.Skip("set MQTT_TEST_URL to read a real broker")
	}
	topics := os.Getenv("MQTT_TEST_TOPICS")
	if topics == "" {
		topics = "'#'"
	}
	m := running(t, "url: "+url+"\ntopics: ["+topics+"]\n"+os.Getenv("MQTT_TEST_OPTIONS"))
	for end := time.Now().Add(30 * time.Second); !m.isConnected(); time.Sleep(100 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatalf("not connected: %+v", m.Health())
		}
	}
	time.Sleep(5 * time.Second)
	m.mu.Lock()
	defer m.mu.Unlock()
	t.Logf("%d topics, %d metrics, %d dropped, health %+v\n%s",
		len(m.state.topics), len(m.state.cat.metrics), m.state.dropped, m.health.Load(), render(m.state, time.Now()))
}
