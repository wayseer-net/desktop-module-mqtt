package mqtt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	server "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"wayseer.dev/sdk"
	"wayseer.dev/sdk/sdktest"
)

// testBroker is an MQTT broker in the test's process, listening on a free port.
type testBroker struct {
	*server.Server
	addr string
	once sync.Once
}

// stop closes the broker, once however often it is called.
func (b *testBroker) stop() { b.once.Do(func() { _ = b.Close() }) }

// startBroker serves MQTT over TCP, or websockets if ws, letting in whoever hook allows.
func startBroker(t *testing.T, ws bool, hook server.Hook, config any) *testBroker {
	t.Helper()
	return startBrokerTLS(t, ws, nil, hook, config)
}

// startBrokerTLS is startBroker with TLS if tc is not nil.
func startBrokerTLS(t *testing.T, ws bool, tc *tls.Config, hook server.Hook, config any) *testBroker {
	t.Helper()
	b := &testBroker{Server: server.New(&server.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}), addr: freeAddr(t)}
	if err := b.AddHook(hook, config); err != nil {
		t.Fatal(err)
	}
	cfg := listeners.Config{ID: "test", Address: b.addr, TLSConfig: tc}
	var l listeners.Listener = listeners.NewTCP(cfg)
	if ws {
		l = listeners.NewWebsocket(cfg)
	}
	if err := b.AddListener(l); err != nil {
		t.Fatal(err)
	}
	if err := b.Serve(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.stop)
	return b
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().String()
}

func (b *testBroker) publish(t *testing.T, topic, payload string, retain bool) {
	t.Helper()
	if err := b.Publish(topic, []byte(payload), retain, 0); err != nil {
		t.Fatal(err)
	}
}

// configured is a module configured with yaml.
func configured(t *testing.T, yaml string) *Module {
	t.Helper()
	cfg, err := sdktest.Config("home", yaml)
	if err != nil {
		t.Fatal(err)
	}
	m := New()
	if err := m.Configure(t.Context(), cfg); err != nil {
		t.Fatal(err)
	}
	return m
}

// running configures a module with yaml and runs it until the test ends.
func running(t *testing.T, yaml string) *Module {
	t.Helper()
	m := configured(t, yaml)
	sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
	return m
}

// heard is the topic's readings as the module last read them, or nil.
func (m *Module) heard(name string) *readings {
	m.mu.Lock()
	defer m.mu.Unlock()
	if tp := m.state.topics[name]; tp != nil && !tp.heard.IsZero() {
		r := tp.read
		return &r
	}
	return nil
}

func (m *Module) isConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state.connected
}

func TestReadsRetainedThenLiveMessagesOnTheTopicsNamed(t *testing.T) {
	for _, ws := range []bool{false, true} {
		t.Run(map[bool]string{false: "tcp", true: "websocket"}[ws], func(t *testing.T) {
			b := startBroker(t, ws, new(auth.AllowHook), nil)
			b.publish(t, "zigbee2mqtt/kitchen", `{"temperature":21.5}`, true)
			url := "mqtt://" + b.addr
			if ws {
				url = "ws://" + b.addr + "/mqtt"
			}
			m := running(t, "url: "+url+"\ntopics: [zigbee2mqtt/#, garden/#]\n")
			sdktest.Eventually(t, func() bool { return m.heard("zigbee2mqtt/kitchen") != nil })
			if h := m.Health(); h.Disconnected || h.Err != nil {
				t.Fatalf("health %+v once reading", h)
			}
			b.publish(t, "other/thing", "1", false)
			b.publish(t, "garden/temperature", "14.2", false)
			sdktest.Eventually(t, func() bool { return m.heard("garden/temperature") != nil })
			if m.heard("other/thing") != nil {
				t.Error("read a topic no filter names")
			}
		})
	}
}

func TestSendsThePasswordFromItsSecret(t *testing.T) {
	ledger := &auth.Ledger{
		Auth: auth.AuthRules{{Username: "wayseer", Password: "s3cret", Allow: true}},
		ACL:  auth.ACLRules{{Filters: auth.Filters{"#": auth.ReadOnly}}},
	}
	b := startBroker(t, false, new(auth.Hook), &auth.Options{Ledger: ledger})
	b.publish(t, "garden/temperature", "14.2", true)
	t.Setenv("MQTT_TEST_PASSWORD", "s3cret")
	m := running(t, "url: mqtt://"+b.addr+"\ntopics: ['#']\nusername: wayseer\nsecret_env: MQTT_TEST_PASSWORD\n")
	sdktest.Eventually(t, func() bool { return m.heard("garden/temperature") != nil })
}

func TestAWrongPasswordIsAnErrorThatDoesNotRepeatIt(t *testing.T) {
	ledger := &auth.Ledger{Auth: auth.AuthRules{{Username: "wayseer", Password: "s3cret", Allow: true}}}
	b := startBroker(t, false, new(auth.Hook), &auth.Options{Ledger: ledger})
	t.Setenv("MQTT_TEST_PASSWORD", "guessed")
	m := running(t, "url: mqtt://"+b.addr+"\ntopics: ['#']\nusername: wayseer\nsecret_env: MQTT_TEST_PASSWORD\n")
	sdktest.Eventually(t, func() bool { return m.Health().Err != nil })
	h := m.Health()
	if !h.Disconnected || strings.Contains(h.Err.Error(), "guessed") {
		t.Errorf("health %+v", h)
	}
}

func TestALostBrokerShowsDisconnectedAndTheBrokerDown(t *testing.T) {
	b := startBroker(t, false, new(auth.AllowHook), nil)
	m := running(t, "url: mqtt://"+b.addr+"\ntopics: ['#']\n")
	sdktest.Eventually(t, m.isConnected)
	b.stop()
	sdktest.Eventually(t, func() bool { return m.Health().Disconnected })
	if m.isConnected() {
		t.Error("still connected")
	}
	m.mu.Lock()
	ents, _ := m.state.world(t0)
	m.mu.Unlock()
	if e := ents[m.state.brokerRef()]; e.Status.Level != sdk.StatusDown {
		t.Errorf("broker %+v", e.Status)
	}
}

// selfSigned is a certificate for 127.0.0.1 that is its own CA, and its PEM written to a file.
func selfSigned(t *testing.T) (tls.Certificate, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test broker"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, ca
}

func TestReadsOverTLSTrustingTheCANamed(t *testing.T) {
	cert, ca := selfSigned(t)
	b := startBrokerTLS(t, false, &tls.Config{Certificates: []tls.Certificate{cert}}, new(auth.AllowHook), nil)
	b.publish(t, "garden/temperature", "14.2", true)
	m := running(t, "url: mqtts://"+b.addr+"\ntopics: ['#']\nca_file: "+ca+"\n")
	sdktest.Eventually(t, func() bool { return m.heard("garden/temperature") != nil })
}

func TestAnUntrustedCertificateIsAnError(t *testing.T) {
	cert, _ := selfSigned(t)
	b := startBrokerTLS(t, false, &tls.Config{Certificates: []tls.Certificate{cert}}, new(auth.AllowHook), nil)
	m := running(t, "url: mqtts://"+b.addr+"\ntopics: ['#']\n")
	sdktest.Eventually(t, func() bool { return m.Health().Err != nil })
	if err := m.Health().Err; !strings.Contains(err.Error(), "certificate") {
		t.Errorf("error %v does not name the certificate", err)
	}
}

func TestRefusedFiltersAreANoteUnlessAllAre(t *testing.T) {
	if h := subscribed(map[string]byte{"a/#": 0, "b/#": 1}); h != (sdk.Health{}) {
		t.Errorf("all granted: %+v", h)
	}
	if h := subscribed(map[string]byte{"a/#": 0, "c/#": 0x80, "b/#": 0x80}); h.Err != nil || h.Note != "the broker refused b/#, c/#" {
		t.Errorf("some refused: %+v", h)
	}
	if h := subscribed(map[string]byte{"a/#": 0x80}); h.Err == nil {
		t.Errorf("all refused: %+v", h)
	}
}
