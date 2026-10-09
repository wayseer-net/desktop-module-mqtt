package mqtt

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"wayseer.dev/sdk"
)

// Waits on the broker.
const (
	connectTimeout = 10 * time.Second
	keepAlive      = 30 * time.Second
	firstRetry     = time.Second
	lastRetry      = time.Minute
	subackRefused  = 0x80
	mapCheck       = time.Second // how often bridges due a network map request are looked for
)

// read connects to the broker, trying again with backoff until it first answers; the client
// then reconnects by itself. It disconnects when ctx ends.
func (m *Module) read(ctx context.Context) {
	var c paho.Client
	for wait := firstRetry; ; wait = min(2*wait, lastRetry) {
		var err error
		if c, err = m.dial(ctx); err == nil {
			break
		}
		m.setHealth(false, sdk.Health{Disconnected: true, Err: err})
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
	m.askForMaps(ctx, c)
	c.Disconnect(250)
}

// askForMaps publishes a network map request to each zigbee2mqtt bridge as it falls due, if
// zigbee2mqtt_networkmap is set; it is all the module ever publishes. It returns when ctx ends.
func (m *Module) askForMaps(ctx context.Context, c paho.Client) {
	m.mu.Lock()
	every := m.opts.NetworkMap
	m.mu.Unlock()
	if every == 0 {
		<-ctx.Done()
		return
	}
	t := time.NewTicker(mapCheck)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			if !c.IsConnectionOpen() {
				continue
			}
			m.mu.Lock()
			due := m.state.mapsDue(now, every)
			m.mu.Unlock()
			for _, topic := range due {
				c.Publish(topic, 0, false, z2mMapRequest)
			}
		}
	}
}

// dial makes one attempt to connect.
func (m *Module) dial(ctx context.Context) (paho.Client, error) {
	co, err := m.clientOptions()
	if err != nil {
		return nil, err
	}
	c := paho.NewClient(co)
	tok := c.Connect()
	select {
	case <-ctx.Done():
		c.Disconnect(0)
		return nil, ctx.Err()
	case <-tok.Done():
	}
	if err := tok.Error(); err != nil {
		return nil, fmt.Errorf("connecting to %s: %w", m.state.broker, err)
	}
	return c, nil
}

// clientOptions read the password and TLS files afresh, so a fixed file is picked up on retry.
func (m *Module) clientOptions() (*paho.ClientOptions, error) {
	m.mu.Lock()
	o := m.opts
	m.mu.Unlock()
	pw, err := o.Read()
	if err != nil {
		return nil, err
	}
	tc, err := tlsConfig(&o)
	if err != nil {
		return nil, err
	}
	id := o.ClientID
	if id == "" {
		id = randomClientID()
	}
	return paho.NewClientOptions().AddBroker(dialURL(o.URL)).SetClientID(id).
		SetUsername(o.Username).SetPassword(pw.Reveal()).SetTLSConfig(tc).
		SetCleanSession(o.QoS == 0 || o.ClientID == "").
		SetConnectTimeout(connectTimeout).SetKeepAlive(keepAlive).
		SetAutoReconnect(true).SetMaxReconnectInterval(lastRetry).
		SetOnConnectHandler(m.subscribe).SetConnectionLostHandler(m.lost).
		SetDefaultPublishHandler(m.message), nil
}

// tlsConfig is nil for a plain url, else TLS 1.2 or later with the CA and client pair given.
func tlsConfig(o *options) (*tls.Config, error) {
	if !strings.HasPrefix(o.URL, "mqtts:") && !strings.HasPrefix(o.URL, "wss:") {
		return nil, nil
	}
	tc := &tls.Config{MinVersion: tls.VersionTLS12}
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, fmt.Errorf("ca_file: %w", err)
		}
		tc.RootCAs = x509.NewCertPool()
		if !tc.RootCAs.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file: %s holds no PEM certificate", o.CAFile)
		}
	}
	if o.CertFile != "" {
		pair, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("cert_file and key_file: %w", err)
		}
		tc.Certificates = []tls.Certificate{pair}
	}
	return tc, nil
}

func randomClientID() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return "wayseer-" + hex.EncodeToString(b)
}

// subscribe asks for the topic filters on each connect; paho calls it on a goroutine of its own.
func (m *Module) subscribe(c paho.Client) {
	m.mu.Lock()
	filters := make(map[string]byte, len(m.opts.Topics))
	for _, f := range m.opts.Topics {
		filters[f] = m.opts.QoS
	}
	answered := m.answered
	m.mu.Unlock()
	defer answered()
	tok := c.SubscribeMultiple(filters, nil)
	if !tok.WaitTimeout(connectTimeout) {
		m.setHealth(true, sdk.Health{Err: errors.New("the broker did not answer the subscription")})
		return
	}
	if err := tok.Error(); err != nil {
		m.setHealth(true, sdk.Health{Err: fmt.Errorf("subscribing: %w", err)})
		return
	}
	m.setHealth(true, subscribed(tok.(*paho.SubscribeToken).Result()))
}

// subscribed is the health once the broker has answered for each filter.
func subscribed(codes map[string]byte) sdk.Health {
	var refused []string
	for f, code := range codes {
		if code >= subackRefused {
			refused = append(refused, f)
		}
	}
	slices.Sort(refused)
	switch {
	case len(refused) == 0:
		return sdk.Health{}
	case len(refused) == len(codes):
		return sdk.Health{Err: errors.New("the broker refused every topic filter")}
	}
	return sdk.Health{Note: "the broker refused " + strings.Join(refused, ", ")}
}

func (m *Module) lost(_ paho.Client, err error) {
	m.mu.Lock()
	m.state.mapRefused(time.Now())
	m.mu.Unlock()
	m.setHealth(false, sdk.Health{Disconnected: true, Err: fmt.Errorf("lost the broker: %w", err)})
}

func (m *Module) message(_ paho.Client, msg paho.Message) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.receive(msg.Topic(), msg.Payload(), msg.Retained(), time.Now())
}

// setHealth notes whether the broker is connected, and the health to report with any note on
// refused network map requests.
func (m *Module) setHealth(connected bool, h sdk.Health) {
	m.mu.Lock()
	m.state.connected = connected
	if n := m.state.mapNote(); n != "" {
		h.Note = strings.TrimPrefix(h.Note+"; "+n, "; ")
	}
	m.mu.Unlock()
	m.health.Store(&h)
}
