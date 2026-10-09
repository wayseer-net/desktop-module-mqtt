package mqtt

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"wayseer.dev/sdk"
)

// Limits on what one instance may ask of a broker.
const (
	maxTopicFilters = 64
	maxFilterBytes  = 65535 // MQTT's longest topic
)

type options struct {
	URL               string           `yaml:"url"`                    // the broker: mqtt://, mqtts://, ws:// or wss://
	Topics            []string         `yaml:"topics"`                 // topic filters to subscribe to, such as zigbee2mqtt/#
	QoS               byte             `yaml:"qos"`                    // 0, or 1 with client_id for the broker to keep what was missed
	ClientID          string           `yaml:"client_id"`              // sent to the broker; a random one if empty
	Username          string           `yaml:"username"`               // its password comes from secret_file, secret_env or secret_keyring
	CAFile            string           `yaml:"ca_file"`                // PEM roots for mqtts:// and wss://; the system's if empty
	CertFile          string           `yaml:"cert_file"`              // a PEM client certificate, with key_file
	KeyFile           string           `yaml:"key_file"`               // its PEM key
	Interval          time.Duration    `yaml:"interval"`               // how often changes and series points are sent
	StaleAfter        time.Duration    `yaml:"stale_after"`            // how long a topic may be silent before it shows as stale
	MaxTopics         int              `yaml:"max_topics"`             // most topics kept; the least recently heard go first
	NetworkMap        time.Duration    `yaml:"zigbee2mqtt_networkmap"` // how often to ask each zigbee2mqtt bridge for its mesh; never if 0
	sdk.SecretOptions `yaml:",inline"` // the broker password, if it wants one
}

func defaults() options {
	return options{Interval: 2 * time.Second, StaleAfter: 10 * time.Minute, MaxTopics: 5000}
}

var clientID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// validate checks every option.
func (o *options) validate() error {
	return errors.Join(o.checkURL(), o.checkTopics(), o.checkScalars(), o.checkAuth(), o.Validate())
}

func (o *options) checkURL() error {
	u, err := url.Parse(o.URL)
	switch {
	case o.URL == "":
		return errors.New("url is required")
	case err != nil:
		return fmt.Errorf("url: %w", err)
	case !slices.Contains([]string{"mqtt", "mqtts", "ws", "wss"}, u.Scheme) || u.Host == "":
		return fmt.Errorf("url %q must be mqtt://, mqtts://, ws:// or wss:// with a host", o.URL)
	case u.User != nil:
		return errors.New("url must not hold credentials; use username and secret_file, secret_env or secret_keyring")
	case (o.CAFile != "" || o.CertFile != "") && u.Scheme != "mqtts" && u.Scheme != "wss":
		return errors.New("ca_file and cert_file need mqtts:// or wss://")
	}
	return nil
}

func (o *options) checkTopics() error {
	switch {
	case len(o.Topics) == 0:
		return errors.New("topics: name the topic filters to subscribe to, such as zigbee2mqtt/# or '#'")
	case len(o.Topics) > maxTopicFilters:
		return fmt.Errorf("topics: %d named, at most %d", len(o.Topics), maxTopicFilters)
	}
	for i, f := range o.Topics {
		if err := checkFilter(f); err != nil {
			return fmt.Errorf("topics: %q %w", f, err)
		}
		if slices.Contains(o.Topics[:i], f) {
			return fmt.Errorf("topics: %q is named twice", f)
		}
	}
	return nil
}

// checkFilter applies MQTT's rules for a topic filter: '#' only as the whole last level, '+'
// only as a whole level.
func checkFilter(f string) error {
	switch {
	case f == "":
		return errors.New("is empty")
	case len(f) > maxFilterBytes || !utf8.ValidString(f):
		return errors.New("is too long or not UTF-8")
	case strings.ContainsRune(f, 0):
		return errors.New("holds NUL")
	}
	levels := strings.Split(f, "/")
	for i, l := range levels {
		switch {
		case strings.Contains(l, "#") && (l != "#" || i != len(levels)-1):
			return errors.New("may hold '#' only as its whole last level")
		case strings.Contains(l, "+") && l != "+":
			return errors.New("may hold '+' only as a whole level")
		}
	}
	return nil
}

func (o *options) checkScalars() error {
	switch {
	case o.QoS > 1:
		return fmt.Errorf("qos %d must be 0 or 1", o.QoS)
	case o.ClientID != "" && !clientID.MatchString(o.ClientID):
		return fmt.Errorf("client_id %q must be 1 to 64 letters, digits, '.', '_' or '-'", o.ClientID)
	case o.Interval < time.Second || o.Interval > time.Minute:
		return fmt.Errorf("interval %v must be from 1s to 1m", o.Interval)
	case o.StaleAfter < o.Interval || o.StaleAfter > 7*24*time.Hour:
		return fmt.Errorf("stale_after %v must be from the interval to 168h", o.StaleAfter)
	case o.MaxTopics < 1 || o.MaxTopics > 100_000:
		return fmt.Errorf("max_topics %d must be from 1 to 100000", o.MaxTopics)
	case o.NetworkMap != 0 && (o.NetworkMap < time.Minute || o.NetworkMap > 24*time.Hour):
		return fmt.Errorf("zigbee2mqtt_networkmap %v must be 0 or from 1m to 24h", o.NetworkMap)
	}
	return nil
}

// checkAuth wants client certificates as a pair, and a username for a password, as MQTT does.
func (o *options) checkAuth() error {
	switch {
	case (o.CertFile == "") != (o.KeyFile == ""):
		return errors.New("cert_file and key_file go together")
	case o.Set() && o.Username == "":
		return errors.New("a password needs a username")
	}
	return nil
}

// defaultPorts are each scheme's port when the url names none.
var defaultPorts = map[string]string{"mqtt": "1883", "mqtts": "8883", "ws": "80", "wss": "443"}

// address is the broker's host:port from a valid url, with the scheme's port if it names none.
func address(rawURL string) string {
	u, _ := url.Parse(rawURL)
	if u.Port() != "" {
		return u.Host
	}
	return net.JoinHostPort(u.Hostname(), defaultPorts[u.Scheme])
}
