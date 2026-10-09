package mqtt

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"wayseer.dev/sdk"
)

type options struct {
	URL               string           `yaml:"url"`      // the inventory, http:// or https://
	Interval          time.Duration    `yaml:"interval"` // how often it is read
	Timeout           time.Duration    `yaml:"timeout"`  // longest wait for one read
	sdk.SecretOptions `yaml:",inline"` // an optional bearer token
}

func defaults() options {
	return options{Interval: 15 * time.Second, Timeout: 10 * time.Second}
}

func (o *options) validate() error {
	switch {
	case o.Interval < 100*time.Millisecond:
		return fmt.Errorf("interval %v must be at least 100ms", o.Interval)
	case o.Timeout <= 0 || o.Timeout > 5*time.Minute:
		return fmt.Errorf("timeout %v must be above 0 and at most 5m", o.Timeout)
	}
	return errors.Join(checkURL(o.URL), o.Validate())
}

func checkURL(raw string) error {
	u, err := url.Parse(raw)
	switch {
	case raw == "":
		return errors.New("url is required")
	case err != nil:
		return fmt.Errorf("url: %w", err)
	case u.Scheme != "http" && u.Scheme != "https" || u.Host == "":
		return fmt.Errorf("url %q must be http:// or https:// with a host", raw)
	case u.User != nil:
		return errors.New("url must not hold credentials; use secret_file, secret_env or secret_keyring")
	}
	return nil
}
