package mqtt

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"wayseer.dev/sdk"
)

// Kind is the module kind in config.
const Kind = "mqtt"

const version = "1"

// init registers the kind for a build of the app that imports the package.
func init() { sdk.Register(Kind, func() sdk.Module { return New() }) }

// Module reads what an MQTT broker carries and sends what changed every interval. It publishes
// only zigbee2mqtt network map requests, and only when zigbee2mqtt_networkmap is set.
type Module struct {
	health atomic.Pointer[sdk.Health]

	mu      sync.Mutex // guards what follows, shared by Run and the queries
	name    sdk.ModuleID
	opts    options
	state   *state
	tracker sdk.Tracker
}

// New makes an unconfigured module.
func New() *Module { return &Module{} }

// Info describes the module.
func (m *Module) Info() sdk.Info {
	return sdk.Info{Kind: Kind, Version: version, Description: "Topic trees, payloads, device status and Zigbee meshes from MQTT brokers"}
}

// Configure checks the options; nothing is read until Run.
func (m *Module) Configure(_ context.Context, cfg sdk.Config) error {
	o := defaults()
	if err := cfg.Decode(&o); err != nil {
		return err
	}
	if err := o.validate(); err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.name, m.opts, m.state = cfg.Name, o, newState(cfg.Name, &o)
	m.health.Store(&sdk.Health{})
	return nil
}

// Run reads the broker and sends a snapshot, then a delta every interval, until ctx ends.
func (m *Module) Run(ctx context.Context, sink sdk.Sink) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { m.read(ctx) })
	defer wg.Wait()
	defer cancel()
	m.mu.Lock()
	m.tracker.Reset()
	every := m.opts.Interval
	m.mu.Unlock()
	send := sink.Snapshot
	t := time.NewTimer(0)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := send(ctx, m.tick(time.Now())); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		send = sink.Delta
		t.Reset(every)
	}
}

// tick returns what changed since the last send.
func (m *Module) tick(now time.Time) *sdk.ChangeSet {
	m.mu.Lock()
	defer m.mu.Unlock()
	evs := m.state.flush(now)
	ents, edges := m.state.world(now)
	cs := m.tracker.Changes(ents, edges, now)
	cs.Events = evs
	return cs
}

// Health reports whether the broker is being read, and the catalogue's generation.
func (m *Module) Health() sdk.Health {
	var h sdk.Health
	if p := m.health.Load(); p != nil {
		h = *p
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != nil {
		h.Catalogue = m.state.cat.gen
	}
	return h
}
