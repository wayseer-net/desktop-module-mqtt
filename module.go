package mqtt

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"wayseer.dev/sdk"
)

// Kind is the module kind in config.
const Kind = "mqtt"

const version = "1"

// retryMax is the longest wait before retrying a failed read.
const retryMax = 10 * time.Second

// init registers the kind for a build of the app that imports the package.
func init() { sdk.Register(Kind, func() sdk.Module { return New() }) }

// Module reads the inventory every interval and sends what changed.
type Module struct {
	health atomic.Pointer[sdk.Health]

	mu      sync.Mutex // guards what follows, shared by Run and Discover
	name    sdk.ModuleID
	opts    options
	client  *http.Client
	token   sdk.Secret
	tracker sdk.Tracker
	world   world                       // as last read
	series  map[sdk.SeriesRef]*sdk.Ring // recorded by Run
}

// New makes an unconfigured module.
func New() *Module { return &Module{} }

// Info describes the module.
func (m *Module) Info() sdk.Info {
	return sdk.Info{Kind: Kind, Version: version, Description: "Entities read from a JSON inventory at a URL"}
}

// Configure checks the options and reads the token; nothing is fetched until Run or Discover.
func (m *Module) Configure(_ context.Context, cfg sdk.Config) error {
	o := defaults()
	if err := cfg.Decode(&o); err != nil {
		return err
	}
	if err := o.validate(); err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	token, err := o.Read()
	if err != nil {
		return fmt.Errorf("line %d: %w", cfg.Line, err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.name, m.opts, m.token = cfg.Name, o, token
	m.client = &http.Client{Timeout: o.Timeout}
	m.world, m.series = world{}, map[sdk.SeriesRef]*sdk.Ring{}
	m.health.Store(&sdk.Health{})
	return nil
}

// Run reads the inventory every interval: a snapshot after the first good read, then deltas.
// A failed read shows in Health and is retried sooner; Run returns only when ctx ends.
func (m *Module) Run(ctx context.Context, sink sdk.Sink) error {
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
		cs, err := m.refresh(ctx)
		if ctx.Err() != nil {
			return nil
		}
		m.health.Store(&sdk.Health{Err: err})
		if err != nil {
			t.Reset(min(every, retryMax))
			continue
		}
		if err := send(ctx, cs); err != nil {
			return err
		}
		send = sink.Delta
		t.Reset(every)
	}
}

// refresh reads the inventory, records its metrics, and returns what changed since the last
// send, with an event for each status that changed.
func (m *Module) refresh(ctx context.Context) (*sdk.ChangeSet, error) {
	w, err := m.read(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	var events []sdk.Event
	for _, ref := range slices.Sorted(maps.Keys(w.ents)) {
		e := w.ents[ref]
		if old, ok := m.world.ents[ref]; ok && old.Status.Level != e.Status.Level {
			events = append(events, statusEvent(&old, &e, now))
		}
	}
	m.world = w
	m.record(&w, now)
	cs := m.tracker.Changes(w.ents, w.edges, now)
	cs.Events = events
	return cs, nil
}

func statusEvent(old, e *sdk.Entity, at time.Time) sdk.Event {
	sev := sdk.SevInfo
	switch e.Status.Level {
	case sdk.StatusWarn:
		sev = sdk.SevWarn
	case sdk.StatusCrit, sdk.StatusDown:
		sev = sdk.SevError
	}
	return sdk.Event{
		ID:       string(e.Ref) + "@" + strconv.FormatInt(at.UnixNano(), 10),
		Entity:   e.Ref,
		At:       at,
		Severity: sev,
		Kind:     "status",
		Message:  fmt.Sprintf("%s is now %s, was %s", e.Name, e.Status.Level, old.Status.Level),
		Source:   e.Source,
	}
}

func (m *Module) read(ctx context.Context) (world, error) {
	m.mu.Lock()
	c, url, token, name := m.client, m.opts.URL, m.token, m.name
	m.mu.Unlock()
	items, err := fetch(ctx, c, url, token)
	if err != nil {
		return world{}, err
	}
	return buildWorld(name, items)
}

// Health reports whether the last read worked.
func (m *Module) Health() sdk.Health {
	if h := m.health.Load(); h != nil {
		return *h
	}
	return sdk.Health{}
}

// Discover reads the inventory now and returns all of it.
func (m *Module) Discover(ctx context.Context) (*sdk.ChangeSet, error) {
	w, err := m.read(ctx)
	if err != nil {
		return nil, err
	}
	var fresh sdk.Tracker
	return fresh.Changes(w.ents, w.edges, time.Now()), nil
}
