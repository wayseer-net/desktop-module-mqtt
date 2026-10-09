package mqtt

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"wayseer.dev/sdk"
	"wayseer.dev/sdk/sdktest"
)

// inventory serves a JSON body the test can change.
type inventory struct {
	mu   sync.Mutex
	body string
	auth string // the last Authorization header
}

func (inv *inventory) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.auth = r.Header.Get("Authorization")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(inv.body))
}

func (inv *inventory) set(body string) {
	inv.mu.Lock()
	defer inv.mu.Unlock()
	inv.body = body
}

// serve starts a server for the fixture inventory.
func serve(t *testing.T) (*inventory, *httptest.Server) {
	t.Helper()
	b, err := os.ReadFile("testdata/inventory.json")
	if err != nil {
		t.Fatal(err)
	}
	inv := &inventory{body: string(b)}
	srv := httptest.NewServer(inv)
	t.Cleanup(srv.Close)
	return inv, srv
}

// configured is a module configured with options.
func configured(t *testing.T, options string) *Module {
	t.Helper()
	cfg, err := sdktest.Config("inv", options)
	if err != nil {
		t.Fatal(err)
	}
	m := New()
	if err := m.Configure(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestConformance(t *testing.T) {
	_, srv := serve(t)
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	sdktest.Conform(t, sdktest.Case{
		New:     func() sdk.Module { return New() },
		Name:    "inv",
		Options: "url: " + srv.URL,
		Failing: "url: " + closed.URL,
	})
}

func TestSnapshotMatchesTheInventory(t *testing.T) {
	_, srv := serve(t)
	cs, err := configured(t, "url: "+srv.URL).Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	sdktest.Golden(t, "testdata/inventory.txt", render(cs))
}

// render lists a change set's entities and edges, one per line.
func render(cs *sdk.ChangeSet) string {
	var b strings.Builder
	for _, e := range cs.Upserts {
		keys := make([]string, 0, len(e.Attrs))
		for k, v := range e.Attrs {
			keys = append(keys, k+"="+v.String())
		}
		slices.Sort(keys)
		fmt.Fprintf(&b, "%s %q %s\n", e.Ref, e.Name, strings.Join(append([]string{e.Status.Level.String()}, keys...), " "))
	}
	for _, e := range cs.Edges {
		fmt.Fprintf(&b, "%s %s %s\n", e.From, e.Rel, e.To)
	}
	return b.String()
}

func TestAStatusChangeIsAnEvent(t *testing.T) {
	inv, srv := serve(t)
	m := configured(t, "url: "+srv.URL+"\ninterval: 100ms")
	sink := sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
	sink.WaitFor(t, 1)
	inv.set(`{"items": [{"id": "web-01", "status": "crit"}]}`)
	sdktest.Eventually(t, func() bool { return len(sink.Events()) > 0 })
	ev := sink.Events()[0]
	if ev.Severity != sdk.SevError || ev.Message != "web-01 is now crit, was ok" || ev.Entity.Native() != "web-01" {
		t.Errorf("event %+v", ev)
	}
}

func TestEachReadIsAPointInTheItemsSeries(t *testing.T) {
	inv, srv := serve(t)
	m := configured(t, "url: "+srv.URL+"\ninterval: 100ms")
	sink := sdktest.Run(t, func(ctx context.Context, s *sdktest.Sink) error { return m.Run(ctx, s) })
	sink.WaitFor(t, 1)
	inv.set(`{"items": [{"id": "web-01", "metrics": {"cpu.utilisation": 50}}]}`)
	web := sdk.SeriesRef{Entity: "inv/host/web-01", Metric: "cpu.utilisation"}
	query := func() []sdk.Series {
		now := time.Now()
		got, err := m.QuerySeries(context.Background(), sdk.SeriesQuery{
			Metrics: []string{web.Metric},
			Window:  sdk.TimeWindow{From: now.Add(-time.Minute), To: now.Add(time.Second)},
		})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	sdktest.Eventually(t, func() bool {
		got := query()
		return len(got) == 1 && len(got[0].Points) == 2
	})
	got := query()[0]
	if got.Ref != web || got.Unit != sdk.UnitPercent || got.Points[0].V != 42 || got.Points[1].V != 50 {
		t.Errorf("series %+v", got)
	}
}

func TestTheTokenIsSentButNeverShown(t *testing.T) {
	inv, srv := serve(t)
	t.Setenv("INV_TOKEN", "s3cret-token")
	m := configured(t, "url: "+srv.URL+"\nsecret_env: INV_TOKEN")
	if _, err := m.Discover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if inv.auth != "Bearer s3cret-token" {
		t.Errorf("Authorization %q", inv.auth)
	}
	inv.set("not json")
	_, err := m.Discover(context.Background())
	if err == nil || strings.Contains(err.Error(), "s3cret") {
		t.Errorf("error %v", err)
	}
}

func TestBadInventoriesAreErrors(t *testing.T) {
	for _, body := range []string{
		`not json`,
		`{"items": [{"id": ""}]}`,
		`{"items": [{"id": "a", "status": "fine"}]}`,
		`{"items": [{"id": "a", "kind": "Not A Kind"}]}`,
		`{"items": [{"id": "a", "attrs": {"nested": {"x": 1}}}]}`,
		`{"items": [{"id": "a"}, {"id": "a"}]}`,
		`{"items": [{"id": "a", "metrics": {"no.such.metric": 1}}]}`,
	} {
		inv, srv := serve(t)
		inv.set(body)
		if _, err := configured(t, "url: "+srv.URL).Discover(context.Background()); err == nil {
			t.Errorf("%s accepted", body)
		}
	}
}

func TestBadOptionsAreRejected(t *testing.T) {
	for _, opts := range []string{
		"",
		"url: ftp://example.test/inv.json",
		"url: http://user:pass@example.test/",
		"url: http://example.test/\ninterval: 10ms",
		"url: http://example.test/\ntimeout: 0s",
	} {
		cfg, err := sdktest.Config("inv", opts)
		if err != nil {
			t.Fatal(err)
		}
		if err := New().Configure(context.Background(), cfg); err == nil {
			t.Errorf("%q accepted", opts)
		}
	}
}

func TestRecheckReadsTheItemAgainAndSaysItsStatus(t *testing.T) {
	inv, srv := serve(t)
	m := configured(t, "url: "+srv.URL)
	if err := sdk.ValidateActions(m.Actions()); err != nil {
		t.Fatal(err)
	}
	web := sdk.ActionRequest{Instance: "inv", Action: "recheck", Entity: "inv/host/web-01"}
	inv.set(`{"items": [{"id": "web-01", "status": "warn"}]}`)
	if res, err := m.Do(context.Background(), web); err != nil || res.Message != "web-01 is warn" {
		t.Errorf("recheck: %+v, %v", res, err)
	}
	inv.set(`{"items": []}`)
	if _, err := m.Do(context.Background(), web); err == nil || !strings.Contains(err.Error(), "no longer in the inventory") {
		t.Errorf("a gone item: %v", err)
	}
	for _, bad := range []sdk.ActionRequest{
		{Instance: "inv", Action: "reboot", Entity: web.Entity},
		{Instance: "inv", Action: "recheck", Entity: web.Entity, Params: map[string]string{"x": "1"}},
		{Instance: "inv", Action: "recheck", Entity: "inv/pod/web-01"},
	} {
		if _, err := m.Do(context.Background(), bad); err == nil {
			t.Errorf("%+v ran", bad)
		}
	}
}
