package mqtt

import (
	"context"
	"maps"
	"slices"
	"time"

	"wayseer.dev/sdk"
)

// Bounds on what is charted.
const (
	maxMetrics    = 200  // fields in the catalogue
	maxSeries     = 4096 // series kept
	historyPoints = 256  // points each series keeps
)

// metricMessages is the broker's message rate.
const metricMessages = "messages"

// catalogue is the metrics charted: the broker's message rate, then each numeric field as
// first heard. gen changes as it grows, so the app asks for it again.
type catalogue struct {
	metrics []sdk.Metric
	gen     uint64
}

func newCatalogue() catalogue {
	return catalogue{metrics: []sdk.Metric{{
		Name: metricMessages, Unit: sdk.UnitPerSec, Kinds: []sdk.Kind{kindBroker},
		Description: "messages the broker sent on the subscribed topics", Native: "PUBLISH packets received",
	}}}
}

// note adds field to the catalogue if it is new and there is room, and reports whether it is
// charted. A field of a known unit shows with its topic; others are chosen by name.
func (c *catalogue) note(field string) bool {
	if slices.ContainsFunc(c.metrics, func(m sdk.Metric) bool { return m.Name == field }) {
		return true
	}
	if len(c.metrics) >= maxMetrics {
		return false
	}
	u := unitFor(field)
	c.metrics = append(c.metrics, sdk.Metric{
		Name: field, Unit: unitOf(field), Kinds: []sdk.Kind{kindTopic}, Extra: u == nil && field != fieldValue,
		Description: "the " + field + " in payloads", Native: field,
	})
	c.gen++
	return true
}

// record adds a point per series for the interval just ended: the broker's message rate, and
// each number heard, at the time it was heard. Series of topics gone are forgotten.
func (s *state) record(now time.Time) {
	secs := now.Sub(s.flushed).Seconds()
	if s.flushed.IsZero() || secs <= 0 {
		secs = s.opts.Interval.Seconds()
	}
	s.flushed = now
	s.add(s.brokerRef(), metricMessages, now, float64(s.messages)/secs)
	s.messages = 0
	for name, t := range s.topics {
		for field, v := range t.fresh {
			s.add(s.ref(kindTopic, name), field, t.heard, v)
		}
		t.fresh = nil
	}
	maps.DeleteFunc(s.series, func(ref sdk.SeriesRef, _ *sdk.Ring) bool {
		_, kept := s.topics[ref.Entity.Native()]
		return !kept && ref.Entity != s.brokerRef()
	})
}

func (s *state) add(ref sdk.EntityRef, metric string, at time.Time, v float64) {
	key := sdk.SeriesRef{Entity: ref, Metric: metric}
	r := s.series[key]
	if r == nil {
		if len(s.series) >= maxSeries {
			return
		}
		r = sdk.NewRing(historyPoints)
		s.series[key] = r
	}
	r.Add(sdk.Point{T: at.UnixNano(), V: v})
}

// Metrics lists what QuerySeries can answer; it grows as new fields are heard.
func (m *Module) Metrics() []sdk.Metric {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return newCatalogue().metrics
	}
	return slices.Clone(m.state.cat.metrics)
}

// QuerySeries answers from the points recorded since the module started.
func (m *Module) QuerySeries(ctx context.Context, q sdk.SeriesQuery) ([]sdk.Series, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		return nil, nil
	}
	var out []sdk.Series
	for _, ref := range q.Entities {
		for _, name := range q.Metrics {
			key := sdk.SeriesRef{Entity: ref, Metric: name}
			if r := m.state.series[key]; r != nil {
				out = append(out, sdk.Series{Ref: key, Unit: m.state.unit(name), Points: r.In(q.Window)})
			}
		}
	}
	return out, nil
}

// unit is the catalogue's unit for metric.
func (s *state) unit(metric string) sdk.Unit {
	if i := slices.IndexFunc(s.cat.metrics, func(m sdk.Metric) bool { return m.Name == metric }); i >= 0 {
		return s.cat.metrics[i].Unit
	}
	return sdk.UnitNone
}
