package mqtt

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"wayseer.dev/sdk"
)

// maxBody is the largest inventory read.
const maxBody = 16 << 20

// item is one entry in the inventory, as the source writes it.
type item struct {
	ID        string             `json:"id"`
	Name      string             `json:"name"`    // defaults to the ID
	Kind      string             `json:"kind"`    // defaults to host
	Status    string             `json:"status"`  // ok, warn, crit, down; empty is unknown
	Attrs     map[string]any     `json:"attrs"`   // strings, numbers and booleans
	Metrics   map[string]float64 `json:"metrics"` // this read's value of each catalogue metric
	DependsOn []string           `json:"depends_on"`
}

var levels = map[string]sdk.StatusLevel{
	"": sdk.StatusUnknown, "ok": sdk.StatusOK, "warn": sdk.StatusWarn, "crit": sdk.StatusCrit, "down": sdk.StatusDown,
}

// world is the inventory as entities and edges, keyed as sdk.Tracker wants them, and the
// metric values read with them.
type world struct {
	ents   map[sdk.EntityRef]sdk.Entity
	edges  map[sdk.EdgeKey]sdk.Edge
	points map[sdk.SeriesRef]float64
}

// fetch reads the inventory; errors name the URL but never the token.
func fetch(ctx context.Context, c *http.Client, url string, token sdk.Secret) ([]item, error) {
	resp, err := get(ctx, c, url, token)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var inv struct {
		Items []item `json:"items"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxBody)).Decode(&inv); err != nil {
		return nil, fmt.Errorf("%s: %w", url, err)
	}
	return inv.Items, nil
}

// get requests url with the token, if any, and returns a 200 response.
func get(ctx context.Context, c *http.Client, url string, token sdk.Secret) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, err
	}
	if t := token.Reveal(); t != "" {
		req.Header.Set("Authorization", "Bearer "+t)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%s: %s", url, resp.Status)
	}
	return resp, nil
}

// buildWorld turns items into entities owned by src, with an edge for each dependency found.
func buildWorld(src sdk.ModuleID, items []item) (world, error) {
	w := world{ents: map[sdk.EntityRef]sdk.Entity{}, edges: map[sdk.EdgeKey]sdk.Edge{}, points: map[sdk.SeriesRef]float64{}}
	byID := map[string]sdk.EntityRef{}
	for _, it := range items {
		e, err := entity(src, it)
		if err != nil {
			return world{}, err
		}
		if _, dup := byID[it.ID]; dup {
			return world{}, fmt.Errorf("item %q appears twice", it.ID)
		}
		byID[it.ID] = e.Ref
		w.ents[e.Ref] = e
		for name, v := range it.Metrics {
			if _, ok := inCatalogue(name); !ok {
				return world{}, fmt.Errorf("item %q: metric %q is not in the catalogue", it.ID, name)
			}
			w.points[sdk.SeriesRef{Entity: e.Ref, Metric: name}] = v
		}
	}
	for _, it := range items {
		for _, dep := range it.DependsOn {
			if to, ok := byID[dep]; ok {
				e := sdk.Edge{From: byID[it.ID], To: to, Rel: sdk.RelDependsOn, Weight: 1, Source: src}
				w.edges[sdk.EdgeKey{From: e.From, To: e.To, Rel: e.Rel}] = e
			}
		}
	}
	return w, nil
}

func entity(src sdk.ModuleID, it item) (sdk.Entity, error) {
	kind := sdk.Kind(it.Kind)
	if kind == "" {
		kind = sdk.KindHost
	}
	ref, err := sdk.NewEntityRef(string(src), kind, it.ID)
	if err != nil {
		return sdk.Entity{}, fmt.Errorf("item %q: %w", it.ID, err)
	}
	level, ok := levels[it.Status]
	if !ok {
		return sdk.Entity{}, fmt.Errorf("item %q: status %q is not ok, warn, crit or down", it.ID, it.Status)
	}
	attrs, err := values(it.Attrs)
	if err != nil {
		return sdk.Entity{}, fmt.Errorf("item %q: %w", it.ID, err)
	}
	name := it.Name
	if name == "" {
		name = it.ID
	}
	return sdk.Entity{Ref: ref, Kind: kind, Name: name, Status: sdk.Status{Level: level}, Attrs: attrs, Source: src}, nil
}

// values converts JSON attributes to entity values.
func values(in map[string]any) (map[string]sdk.Value, error) {
	out := make(map[string]sdk.Value, len(in))
	for k, v := range in {
		switch v := v.(type) {
		case string:
			out[k] = sdk.String(v)
		case float64:
			out[k] = sdk.Number(v)
		case bool:
			out[k] = sdk.Bool(v)
		default:
			return nil, fmt.Errorf("attribute %q must be a string, number or boolean", k)
		}
	}
	return out, nil
}
