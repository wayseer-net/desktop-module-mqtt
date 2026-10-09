package mqtt

import (
	"context"
	"fmt"
	"slices"

	"wayseer.dev/sdk"
)

// Actions offers one harmless example, recheck; replace it with the module's own, or delete
// this file. Nothing is offered until the owner names an action for the instance in config,
// and out of process only an action manifest.yaml lists, on kinds it lists.
func (m *Module) Actions() []sdk.Action {
	return []sdk.Action{{
		ID: "recheck", Title: "Recheck", Kinds: []sdk.Kind{sdk.KindHost, sdk.KindService, sdk.KindDatabase},
		Changes: "Nothing: it reads the item from the inventory again and says its status",
	}}
}

// Do runs an action the app has checked and the owner confirmed. It checks the request again,
// as every module should, and stops when ctx ends.
func (m *Module) Do(ctx context.Context, req sdk.ActionRequest) (sdk.ActionResult, error) {
	acts := m.Actions()
	i := slices.IndexFunc(acts, func(a sdk.Action) bool { return a.ID == req.Action })
	if i < 0 || !slices.Contains(acts[i].Kinds, req.Entity.Kind()) {
		return sdk.ActionResult{}, fmt.Errorf("no action %q on a %s", req.Action, req.Entity.Kind())
	}
	if _, err := acts[i].Check(req.Params); err != nil {
		return sdk.ActionResult{}, err
	}
	w, err := m.read(ctx)
	if err != nil {
		return sdk.ActionResult{}, err
	}
	e, ok := w.ents[req.Entity]
	if !ok {
		return sdk.ActionResult{}, fmt.Errorf("%s is no longer in the inventory", req.Entity.Native())
	}
	return sdk.ActionResult{Message: fmt.Sprintf("%s is %s", e.Name, e.Status.Level)}, nil
}
