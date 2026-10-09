package mqtt_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	mqtt "github.com/wayseer-net/desktop-module-mqtt"
	"wayseer.dev/sdk/manifest"
)

// TestManifestDeclaresEveryAction keeps manifest.yaml in step with Actions: Wayseer offers a
// packaged module's action only as its manifest declares it, and the marketplace refuses one
// that offers more.
func TestManifestDeclaresEveryAction(t *testing.T) {
	m := readManifest(t)
	for _, a := range mqtt.New().Actions() {
		i := slices.IndexFunc(m.Actions, func(d manifest.Action) bool { return d.ID == a.ID })
		if i < 0 {
			t.Errorf("manifest.yaml doesn't declare %s", a.ID)
			continue
		}
		var kinds []string
		for _, k := range a.Kinds {
			kinds = append(kinds, string(k))
		}
		if d := m.Actions[i]; !slices.Equal(d.Kinds, kinds) || d.Title != a.Title || d.Changes != a.Changes {
			t.Errorf("manifest.yaml declares %s as %+v; Actions offers %+v", a.ID, d, a)
		}
	}
	if len(m.Actions) != len(mqtt.New().Actions()) {
		t.Errorf("manifest.yaml declares %d actions; Actions offers %d", len(m.Actions), len(mqtt.New().Actions()))
	}
}

// readManifest reads manifest.yaml as dev sign does, filling the fields it writes.
func readManifest(t *testing.T) manifest.Manifest {
	t.Helper()
	data, err := os.ReadFile("manifest.yaml")
	if err != nil {
		t.Fatal(err)
	}
	_, m, err := manifest.Fill(data, manifest.Platform{OS: "linux", Arch: "amd64", SHA256: strings.Repeat("0", 64)})
	if err != nil {
		t.Fatal(err)
	}
	return m
}
