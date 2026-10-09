package mqtt_test

import (
	"os"
	"slices"
	"strings"
	"testing"

	"wayseer.dev/sdk/manifest"
)

// TestManifestNamesTheModule keeps manifest.yaml as the marketplace wants it: this id, version
// and namespace, no actions, a broker the owner names, and a password that may be in the keyring.
func TestManifestNamesTheModule(t *testing.T) {
	m := readManifest(t)
	if m.ID != "wayseer-labs/mqtt" || m.Namespace != "mqtt" || m.Version != "0.1.0" {
		t.Errorf("manifest.yaml names %s %s in %s", m.ID, m.Version, m.Namespace)
	}
	if len(m.Actions) != 0 || !m.Secrets || !slices.Equal(m.Network, []string{"configured by user"}) {
		t.Errorf("manifest.yaml declares actions %v, secrets %v, network %v", m.Actions, m.Secrets, m.Network)
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
