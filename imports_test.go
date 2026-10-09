package mqtt_test

import (
	"os/exec"
	"strings"
	"testing"
)

// TestImportsOnlyTheSDK keeps the module free of the app: of Wayseer's code it links only the SDK.
func TestImportsOnlyTheSDK(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-test", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, pkg := range strings.Fields(string(out)) {
		if strings.HasPrefix(pkg, "wayseer/") || strings.HasPrefix(pkg, "wayseer.dev/") && !strings.HasPrefix(pkg, "wayseer.dev/sdk") {
			t.Errorf("the module imports %s", pkg)
		}
	}
}
