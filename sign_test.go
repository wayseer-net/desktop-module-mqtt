package mqtt_test

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"wayseer.dev/sdk/manifest"
	"wayseer.dev/sdk/wsmod"
)

// TestMakeSignPackagesTheModule runs make sign with a developer key and certificate made from
// throwaway keys, and reads back the package wayseer dev sign wrote.
func TestMakeSignPackagesTheModule(t *testing.T) {
	core := coreCheckout(t)
	tmp := t.TempDir()
	wayseer := build(t, core, "./cmd/wayseer", tmp)
	licenser := build(t, core, "./scripts/wayseer-license", tmp)
	key, cert := testIdentity(t, wayseer, licenser, tmp)

	dist := filepath.Join(tmp, "dist")
	cmd := exec.Command("make", "-s", "sign", "WAYSEER="+wayseer, "DIST="+dist)
	cmd.Env = append(isolated(t, tmp), "WAYSEER_DEV_KEY="+key, "WAYSEER_DEV_CERT="+cert)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("make sign: %v\n%s", err, out)
	}

	pkgs, _ := filepath.Glob(filepath.Join(dist, "*.wsmod.*"))
	if len(pkgs) != 1 {
		t.Fatalf("make sign wrote %v, want one package", pkgs)
	}
	m := readPackage(t, pkgs[0])
	if m.ID != "wayseer-labs/mqtt" || m.OS != runtime.GOOS || m.Arch != runtime.GOARCH {
		t.Errorf("the package's manifest is %s for %s/%s", m.ID, m.OS, m.Arch)
	}
}

// coreCheckout is Wayseer's own source beside this repository, which only Wayseer has; without
// it the test skips.
func coreCheckout(t *testing.T) string {
	t.Helper()
	core, err := filepath.Abs("../core")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(core, "cmd", "wayseer")); err != nil {
		t.Skip("no Wayseer source in ../core to build the app and test keys from")
	}
	return core
}

// build compiles pkg from the core into dir and returns the program's path.
func build(t *testing.T, core, pkg, dir string) string {
	t.Helper()
	exe := filepath.Join(dir, filepath.Base(pkg))
	if runtime.GOOS == "windows" {
		exe += ".exe"
	}
	if out, err := exec.Command("go", "build", "-C", core, "-o", exe, pkg).CombinedOutput(); err != nil {
		t.Fatalf("building %s: %v\n%s", pkg, err, out)
	}
	return exe
}

// testIdentity makes a throwaway root, a developer signing key it certifies, and a developer
// key with a certificate for the manifest's namespace, mqtt; it returns the key's and the cert's paths.
func testIdentity(t *testing.T, wayseer, licenser, dir string) (key, cert string) {
	t.Helper()
	root, signer := filepath.Join(dir, "root.key"), filepath.Join(dir, "signer.key")
	key, cert, pub := filepath.Join(dir, "dev.key"), filepath.Join(dir, "dev.cert"), filepath.Join(dir, "dev.pub")
	run(t, dir, licenser, "keygen", root)
	run(t, dir, licenser, "mint", "-purpose", "developer", signer)
	out := run(t, dir, wayseer, "dev", "keygen", key)
	m := regexp.MustCompile(`(?m)^public key: (\S+)$`).FindSubmatch(out)
	if m == nil {
		t.Fatalf("dev keygen printed no public key")
	}
	if err := os.WriteFile(pub, m[1], 0o600); err != nil {
		t.Fatal(err)
	}
	run(t, dir, licenser, "dev-cert", "-licence-id", "L-0001", "-namespace", "mqtt", "-pub", pub, cert)
	return key, cert
}

// run runs a program with the test's keys and a home of its own, and returns its output.
func run(t *testing.T, dir, exe string, args ...string) []byte {
	t.Helper()
	cmd := exec.Command(exe, args...)
	cmd.Env = append(isolated(t, dir),
		"WAYSEER_LICENSE_ROOT_KEY="+filepath.Join(dir, "root.key"),
		"WAYSEER_LICENSE_SIGNING_KEY="+filepath.Join(dir, "signer.key"))
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("%s %s: %v", filepath.Base(exe), args[0], err)
	}
	return out
}

// isolated is the environment with every home and config folder moved under dir, keeping Go's
// own caches where they are.
func isolated(t *testing.T, dir string) []string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOPATH", "GOCACHE", "GOMODCACHE").Output()
	if err != nil {
		t.Fatal(err)
	}
	paths := strings.Fields(string(out))
	if len(paths) != 3 {
		t.Fatalf("go env printed %q", out)
	}
	home := filepath.Join(dir, "home")
	return append(os.Environ(), "HOME="+home, "USERPROFILE="+home, "XDG_CONFIG_HOME="+home,
		"XDG_STATE_HOME="+home, "XDG_DATA_HOME="+home, "APPDATA="+home, "LOCALAPPDATA="+home,
		"GOPATH="+paths[0], "GOCACHE="+paths[1], "GOMODCACHE="+paths[2])
}

// readPackage reads a signed package and returns its manifest.
func readPackage(t *testing.T, path string) manifest.Manifest {
	t.Helper()
	format, err := wsmod.FormatOf(path)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	p, err := wsmod.Read(bytes.NewReader(b), format)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Module) == 0 || len(p.Signature) == 0 {
		t.Fatal("the package has no program or no signature")
	}
	m, err := manifest.Parse(p.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
