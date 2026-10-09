# Wayseer module template

A complete module for Wayseer Desktop to start your own from. It reads a JSON inventory from a
URL, turns each item into an entity, links items that depend on each other, records their
metrics, and turns status changes into events. It passes the SDK's conformance suite, runs as
its own program, and signs into a package the app installs.

## Start

Make your copy with GitHub's "Use this template", or with `gonew`, which also renames the Go
module:

```
go run golang.org/x/tools/cmd/gonew@latest github.com/wayseer-net/desktop-module-template example.com/widget
```

Then rename what is still called `inventory`:

1. The package in every `.go` file, and `Kind` in `module.go`.
2. `cmd/wayseer-inventory`, and `PROGRAM` in the `Makefile` to match.
3. In `manifest.yaml`: `id`, `name`, `description` and `namespace`. The `id` starts with your
   developer certificate's namespace. The `namespace` is your certificate's for a package you
   sign; for the marketplace, it is the one Wayseer allocates to the module.

Check that the copy works before you change it:

```
make check
```

Wayseer's user guide, under "Writing a module", walks through the files and how to adapt them
to your source.

| File | Holds |
|---|---|
| `doc.go` | What the module reads, for `go doc`. |
| `options.go` | The options, their defaults and their checks. |
| `module.go` | `Info`, `Configure`, `Run`, `Health` and `Discover`: the lifecycle. |
| `inventory.go` | Fetching the source and turning it into entities and edges. |
| `series.go` | The metric catalogue and `QuerySeries`. |
| `actions.go` | One example action. |
| `module_test.go` | The conformance suite against an `httptest` server, and the module's own tests. |
| `cmd/wayseer-inventory` | The program that serves the module to the app. |
| `manifest.yaml` | What the module is and may do, for its signed package. |

## Sign a package

You need Wayseer installed, and a developer key and certificate (the guide's "Getting a
developer certificate"):

```
export WAYSEER_DEV_KEY=~/.config/wayseer/dev.pem
export WAYSEER_DEV_CERT=~/.config/wayseer/dev.cert
make sign
```

`make sign` builds the program and runs `wayseer dev sign`, which writes the package to
`dist/`. For another platform, set `GOOS` and `GOARCH`. Raise `version` in `manifest.yaml`
before signing again; `dev sign` never overwrites a package.

## Working on it

```
make check   # what CI runs: tests with the conformance suite, vet and lint for every platform, a key scan
make help    # every target
```

The module imports only the SDK (`wayseer.dev/sdk`), the standard library and its own
dependencies; `TestImportsOnlyTheSDK` keeps it that way. golangci-lint is pinned in
`tools/go.mod`, and gitleaks runs at a pinned version through `go run`.

`TestMakeSignPackagesTheModule` signs a package with throwaway keys. It needs Wayseer's own
source beside this folder, in `../core`, so it skips in your copy; delete it if you like.

To change the module alongside the SDK, use a Go workspace: `go.work` here with
`use . ../sdk`. `go.work` is ignored by git.

## Licence

MIT No Attribution; see `LICENSE`. Your copy is yours to license as you choose.
