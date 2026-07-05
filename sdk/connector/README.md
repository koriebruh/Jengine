# jengine-connector-sdk

Go SDK for writing third-party Jengine source connectors as sandboxed
WASM guests (plans/task/core/25). See `plans/docs/03-data-ingestion.md`
§3.1 for the design rationale (WASM/TinyGo over native `.so` plugins:
sandboxing untrusted third-party code + no Go-plugin ABI fragility).

## Module layout

This tree is **three independently-versioned Go modules**, not one -
TinyGo 0.34.x requires `go 1.19`-`1.23` in the target module's
`go.mod`, but `wazero >= v1.12.0` (needed to actually execute a WASM
guest) requires `go >= 1.25.0`. Those two constraints cannot coexist in
a single `go.mod`, so the tree is split at the module boundary that
actually needs wazero:

| Module | Path | go directive | Depends on wazero? |
|---|---|---|---|
| `jengine-connector-sdk` | `sdk/connector` (root: `api/`, `wasmguest/`) | `1.23.6` | No - this is what gets compiled BY TinyGo |
| `jengine-connector-sdk/testharness` | `sdk/connector/testharness` | `1.25.0` | Yes - runs compiled `.wasm` guests host-side |
| `jengine-connector-sdk/cmd/jengine-connector` | `sdk/connector/cmd/jengine-connector` | `1.25.0` | Yes (via testharness) |

A connector author's own project only ever depends on the root module
(`api` + `wasmguest`) and gets compiled with TinyGo - it never touches
wazero/testharness directly. The CLI (`jengine-connector`) is what
drives testharness on the author's behalf.

## Installing TinyGo

The scaffold CLI's `build` subcommand shells out to `tinygo build`.
TinyGo is **not** a Go module dependency - it's a separate compiler
toolchain that must be on `PATH`.

If a system package isn't available (no root access, no apt/deb
install path), fetch the official release directly and extract it
without installing:

```sh
curl -LO https://github.com/tinygo-org/tinygo/releases/download/v0.34.0/tinygo_0.34.0_amd64.deb
dpkg-deb -x tinygo_0.34.0_amd64.deb "$HOME/.local/tinygo-extract"
export PATH="$HOME/.local/tinygo-extract/usr/local/bin:$PATH"
```

Add the `export PATH=...` line to your shell profile (`.bashrc`/
`.zshrc`) - a one-off `dpkg-deb -x` extraction doesn't register
anywhere else and won't persist across shells otherwise. Verify with
`tinygo version` (expect `0.34.0`).

## Workflow

```sh
cd sdk/connector/cmd/jengine-connector
go build -o jengine-connector .
./jengine-connector new my-connector
cd my-connector
# edit main.go's Fetch/Validate/SupportsStreaming, manifest.json
../jengine-connector build          # tinygo build -target=wasi
../jengine-connector test --update-golden
../jengine-connector test
../jengine-connector cert-scan      # pre-submission security scan
```

## The one footgun worth knowing before you write `Fetch`

**Do not populate your `Fetch` channel from a spawned goroutine.**
TinyGo's WASI cooperative/asyncify-based goroutine scheduler has been
observed (via direct testing, not a hypothetical) to corrupt guest
state when a spawned goroutine calls back into an imported host
function (`wasmguest.EmitRecord`/`GetSecret`/etc. are all
`//go:wasmimport` calls) - it doesn't crash where the actual problem
is; it surfaces later as an unrelated nil-map panic inside
`encoding/json`. Fill your channel synchronously, before `Fetch`
returns:

```go
func (c *myConnector) Fetch(ctx context.Context, cfg api.ConnectorConfig) (<-chan api.RawRecord, error) {
    ch := make(chan api.RawRecord, 1)
    ch <- api.RawRecord{Payload: []byte(`{"example":"record"}`)}
    close(ch)
    return ch, nil
}
```

The scaffold template (`cmd/jengine-connector new`) already does this
correctly - this note is for anyone hand-rolling a `Fetch` outside the
template. See `QA_REPORT.md`'s own entry on this for the full
diagnosis.
