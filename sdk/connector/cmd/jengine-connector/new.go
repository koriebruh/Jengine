package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func runNew(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: jengine-connector new <name>")
	}
	name := args[0]

	if _, err := os.Stat(name); err == nil {
		return fmt.Errorf("directory %q already exists", name)
	}
	if err := os.MkdirAll(filepath.Join(name, "testdata"), 0o755); err != nil {
		return fmt.Errorf("create project directory: %w", err)
	}

	files := map[string]string{
		"go.mod":                        fmt.Sprintf(goModTemplate, name),
		"main.go":                       mainGoTemplate,
		"manifest.json":                 manifestTemplate,
		"README.md":                     fmt.Sprintf(readmeTemplate, name),
		"testdata/sample_input.json":    sampleInputTemplate,
		"testdata/expected_output.json": expectedOutputTemplate,
	}
	for path, content := range files {
		full := filepath.Join(name, path)
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", full, err)
		}
	}

	fmt.Printf("created %s/\n", name)
	fmt.Println("next steps:")
	fmt.Println("  1. edit main.go's Fetch/Validate/SupportsStreaming implementation")
	fmt.Println("  2. edit manifest.json to declare any secret keys/egress domains you need")
	fmt.Println("  3. cd " + name + " && jengine-connector build")
	fmt.Println("  4. jengine-connector test")
	return nil
}

const goModTemplate = `module %s

// TinyGo 0.34.x requires go 1.19-1.23 - keep this directive within that
// range even as the toolchain advances elsewhere, or ` + "`tinygo build`" + `
// rejects the module outright ("requires go version 1.19 through 1.23").
go 1.23.6

require github.com/koriebruh/jengine-connector-sdk v0.0.0

// Point this at wherever you vendored/cloned the SDK until it has a
// tagged release - see the SDK's own README for the current recommended
// pinning approach.
replace github.com/koriebruh/jengine-connector-sdk => ../..
`

const mainGoTemplate = `package main

import (
	"context"

	"github.com/koriebruh/jengine-connector-sdk/api"
	"github.com/koriebruh/jengine-connector-sdk/wasmguest"
)

// myConnector implements api.SourceConnector - replace this with your
// actual source integration.
type myConnector struct{}

// Fetch fills and returns a buffered channel SYNCHRONOUSLY - do not
// spawn a goroutine to populate it. TinyGo's WASI target uses a
// cooperative, asyncify-based goroutine scheduler with real gaps
// around a spawned goroutine calling back into an imported host
// function (this SDK's wasmguest.EmitRecord/GetSecret/etc. are all
// //go:wasmimport calls) - doing so has been observed to corrupt the
// guest runtime and panic with a nil-map fault deep in unrelated code
// (found via direct testing, not a hypothetical). Populate your
// channel directly in this function's own goroutine before returning
// it; if you need concurrent I/O internally, complete it before
// sending to ch.
func (c *myConnector) Fetch(ctx context.Context, cfg api.ConnectorConfig) (<-chan api.RawRecord, error) {
	ch := make(chan api.RawRecord, 1)
	// TODO: replace with your real fetch logic. Use
	// wasmguest.GetSecret/HTTPFetch for anything needing a declared
	// secret key or egress domain (see manifest.json).
	ch <- api.RawRecord{Payload: []byte(` + "`" + `{"example":"record"}` + "`" + `)}
	close(ch)
	return ch, nil
}

func (c *myConnector) Validate(cfg api.ConnectorConfig) error {
	// TODO: validate cfg.Settings.
	return nil
}

func (c *myConnector) SupportsStreaming() bool { return false }

func (c *myConnector) Checkpoint() (api.Cursor, error) {
	cursor, ok := wasmguest.CheckpointLoad()
	if !ok {
		return api.Cursor{}, nil
	}
	return api.Cursor{State: cursor}, nil
}

func main() {
	wasmguest.Register(&myConnector{})
}
`

const manifestTemplate = `{
  "name": "my-connector",
  "version": "0.1.0",
  "allowed_secret_keys": [],
  "allowed_egress_domains": []
}
`

const readmeTemplate = `# %s

A Jengine third-party source connector, scaffolded by jengine-connector.

## Build

	jengine-connector build

## Test

	jengine-connector test

## Security scan (pre-submission)

	jengine-connector cert-scan

See manifest.json to declare any secret keys or egress domains your
connector needs - undeclared keys/domains are denied at runtime, not
silently allowed.
`

const sampleInputTemplate = `{"note": "replace with a real sample raw payload for your source format"}
`

const expectedOutputTemplate = `[]
`
