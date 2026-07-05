package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/koriebruh/jengine-connector-sdk/testharness"
)

// runTest runs the built connector.wasm through testharness against
// testdata/sample_input.json, comparing emitted records against
// testdata/expected_output.json (golden-file style) - or writes the
// golden fixture if --update-golden is passed (a first-time author
// establishing their initial expected output rather than hand-authoring
// it).
func runTest(args []string) error {
	fs := flag.NewFlagSet("test", flag.ExitOnError)
	updateGolden := fs.Bool("update-golden", false, "write testdata/expected_output.json from this run's actual output instead of comparing")
	wasmPath := fs.String("wasm", "connector.wasm", "path to the built WASM module")
	inputPath := fs.String("input", "testdata/sample_input.json", "path to the sample input config")
	goldenPath := fs.String("golden", "testdata/expected_output.json", "path to the golden fixture")
	if err := fs.Parse(args); err != nil {
		return err
	}

	wasmBytes, err := os.ReadFile(*wasmPath)
	if err != nil {
		return fmt.Errorf("read %s (did you run `jengine-connector build`?): %w", *wasmPath, err)
	}
	input, err := os.ReadFile(*inputPath)
	if err != nil {
		return fmt.Errorf("read %s: %w", *inputPath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	h, err := testharness.New(ctx, wasmBytes, testharness.Options{Timeout: 10 * time.Second})
	if err != nil {
		return fmt.Errorf("load connector: %w", err)
	}
	defer func() { _ = h.Close(ctx) }()

	if err := h.Validate(ctx, input); err != nil {
		return fmt.Errorf("Validate failed: %w", err)
	}

	records, err := h.Fetch(ctx, input)
	if err != nil {
		return fmt.Errorf("Fetch failed: %w", err)
	}
	fmt.Printf("Fetch emitted %d record(s)\n", len(records))

	if *updateGolden {
		if err := testharness.WriteGolden(records, *goldenPath); err != nil {
			return err
		}
		fmt.Printf("wrote golden fixture to %s\n", *goldenPath)
		return nil
	}

	if err := testharness.CompareGolden(records, *goldenPath); err != nil {
		return err
	}
	fmt.Println("PASS: emitted records match golden fixture")
	return nil
}
