package main

import (
	"fmt"
	"os"
	"os/exec"
)

// runBuild shells out to TinyGo (plans/task/core/25's own explicit
// toolchain choice for guest connectors - §3.1: sandboxing + smaller,
// more portable binaries than stock Go's wasip1 output). Does not
// vendor or reimplement TinyGo itself; requires it on PATH.
func runBuild(args []string) error {
	if _, err := exec.LookPath("tinygo"); err != nil {
		return fmt.Errorf("tinygo not found on PATH - install from https://tinygo.org/getting-started/install/ : %w", err)
	}

	cmd := exec.Command("tinygo", "build", "-o", "connector.wasm", "-target=wasi", ".")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("tinygo build failed: %w", err)
	}
	fmt.Println("built connector.wasm")
	return nil
}
