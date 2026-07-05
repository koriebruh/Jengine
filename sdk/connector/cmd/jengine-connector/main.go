// Command jengine-connector is the scaffold CLI for third-party
// connector authors (plans/task/core/25): `new` generates a project
// skeleton, `build` compiles it via TinyGo, `test` runs the local
// harness against the built module, `cert-scan` runs the automated
// pre-submission security checklist (the code-facing slice of
// "certification" only - the human marketplace review itself is
// explicitly out of this task's scope).
package main

import (
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "new":
		err = runNew(os.Args[2:])
	case "build":
		err = runBuild(os.Args[2:])
	case "test":
		err = runTest(os.Args[2:])
	case "cert-scan":
		err = runCertScan(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "jengine-connector:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `jengine-connector - scaffold CLI for Jengine third-party connectors

Usage:
  jengine-connector new <name>      Generate a TinyGo project skeleton
  jengine-connector build           Build connector.wasm via TinyGo
  jengine-connector test            Run the local harness against the built module
  jengine-connector cert-scan       Run the automated pre-submission security scan`)
}
