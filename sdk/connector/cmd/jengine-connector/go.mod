module github.com/koriebruh/jengine-connector-sdk/cmd/jengine-connector

go 1.25.0

require github.com/koriebruh/jengine-connector-sdk/testharness v0.0.0

require (
	github.com/tetratelabs/wazero v1.12.0 // indirect
	golang.org/x/sys v0.44.0 // indirect
)

replace github.com/koriebruh/jengine-connector-sdk/testharness => ../../testharness
