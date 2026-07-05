// This file exists solely to mark web/ as a separate Go module boundary,
// so `go vet ./...`/`golangci-lint run ./...` from the repo root don't
// descend into web/node_modules - some npm packages (e.g. "flatted")
// vendor a Go implementation alongside their JS one, with no go.mod of
// their own, which Go's tooling would otherwise treat as part of the
// root module's package tree. web/ itself has no Go code; this module
// is never built or imported.
module github.com/koriebruh/Jengine/web

go 1.25.0
