package mapping

import _ "embed"

// MT940DefaultSpecYAML and CSVDefaultSpecYAML are the exact fixture
// mapping specs from testdata/, exported so callers outside this
// package's own tests (e.g. cmd/ingestion-gateway's manual-verification
// seed flow, plans/task/core/08 DoD) can seed a real MappingSpec without
// duplicating the YAML inline or depending on a relative testdata path.

//go:embed testdata/mt940_default.yaml
var MT940DefaultSpecYAML []byte

//go:embed testdata/csv_default.yaml
var CSVDefaultSpecYAML []byte
