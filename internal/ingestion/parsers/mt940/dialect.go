package mt940

import (
	"regexp"
	"strings"
	"sync"
)

// Dialect customizes how a bank's :86: narrative sub-fields are
// interpreted - plans/task/core/07 explicitly requires this mechanism to
// exist (banks vary in :86: structured-subfield conventions and date
// formats), even though only the generic default and one concrete
// alternate dialect are shipped at this stage.
type Dialect struct {
	Name string
	// ParseNarrative extracts/cleans the :86: block's text. The generic
	// default returns it unchanged; a bank-specific dialect might strip
	// structured "?nn" subfield codes or reorder fields.
	ParseNarrative func(raw string) string
}

// DefaultDialect passes the :86: narrative through unchanged - the safe
// choice when a bank's exact convention isn't known.
var DefaultDialect = Dialect{
	Name:           "generic",
	ParseNarrative: func(raw string) string { return raw },
}

// structuredSubfieldPattern matches SWIFT's common "?nn" structured
// narrative subfield convention, e.g. "?20REF123?32Some text" - used by
// the "structured" example dialect below.
var structuredSubfieldPattern = regexp.MustCompile(`\?\d{2}`)

// StructuredDialect is a concrete second dialect (proving the mechanism
// really works, not just its shape) for banks that prefix :86: subfields
// with SWIFT's "?nn" structured code convention - it strips the codes and
// joins the remaining text with single spaces, so the narrative is
// readable free text rather than raw code-tagged fragments.
var StructuredDialect = Dialect{
	Name: "structured",
	ParseNarrative: func(raw string) string {
		parts := structuredSubfieldPattern.Split(raw, -1)
		var cleaned []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				cleaned = append(cleaned, p)
			}
		}
		return strings.Join(cleaned, " ")
	},
}

var (
	dialectsMu sync.RWMutex
	dialects   = map[string]Dialect{
		DefaultDialect.Name:    DefaultDialect,
		StructuredDialect.Name: StructuredDialect,
	}
)

// RegisterDialect adds or replaces a named dialect - tenants/connectors
// select one via ConnectorConfig.Settings' "dialect" key
// (plans/task/core/07 Implementation Notes).
func RegisterDialect(d Dialect) {
	dialectsMu.Lock()
	defer dialectsMu.Unlock()
	dialects[d.Name] = d
}

// GetDialect looks up a dialect by name, falling back to DefaultDialect
// if name is empty or unregistered - an unrecognized dialect name must
// never fail parsing outright.
func GetDialect(name string) Dialect {
	dialectsMu.RLock()
	defer dialectsMu.RUnlock()
	if d, ok := dialects[name]; ok {
		return d
	}
	return DefaultDialect
}
