package mapping

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// MappingSpec is a tenant-configurable, versioned field-mapping spec -
// the exact YAML shape from plans/docs/02-data-ingestion.md §3.2 (do not
// invent a different shape - plans/task/core/08 Common Pitfalls).
type MappingSpec struct {
	SourceFormat string         `yaml:"source_format" json:"source_format"`
	Version      int            `yaml:"version" json:"version"`
	Mappings     []FieldMapping `yaml:"mappings" json:"mappings"`
}

// FieldMapping maps one source field to one canonical target field
// through an ordered transform chain.
type FieldMapping struct {
	Target    string   `yaml:"target" json:"target"`
	Source    string   `yaml:"source" json:"source"`
	Transform []string `yaml:"transform" json:"transform"`
}

// TransformCall is one parsed entry from a FieldMapping's Transform list:
// a function name plus an optional single argument, e.g.
// "parse_date(\"YYMMDD\")" -> Name: "parse_date", Arg: "YYMMDD", HasArg:
// true; "apply_sign_from(field_61.debit_credit_mark)" -> Name:
// "apply_sign_from", Arg: "field_61.debit_credit_mark" (a field
// reference, not a literal - resolved against the record at apply time,
// not here); "trim" (no parens) -> Name: "trim", HasArg: false.
type TransformCall struct {
	Name   string
	Arg    string
	HasArg bool
}

// callPattern matches "name" or "name(arg)", where arg is either a
// "quoted string" or a bare identifier/field-reference like
// field_61.debit_credit_mark. Deliberately narrow - plans/task/core/08
// Non-Goals forbids a general expression-language grammar here.
var callPattern = regexp.MustCompile(`^([a-zA-Z_][a-zA-Z0-9_]*)(?:\((?:"([^"]*)"|([^)]*))\))?$`)

// ParseTransformCall parses one Transform list entry into a TransformCall.
func ParseTransformCall(raw string) (TransformCall, error) {
	raw = strings.TrimSpace(raw)
	m := callPattern.FindStringSubmatch(raw)
	if m == nil {
		return TransformCall{}, fmt.Errorf("mapping: invalid transform call syntax %q", raw)
	}

	name := m[1]
	quotedArg, bareArg := m[2], m[3]

	// Distinguish "name()" (empty parens, no real arg) from "name" (no
	// parens at all) - both parse fine here, neither has a usable arg.
	hasParens := strings.Contains(raw, "(")
	switch {
	case quotedArg != "":
		return TransformCall{Name: name, Arg: quotedArg, HasArg: true}, nil
	case bareArg != "":
		return TransformCall{Name: name, Arg: strings.TrimSpace(bareArg), HasArg: true}, nil
	case hasParens:
		return TransformCall{Name: name, HasArg: false}, nil
	default:
		return TransformCall{Name: name, HasArg: false}, nil
	}
}

// ParseSpecYAML parses a MappingSpec from YAML bytes.
func ParseSpecYAML(data []byte) (MappingSpec, error) {
	var spec MappingSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return spec, fmt.Errorf("mapping: parse yaml: %w", err)
	}
	return spec, nil
}
