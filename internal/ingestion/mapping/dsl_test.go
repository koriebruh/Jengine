package mapping_test

import (
	"testing"

	"github.com/koriebruh/Jengine/internal/ingestion/mapping"
)

func TestParseTransformCall(t *testing.T) {
	cases := []struct {
		raw      string
		wantName string
		wantArg  string
		wantHas  bool
	}{
		{"parse_decimal", "parse_decimal", "", false},
		{"trim", "trim", "", false},
		{"apply_sign_from(field_61.debit_credit_mark)", "apply_sign_from", "field_61.debit_credit_mark", true},
		{`parse_date("YYMMDD")`, "parse_date", "YYMMDD", true},
		{`extract_regex("REF:(\S+)")`, "extract_regex", `REF:(\S+)`, true},
		{"uppercase", "uppercase", "", false},
	}

	for _, c := range cases {
		t.Run(c.raw, func(t *testing.T) {
			got, err := mapping.ParseTransformCall(c.raw)
			if err != nil {
				t.Fatalf("ParseTransformCall(%q) failed: %v", c.raw, err)
			}
			if got.Name != c.wantName {
				t.Errorf("Name: got %q, want %q", got.Name, c.wantName)
			}
			if got.Arg != c.wantArg {
				t.Errorf("Arg: got %q, want %q", got.Arg, c.wantArg)
			}
			if got.HasArg != c.wantHas {
				t.Errorf("HasArg: got %v, want %v", got.HasArg, c.wantHas)
			}
		})
	}
}

func TestParseTransformCall_InvalidSyntax(t *testing.T) {
	_, err := mapping.ParseTransformCall("123invalid(")
	if err == nil {
		t.Fatal("expected an error for invalid transform call syntax")
	}
}

func TestParseSpecYAML_FullExample(t *testing.T) {
	yamlDoc := `
source_format: MT940
mappings:
  - target: transaction.amount
    source: field_61.amount
    transform: [parse_decimal, apply_sign_from(field_61.debit_credit_mark)]
  - target: transaction.currency
    source: field_61.currency
    transform: [uppercase, iso4217_validate]
  - target: transaction.value_date
    source: field_61.value_date
    transform: [parse_date("YYMMDD")]
  - target: transaction.reference
    source: field_86.narrative
    transform: [extract_regex("REF:(\S+)")]
`
	spec, err := mapping.ParseSpecYAML([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("ParseSpecYAML failed: %v", err)
	}
	if spec.SourceFormat != "MT940" {
		t.Errorf("expected source_format MT940, got %q", spec.SourceFormat)
	}
	if len(spec.Mappings) != 4 {
		t.Fatalf("expected 4 mappings, got %d", len(spec.Mappings))
	}
	if spec.Mappings[0].Target != "transaction.amount" || spec.Mappings[0].Source != "field_61.amount" {
		t.Errorf("unexpected first mapping: %+v", spec.Mappings[0])
	}
	if len(spec.Mappings[0].Transform) != 2 {
		t.Fatalf("expected 2 transforms on first mapping, got %d", len(spec.Mappings[0].Transform))
	}
}
