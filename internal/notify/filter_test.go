package notify_test

import (
	"testing"

	"github.com/koriebruh/Jengine/internal/notify"
)

func TestMatchesFilter(t *testing.T) {
	payload := []byte(`{"amount_at_risk": 75000, "currency": "USD", "priority": "HIGH"}`)

	tests := []struct {
		name    string
		expr    string
		want    bool
		wantErr bool
	}{
		{"empty filter always matches", "", true, false},
		{"numeric greater-than true", "amount_at_risk > 50000", true, false},
		{"numeric greater-than false", "amount_at_risk > 100000", false, false},
		{"numeric greater-or-equal boundary", "amount_at_risk >= 75000", true, false},
		{"numeric less-than", "amount_at_risk < 50000", false, false},
		{"string equality true", "currency == USD", true, false},
		{"string equality false", "currency == EUR", false, false},
		{"string inequality true", "priority != LOW", true, false},
		{"field absent from payload", "nonexistent_field > 1", false, false},
		{"unparseable expression", "amount_at_risk ??? 5", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := notify.MatchesFilter(tt.expr, payload)
			if (err != nil) != tt.wantErr {
				t.Fatalf("MatchesFilter() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("MatchesFilter(%q) = %v, want %v", tt.expr, got, tt.want)
			}
		})
	}
}
