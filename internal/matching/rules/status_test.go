package rules_test

import (
	"testing"

	"github.com/koriebruh/Jengine/internal/matching/rules"
)

func TestIsValidTransition(t *testing.T) {
	cases := []struct {
		from, to rules.RuleStatus
		want     bool
	}{
		{rules.RuleStatusDraft, rules.RuleStatusActive, true},
		{rules.RuleStatusDraft, rules.RuleStatusArchived, true},
		{rules.RuleStatusActive, rules.RuleStatusArchived, true},
		{rules.RuleStatusActive, rules.RuleStatusDraft, false},
		{rules.RuleStatusArchived, rules.RuleStatusActive, false},
		{rules.RuleStatusArchived, rules.RuleStatusDraft, false},
		{rules.RuleStatusDraft, rules.RuleStatusDraft, false},
	}
	for _, c := range cases {
		if got := rules.IsValidTransition(c.from, c.to); got != c.want {
			t.Errorf("IsValidTransition(%s, %s) = %v, want %v", c.from, c.to, got, c.want)
		}
	}
}
