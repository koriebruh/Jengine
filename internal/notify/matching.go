package notify

import (
	"log/slog"

	"github.com/koriebruh/Jengine/internal/domain"
)

// MatchingSubscriptions filters candidates (already event-type-scoped
// via domain.WebhookSubscriptionRepository.ListActiveByEventType - see
// that method's own doc comment) down to those whose FilterExpr (if
// any) matches payload. A malformed FilterExpr fails open to "no match"
// for that one subscription (logged, not propagated) rather than
// aborting delivery to every OTHER matching subscription over one
// tenant's bad filter syntax.
func MatchingSubscriptions(candidates []domain.WebhookSubscription, payload []byte) []domain.WebhookSubscription {
	var matched []domain.WebhookSubscription
	for _, sub := range candidates {
		filterExpr := ""
		if sub.FilterExpr != nil {
			filterExpr = *sub.FilterExpr
		}
		ok, err := MatchesFilter(filterExpr, payload)
		if err != nil {
			slog.Warn("notify: filter expression evaluation failed, skipping subscription", "subscription_id", sub.ID, "filter_expr", filterExpr, "error", err)
			continue
		}
		if ok {
			matched = append(matched, sub)
		}
	}
	return matched
}
