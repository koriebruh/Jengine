// Package notify implements plans/task/core/21's outbound webhook
// support: the event catalog, HMAC signing, subscription matching/
// filtering, and retry-backoff schedule. cmd/webhook-dispatcher is the
// one consumer of this package for actual delivery; internal/apiserver's
// WebhookServiceHandler uses it for subscription validation.
package notify

// EventCatalog lists every cataloged event type
// (plans/docs/07-api-extensibility.md §8.2) a webhook subscription may
// register for. match.reconciliation_variance is an intentional
// EXTENSION beyond the doc's own (non-exhaustive, "etc.") list - a
// direct consequence of plans/task/core/19's RECONCILIATION_VARIANCE
// break type needing a delivery event, documented here rather than
// silently added.
const (
	EventTransactionIngested        = "transaction.ingested"
	EventMatchFound                 = "match.found"
	EventMatchAutoConfirmed         = "match.auto_confirmed"
	EventMatchReconciliationVariant = "match.reconciliation_variance" // extension, see doc comment above
	EventBreakCreated               = "break.created"
	EventBreakAssigned              = "break.assigned"
	EventBreakSLAWarning            = "break.sla_warning"
	EventBreakSLABreached           = "break.sla_breached"
	EventBreakResolved              = "break.resolved"
	EventCaseApprovalRequested      = "case.approval_requested"
	EventRuleActivated              = "rule.activated"
)

// EventCatalog is every event type above, for validation (e.g.
// rejecting a subscription request naming an unknown event type).
var EventCatalog = []string{
	EventTransactionIngested, EventMatchFound, EventMatchAutoConfirmed, EventMatchReconciliationVariant,
	EventBreakCreated, EventBreakAssigned, EventBreakSLAWarning, EventBreakSLABreached, EventBreakResolved,
	EventCaseApprovalRequested, EventRuleActivated,
}

// IsCataloged reports whether eventType is a recognized catalog entry.
func IsCataloged(eventType string) bool {
	for _, e := range EventCatalog {
		if e == eventType {
			return true
		}
	}
	return false
}
