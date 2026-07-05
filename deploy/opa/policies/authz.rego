package jengine.authz

# plans/task/core/23 - real authorization policy, not a placeholder.
# input shape (see internal/platform/authz.OPAInput):
#   input.subject: {user_id, tenant_id, roles: [...], business_unit}
#   input.action: e.g. "case.approve", "case.act", "rule.activate"
#   input.resource: {entity_type, entity_id, tenant_id, account_business_unit, maker_user_id}
#
# default-deny: every action is rejected unless an explicit `allow`
# rule matches - the only way something becomes permitted is a rule
# below, never an absence of a deny.
default allow := false

# --- read-only roles never get a mutating allow rule at all ---
# Auditor/Read-Only is deliberately absent from every `allow` rule in
# this file rather than special-cased with a `deny` - plans/task/core/23's
# own DoD calls out "auditor role cannot perform any mutating action"
# as an edge case to verify, which a default-deny policy satisfies for
# free as long as no rule below ever grants auditor an allow.

# Approver cannot approve their own submitted maker action (maker !=
# checker, enforced at the policy layer - plans/docs/09's ABAC example).
allow if {
	input.action == "case.approve"
	some role in input.subject.roles
	role == "approver"
	input.resource.maker_user_id != input.subject.user_id
}

# Tenant Admin and Recon Manager may also approve (not just the
# dedicated Approver role) - still subject to the same maker != checker
# guard, since a Tenant Admin who filed the write-off request can't
# rubber-stamp their own submission either.
allow if {
	input.action == "case.approve"
	some role in input.subject.roles
	role == "tenant_admin"
	input.resource.maker_user_id != input.subject.user_id
}

allow if {
	input.action == "case.approve"
	some role in input.subject.roles
	role == "recon_manager"
	input.resource.maker_user_id != input.subject.user_id
}

# Analyst can only act on breaks for accounts in their own business
# unit (ABAC scoping - plans/docs/09's other named example).
allow if {
	input.action == "case.act"
	some role in input.subject.roles
	role == "analyst"
	input.resource.account_business_unit == input.subject.business_unit
}

# Recon Manager and Tenant Admin can act on any break regardless of
# business unit - broader role, not scoped the way a line analyst is.
allow if {
	input.action == "case.act"
	some role in input.subject.roles
	role == "recon_manager"
}

allow if {
	input.action == "case.act"
	some role in input.subject.roles
	role == "tenant_admin"
}

# Rule activation (rule.activate) - maker/checker applies here too:
# rule changes are financially consequential (plans/docs/05 §6.4's own
# framing: "a bad rule change can silently misreconcile millions").
allow if {
	input.action == "rule.activate"
	some role in input.subject.roles
	role == "tenant_admin"
	input.resource.maker_user_id != input.subject.user_id
}

allow if {
	input.action == "rule.activate"
	some role in input.subject.roles
	role == "recon_manager"
	input.resource.maker_user_id != input.subject.user_id
}
