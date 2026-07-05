package jengine.authz

import rego.v1

# --- case.approve: maker != checker ---

test_approver_can_approve_others_submission if {
	allow with input as {
		"subject": {"user_id": "approver-1", "tenant_id": "t1", "roles": ["approver"], "business_unit": "bu1"},
		"action": "case.approve",
		"resource": {"maker_user_id": "maker-1"},
	}
}

test_approver_cannot_approve_own_submission if {
	not allow with input as {
		"subject": {"user_id": "same-user", "tenant_id": "t1", "roles": ["approver"], "business_unit": "bu1"},
		"action": "case.approve",
		"resource": {"maker_user_id": "same-user"},
	}
}

test_tenant_admin_cannot_approve_own_submission if {
	not allow with input as {
		"subject": {"user_id": "admin-1", "tenant_id": "t1", "roles": ["tenant_admin"], "business_unit": "bu1"},
		"action": "case.approve",
		"resource": {"maker_user_id": "admin-1"},
	}
}

test_analyst_cannot_approve_at_all if {
	not allow with input as {
		"subject": {"user_id": "analyst-1", "tenant_id": "t1", "roles": ["analyst"], "business_unit": "bu1"},
		"action": "case.approve",
		"resource": {"maker_user_id": "maker-1"},
	}
}

# --- case.act: analyst scoped to own business unit ---

test_analyst_can_act_within_own_business_unit if {
	allow with input as {
		"subject": {"user_id": "analyst-1", "tenant_id": "t1", "roles": ["analyst"], "business_unit": "bu1"},
		"action": "case.act",
		"resource": {"account_business_unit": "bu1"},
	}
}

test_analyst_cannot_act_outside_own_business_unit if {
	not allow with input as {
		"subject": {"user_id": "analyst-1", "tenant_id": "t1", "roles": ["analyst"], "business_unit": "bu1"},
		"action": "case.act",
		"resource": {"account_business_unit": "bu2"},
	}
}

test_recon_manager_can_act_across_business_units if {
	allow with input as {
		"subject": {"user_id": "rm-1", "tenant_id": "t1", "roles": ["recon_manager"], "business_unit": "bu1"},
		"action": "case.act",
		"resource": {"account_business_unit": "bu2"},
	}
}

# --- auditor: read-only, never a mutating allow ---

test_auditor_cannot_approve if {
	not allow with input as {
		"subject": {"user_id": "auditor-1", "tenant_id": "t1", "roles": ["auditor"], "business_unit": "bu1"},
		"action": "case.approve",
		"resource": {"maker_user_id": "maker-1"},
	}
}

test_auditor_cannot_act if {
	not allow with input as {
		"subject": {"user_id": "auditor-1", "tenant_id": "t1", "roles": ["auditor"], "business_unit": "bu1"},
		"action": "case.act",
		"resource": {"account_business_unit": "bu1"},
	}
}

test_auditor_cannot_activate_rule if {
	not allow with input as {
		"subject": {"user_id": "auditor-1", "tenant_id": "t1", "roles": ["auditor"], "business_unit": "bu1"},
		"action": "rule.activate",
		"resource": {"maker_user_id": "maker-1"},
	}
}

# --- rule.activate: maker != checker applies here too ---

test_tenant_admin_can_activate_others_rule if {
	allow with input as {
		"subject": {"user_id": "admin-1", "tenant_id": "t1", "roles": ["tenant_admin"], "business_unit": "bu1"},
		"action": "rule.activate",
		"resource": {"maker_user_id": "author-1"},
	}
}

test_tenant_admin_cannot_activate_own_rule if {
	not allow with input as {
		"subject": {"user_id": "admin-1", "tenant_id": "t1", "roles": ["tenant_admin"], "business_unit": "bu1"},
		"action": "rule.activate",
		"resource": {"maker_user_id": "admin-1"},
	}
}

# --- default-deny: unrecognized action never allowed ---

test_unrecognized_action_denied_by_default if {
	not allow with input as {
		"subject": {"user_id": "admin-1", "tenant_id": "t1", "roles": ["tenant_admin"], "business_unit": "bu1"},
		"action": "some.unmodeled.action",
		"resource": {},
	}
}
