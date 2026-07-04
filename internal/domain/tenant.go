package domain

import "github.com/koriebruh/Jengine/internal/tenancy"

// Tenant is an alias for tenancy.Tenant, not a second struct - see
// plans/task/core/05 Common Pitfalls: duplicating this type here would
// let the two definitions drift apart. internal/tenancy owns the Tenant
// Registry; this package only needs to refer to the same type.
type Tenant = tenancy.Tenant
