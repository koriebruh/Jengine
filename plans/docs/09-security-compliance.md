> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [08-storage-architecture.md](08-storage-architecture.md)

# 09 — Audit, Compliance & Security

## 10.1 Immutable Audit Log Design
- `AuditEvent` table append-only at application layer (no UPDATE/DELETE grants for app DB role — Postgres role permission enforcement, not just convention), **hash-chained**: each event's `hash_chain_prev` links to previous event's `SHA-256(payload + prev_hash)` — retroactive modification breaks the chain, detectable via periodic verification job.
- Streamed via CDC to WORM object-storage archive near-real-time — even full DB compromise doesn't destroy historical record; WORM copy is ultimate source of truth.
- Every event captures who (actor + auth method), what (entity + before/after diff), when, where (IP/geo), why (request correlation id linking to originating API call/trace).

## 10.2 SOC2 / PCI-DSS
- SOC2 Type II readiness: reviewed/approved migrations only (no direct prod DB access), quarterly RBAC access-review reports auto-generated, incident response runbooks, vendor risk mgmt for third-party connectors.
- PCI-DSS: Jengine reconciles settlement data (not raw PANs), but any connector touching payment-gateway data supports field-level tokenization at ingestion for card-related fields (configurable "sensitive field" tagging + vault-based tokenization) — avoids unnecessary PCI scope expansion, raw PANs never persisted in `raw_payload`.
- Network segmentation: PCI/SOC2-relevant components (secrets, tokenization service, audit archive) in isolated k8s namespaces/network policies with restricted egress.

## 10.3 Encryption & RBAC/ABAC
- At rest: encrypted disk volumes + Postgres `pgcrypto` for especially sensitive columns; ClickHouse encrypted volumes; S3 SSE-KMS with per-tenant KEK.
- In transit: mTLS between internal services (Linkerd — lower ops overhead than Istio at target scale, or manual mTLS via cert-manager deferred to V2), TLS 1.3 external.
- RBAC: Tenant Admin, Recon Manager, Analyst, Approver, Auditor/Read-Only, API Integration Role — scoped per tenant.
- ABAC on top via **Open Policy Agent (OPA)** (e.g. "Analyst can only act on breaks for accounts in their business unit," "Approver cannot approve own submitted maker actions") — Rego policy, auditable/adjustable by compliance without code deploy.

## 10.4 Data Residency
- Tenant registry records region/residency requirement per tenant; Dedicated/Isolated tenants pinned to specific k8s cluster/region/cloud account (e.g. EU tenant data never leaves EU region — GDPR + banking localization law).
- Control plane may be global; all transactional/PII data planes region-pinned per tenant config.

## 10.5 WORM Storage
Covered in [08-storage-architecture.md](08-storage-architecture.md) §9.4 and §10.1 above — S3 Object Lock compliance mode, immutable for configured retention even against admin/root deletion attempts (specific regulator requirement in some jurisdictions).

---
Next: [10-observability-reliability.md](10-observability-reliability.md)
