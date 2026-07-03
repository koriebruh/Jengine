# Task 26: V2 Backlog Notes

**This is not a buildable task.** It is a pointer-only list of Phase 2 (V2) ideas from the design doc set, kept here so they aren't lost — not so they get started before the V1 tasks (18–25) are working end-to-end and verified. Do not implement anything in this file. Each item below is one line plus a doc pointer; if you're an implementing agent and you've landed here looking for something to build, stop and check `plans/task/README.md` for the actual current task instead.

- **ML-based match scoring with feedback loop** — gradient-boosted scoring model trained on historical analyst match/reject decisions. `plans/docs/04-matching-engine.md` §5.3.
- **Rule backtesting sandbox refinement** — the sandbox itself ships in V1 (frontend task 08); V2 is deepening/refining it. `plans/docs/11-scalability-roadmap.md` §12.2 Phase 2.
- **GraphQL reporting gateway** — separate read-only `gqlgen` gateway in front of ClickHouse for ad-hoc BI/analyst queries. `plans/docs/07-api-extensibility.md` §8.4.
- **Multi-region deployment** — region-pinned data planes maturing beyond the single-region-per-tenant baseline; matured DR automation (game days, automated failover). `plans/docs/09-security-compliance.md` §10.4, `plans/docs/11-scalability-roadmap.md` §12.2 Phase 2.
- **Validated 50M+/day scale-out** — chaos/load testing, partition/shard tuning from real production telemetry. `plans/docs/11-scalability-roadmap.md` §12.2 Phase 2.
- **Advanced OPA ABAC policies** — deeper policy sets beyond the V1 starter examples built in task 23. `plans/docs/11-scalability-roadmap.md` §12.2 Phase 2.
- **SOC2 Type II audit completed** — the audit/certification process itself, distinct from the technical controls task 23 builds. `plans/docs/09-security-compliance.md` §10.2, `plans/docs/11-scalability-roadmap.md` §12.2 Phase 2.
- **Connector marketplace opened to third parties** — the business/ops process on top of the SDK and cert-scan tooling task 25 builds. `plans/docs/07-api-extensibility.md` §8.3, `plans/docs/11-scalability-roadmap.md` §12.2 Phase 2.
