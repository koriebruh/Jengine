# Task 25: Connector SDK and Extensibility (WASM)

## Goal
Publish the Connector SDK that lets third parties build reconciliation source connectors without Jengine granting them native code execution: a stable Go-interface-mirroring SDK, a WASM sandbox (TinyGo/wazero) that untrusted connector code runs inside, a scaffold CLI, and a local test harness. This directly answers ReconArt's closed-integration weakness with an ecosystem play (Fivetran/Segment-style). Scope is deliberately narrowed to the code-facing parts: SDK interface stability, the WASM host-function surface, and the test harness. The human review/marketplace-listing process is a business process, not a code deliverable, and is explicitly out of scope.

## Prerequisites
- Core task 06 (`SourceConnector` interface + registry — the native-Go contract this SDK/WASM boundary mirrors for third parties).
- Core task 18 (webhook-receiver/Kafka-source connectors — useful reference implementations of the connector pattern, though those stay native Go, not WASM).
- Core task 08 (field-mapping DSL — third-party connectors' output still flows through it).

## Scope / Deliverables
- `sdk/connector/` — a separate Go module (own `go.mod`, decoupled release/versioning from the main app) containing:
  - `sdk/connector/api/` — the stable interface third-party connector authors implement.
  - `sdk/connector/wasmguest/` — TinyGo-buildable helper package with the required `//export` glue.
  - `sdk/connector/cmd/jengine-connector/` — the scaffold CLI.
  - `sdk/connector/testharness/` — the local test harness.
- `internal/ingestion/wasmrunner/` (in the main module) — the sandbox executor: loads a compiled WASM module via wazero, wires host functions, invokes guest exports, marshals `RawRecord`s across the boundary.

## Design Reference
- `plans/docs/02-data-ingestion.md` §3.1 (third-party extensibility: why WASM over native `.so` plugins — sandboxing + ABI stability).
- `plans/docs/07-api-extensibility.md` §8.3 (SDK, scaffold CLI, test harness, marketplace/certification — read this for the certification process's business-side framing; this task implements only its code-facing subset).
- `plans/docs/16-development-workflow.md` §16.4 (connector conformance testing philosophy — the test harness here should follow the same golden-fixture approach used for MT940/BAI2/ISO20022 conformance tests).

## Implementation Notes

### WASM host-function surface (guest calls into host)
```
jengine_host module:
  emit_record(ptr, len) -> i32
    // guest calls once per parsed record; payload is a protobuf-encoded RawRecord
  get_secret(key_ptr, key_len, out_ptr, out_len) -> i32
    // resolves a Vault-path-referenced secret by declared key; guest never sees raw Vault
    // credentials, only the resolved value for keys it explicitly declared in its manifest
  log(level, msg_ptr, msg_len)
    // structured logging bridged to the host's slog
  checkpoint_save(cursor_ptr, cursor_len) -> i32
  checkpoint_load(out_ptr, out_len) -> i32
  http_fetch(req_ptr, req_len, out_ptr, out_len) -> i32
    // sandboxed outbound HTTP; host enforces a per-tenant/per-connector egress allowlist and
    // rate limit — the guest cannot make arbitrary, unrestricted network calls
```
### Guest exports (TinyGo side, required of every connector)
```go
//export jengine_fetch
func fetch(configPtr, configLen uint32) uint32

//export jengine_validate
func validate(configPtr, configLen uint32) uint32

//export jengine_supports_streaming
func supportsStreaming() uint32
```
These mirror `SourceConnector.Fetch`/`Validate`/`SupportsStreaming` from task 06 — the SDK's whole point is that a WASM connector looks and behaves like a native one from the host's perspective, just executed inside `wazero` instead of an in-process Go call.

### Sandbox constraints
- Bounded memory (wazero module config, e.g. capped linear-memory pages).
- Execution timeout: wazero does not provide true CPU-cycle fuel metering. Implement best-effort preemption — run the guest call in a goroutine, `select` on `ctx.Done()`/a wall-clock timer, and hard-cancel via closing the module instance if exceeded. **Document this as a known limitation** — it is wall-clock preemption, not true fuel-metered CPU limiting; do not claim otherwise in comments or docs.
- No filesystem or raw network access for the guest beyond what's exposed through the declared host functions — this is the entire point of choosing WASM over native Go plugins (§3.1), so there must be no side-channel that reintroduces unrestricted I/O.

### Scaffold CLI
`sdk/connector/cmd/jengine-connector/main.go`:
- `jengine-connector new <name>` — generates a TinyGo project skeleton (`go.mod`, `main.go` with the three exports stubbed, a `testdata/` folder, a README).
- `jengine-connector build` — runs `tinygo build -o connector.wasm -target=wasi ./...`.
- `jengine-connector test` — runs the local harness against the built module.
- `jengine-connector cert-scan` — an automated pre-submission security scan (see below); this is the code-facing slice of "certification," not the human review itself.

### Local test harness
`sdk/connector/testharness/` — replays a sample raw file/payload through the compiled WASM module without a running Jengine instance: mock pipeline, dry-run mapping preview (applies a sample field-mapping spec to the harness's emitted `RawRecord`s and shows the resulting canonical `Transaction` shape), asserts no panics/timeouts, and diffs emitted records against an expected-output fixture (golden-file style, matching the conformance-test philosophy already used for the built-in format parsers).

### Certification scan (code-facing slice only)
`cert-scan` runs the compiled module through an automated checklist: memory limit respected, no disallowed host-module imports beyond the declared surface, egress calls only to allowlisted domains declared in the connector's manifest, and a check that no secret-shaped string is ever passed to the `log` host function (static string-pattern check plus a dynamic check via the harness intercepting log calls during a test run). The actual human marketplace review and listing process is explicitly not part of this task.

## Non-Goals / Guardrails
- Do not build the marketplace web UI, listing site, or the human certification review workflow — business/ops process, not a code deliverable of this task.
- Do not migrate MVP's existing native connectors (SFTP, CSV, MT940 from task 07; webhook-receiver, Kafka-source from task 18) to WASM — they stay native Go for performance and because they're first-party/already-trusted code with no sandboxing need. WASM is a parallel path for third-party-authored connectors only, not a replacement of the native `SourceConnector` path.
- Do not implement true CPU-cycle fuel metering — wazero doesn't provide it; document the wall-clock-timeout limitation instead of pretending otherwise.
- Do not use Go's native `plugin` package anywhere — explicitly rejected by the design for ABI-fragility reasons; if you find yourself reaching for it, that's a sign of drifting off the WASM design.

## Definition of Done
- Unit tests for `wasmrunner` host functions: secret resolution respects declared-key scoping, egress enforcement blocks non-allowlisted domains, checkpoint save/load round-trips correctly.
- Integration test: scaffold a sample TinyGo connector via the CLI, build it, run it through the test harness end-to-end (build → run → emit records → compare against a golden fixture).
- A timeout/resource-limit test: a deliberately infinite-looping guest module gets killed within a bounded wall-clock time, and the host process itself remains healthy afterward.
- A security-scan test: a deliberately malicious sample connector attempting disallowed egress or secret exfiltration is caught by `cert-scan`.

## Common Pitfalls
- Giving the WASM guest any direct filesystem/network access path instead of routing everything through explicit host functions — defeats the entire sandboxing rationale.
- Exposing raw Vault credentials to guest code instead of only resolved secret values for keys the connector explicitly declared.
- Treating wazero's context-cancellation as equivalent to true CPU-fuel limiting — it isn't; a tight non-yielding loop can still consume a full timeout window of CPU before being killed. Document this rather than overclaiming safety.
- Reaching for Go's native `plugin` package anywhere in this task — explicitly rejected by the design.
- Building marketplace UI or the human certification workflow as if they were this task's code deliverables.
- Forcing existing native MVP connectors through the WASM path "for consistency" — they are intentionally exempt.
