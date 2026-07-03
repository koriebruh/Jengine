# Operating Instructions — How an AI Agent Should Work Through `task/`

This file is the process contract for whoever (human or AI) executes the tasks in `core/` and `frontend/`. The task files describe **what** to build; this file describes **how to move through them safely**, so a long, mostly-unsupervised build doesn't silently drift from the design or fake completion.

Read this file once before starting any task. Read it again if you're resuming after a break.

## 1. Before starting any task

1. Read `plans/task/README.md` — build order, MVP-vs-V1 phase boundary, why `core/` comes before `frontend/`.
2. Open the specific task file. Read it in full — Goal, Prerequisites, Scope/Deliverables, Design Reference, Implementation Notes, Non-Goals/Guardrails, Definition of Done, Common Pitfalls. Do not skim past Non-Goals — it's the main defense against scope creep.
3. **Verify Prerequisites are actually satisfied in this repo right now — not just "the task number is lower."** Concretely: locate the prerequisite task's Definition of Done and confirm its tests currently pass / its artifacts exist. A lower task number that was never finished, or was finished differently than specced, is not a satisfied prerequisite. If a prerequisite is unmet or ambiguous, stop and fix/flag that first — don't build on top of an assumption.
4. If the task's Design Reference points at a `plans/docs/*.md` section and something in the task is unclear, read that section before improvising. Task files intentionally don't repeat design rationale — it's one link away, not a guess.

## 2. While implementing

- Build only what's in **Scope/Deliverables**. Non-Goals are a hard boundary: if something looks like it should be fixed/added but is listed as a Non-Goal (or owned by a different task number), leave it — note it in `QA_REPORT.md` (see §4) instead of doing it inline.
- Don't refactor or "improve" unrelated code while implementing a task. A task that touches file X doesn't license cleanup of file Y.
- Follow the Implementation Notes' concrete decisions (struct fields, function signatures, library choices) as given — they were chosen for cross-task consistency (e.g. a type or table name another task already depends on). If a Note seems wrong, that's a conflict (§3), not something to silently override.

## 3. When you find a conflict between tasks

This will happen — 37 files written across many sessions will have some mismatches (a field name, an error-handling convention, a library choice) that only surface once code is actually written.

- **Small, unambiguous, backward-compatible gaps** (e.g. an enum value present in a design diagram but missing from a schema migration): fix directly, and note what you fixed and why in the commit message and in `QA_REPORT.md` until verified. Example precedent: task 03's schema was missing the `REOPENED` case-status value that task 05's lifecycle diagram requires — adding the missing enum value is this kind of fix.
- **Design-level ambiguity or contradiction** (two tasks assume different libraries/approaches, a genuinely unclear ownership boundary, a Prerequisite that can't be satisfied as written): **stop. Do not guess or pick a side silently.** Log it in `QA_REPORT.md` with: which tasks conflict, what the actual mismatch is, and your proposed resolution — then surface it for a human decision before continuing past that point. Guessing here is exactly how small inconsistencies compound into a much larger rework later.

## 4. QA_REPORT.md convention (no clutter, ever)

- One file, root of the repo: `QA_REPORT.md`. Create it if it doesn't exist.
- It holds **only currently open issues** — nothing else. Not a history, not a log, not a changelog.
- Found an issue → add an entry. Fixed and re-verified → **delete that entry**, don't check it off. An empty `QA_REPORT.md` is the clean/expected state.
- Never create a second file (`qa-report-2.md`, `qa-final.md`, `qa-v2.md`, etc.) — always edit this one in place.
- History of what was fixed lives in git commits (one commit per fix), not in this file.

## 5. Definition of Done — verify, don't assert

- A task is done when its stated tests (unit, integration, golden-dataset, or manual-verification step per the task file) **actually pass, actually run** — not "this looks correct so it should pass." Run them.
- If a DoD step can't be verified in the current environment (missing infra, a dependency not yet built), **that task is not done** — stop and report the blocker, don't mark it complete anyway and move on.
- Task files themselves are never edited to add a "✅ Done" marker or status field. Completion is recorded by: the test suite passing, and git history (the commit(s) that implemented the task). Two sources of truth, both external to the task file's text.
- If, once implementing, a task's stated scope turns out to be wrong or incomplete, fix the task file's content to match reality — don't leave the wrong spec in place and don't just append a note that it changed.

## 6. Execution order

- `core/` tasks 01–17 and `frontend/` tasks 01–07 are MVP — build these first, in the numbered order within each folder (frontend tasks may start as soon as their *specific stated* core prerequisites exist — see each frontend task's Prerequisites — not "all of core 01-17 finished").
- Do not start `core/` 18+ or `frontend/` 08+ (V1-phase) until **all four scenarios in [`MVP_ACCEPTANCE_GATE.md`](MVP_ACCEPTANCE_GATE.md) pass against the real dev stack.** Individual task-level Definitions of Done verify each task in isolation — they do not prove the tasks actually work wired together. The acceptance gate is what proves that.
- `core/26-v2-backlog-notes.md` is a pointer file, not a buildable task — don't build V2 ideas early because they happen to be listed.

## 7. When genuinely blocked

If a requirement is ambiguous in a way that only a human can resolve (a product decision, not a technical one), or a conflict from §3 needs a real judgment call — stop and ask. Don't proceed on a guess whose cost of being wrong is high (schema decisions, cross-task contracts, security/compliance choices). It's cheaper to pause once than to unwind a cascade of code built on a wrong assumption.
