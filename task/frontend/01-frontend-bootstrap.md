# Task 01: Frontend Bootstrap

## Goal
Establish the Next.js (App Router) + TypeScript project inside `web/` that every later frontend task builds on: routing skeleton, Tailwind + shadcn/ui installed and themed, shared layout shell, and the project-level conventions (no global client-state library, directory structure for `components/`, `lib/`) that keep later screens consistent instead of each one inventing its own patterns. This is the frontend's equivalent of core task 01 — scaffolding only, no API calls, no real screens yet.

## Prerequisites
- Core task 01 (repo bootstrap and tooling) — the `web/` directory placeholder must exist at repo root before this task populates it.
- No other core task is required to start this — this task creates no API calls and needs no backend running.

## Scope / Deliverables
- `web/package.json`, `web/tsconfig.json`, `web/next.config.ts` — Next.js (App Router) + TypeScript, strict mode on.
- `web/tailwind.config.ts`, `web/postcss.config.js`, `web/app/globals.css` — Tailwind installed.
- shadcn/ui initialized (`components.json`) with a small initial set of primitives actually needed for the shell: `button`, `card`, `separator`, `sheet` (mobile nav), `tooltip`, `sonner` (toast) — do not pre-generate the full shadcn catalog speculatively; later tasks add primitives as their screens need them (table, badge, dialog, etc. arrive with frontend task 03+).
- `web/app/layout.tsx` — root layout: HTML shell, font loading, `<Toaster />`, theme provider (light/dark via `next-themes` is acceptable — a small, justified addition, not a "global state library").
- `web/app/(dashboard)/layout.tsx` — the authenticated shell: `components/layout/sidebar.tsx` (nav links to the 8 screens' routes, stubbed as placeholder `<div>` pages for now except where a later task fills them in) and `components/layout/topbar.tsx` (tenant name placeholder, user menu placeholder — no real auth wiring yet, that's frontend task 02).
- `web/app/(dashboard)/page.tsx` — placeholder route for the Overview Dashboard (frontend task 07 fills this in).
- Placeholder route files (empty `<div>Not yet implemented</div>` pages, just enough for the sidebar links to resolve without 404s) for: `web/app/(dashboard)/cases/page.tsx`, `web/app/(dashboard)/matches/page.tsx`, `web/app/(dashboard)/connectors/page.tsx`, `web/app/(dashboard)/rules/page.tsx`, `web/app/(dashboard)/admin/page.tsx`, `web/app/(dashboard)/audit/page.tsx`.
- `web/components/ui/` (shadcn output — generated, not hand-authored).
- `web/components/layout/` — `sidebar.tsx`, `topbar.tsx`.
- `web/lib/utils.ts` — shadcn's `cn()` helper.
- `web/.eslintrc.json` (or flat config) + `web/.prettierrc` — lint/format config consistent with the Go repo's lean-config philosophy (correctness/consistency rules, not speculative style nitpicking).
- `web/README.md` (a short "how to run this" note is acceptable here since it's a directory-local operational doc, not a repo-root doc) — `npm run dev`/`build`/`lint` commands.
- Root `Makefile` additions (edit, don't recreate): `web-dev`, `web-build`, `web-lint` targets that `cd web && npm run ...` — keeps the single `make` entrypoint convention from core task 01 consistent across the whole repo.
- `web/package.json` engines field pinning a Node LTS version; document it in `web/README.md`.

## Design Reference
- plans/docs/16-development-workflow.md §16.1 — `web/` sits at repo root alongside `cmd/`, `internal/`, `proto/`; this task must not restructure anything outside `web/`.
- plans/docs/14-dashboard-frontend.md §14.1 — tech stack table (Next.js App Router, TanStack Query, TanStack Table, shadcn/ui + Tailwind, React Context/`useState`/`useReducer` for local view state, explicitly **no** Redux/Zustand).
- plans/docs/14-dashboard-frontend.md §14.2 — the 8 screens this sidebar must have nav entries for (even before they're built).
- plans/docs/14-dashboard-frontend.md §14.3 — component architecture (`components/data-table/`, `components/charts/`, `components/case/`, `lib/api/`) — this task creates the directory convention; it does not populate `data-table`, `charts`, `case`, or `lib/api` with real content (those arrive in frontend tasks 02, 03, 07).

## Implementation Notes
- Use the App Router route-group pattern: `(dashboard)` groups all authenticated screens under one layout without adding a `/dashboard` URL segment. A future `(auth)` group (frontend task 02) will hold `/login`.
- Sidebar nav item list (label → route), in this fixed order so later tasks don't have to guess placement: Overview (`/`), Cases (`/cases`), Match Review (`/matches`), Connectors (`/connectors`), Rules (`/rules`), Admin (`/admin`), Audit (`/audit`). Mark Rules, Admin, and Audit nav items with a small "V1" badge/visual treatment (a plain Tailwind class, not a feature-flag system) since frontend tasks 08/09/11 build them later — this keeps the shell honest about what's real today without hiding the eventual information architecture.
- No `middleware.ts` yet — auth-gating the route group is frontend task 02's job. This task's `(dashboard)/layout.tsx` renders unconditionally.
- Keep `next.config.ts` minimal: no experimental flags, no custom webpack config, unless something in this task's own scope requires it (it shouldn't).
- Theming: pick a Tailwind color/typography baseline now (even a simple neutral shadcn default) so screens built in later tasks aren't each choosing their own ad-hoc palette — but do not over-invest in visual design here; this is scaffolding, not the final look.
- State management rule to enforce from day one: no `zustand`, `redux`, `jotai`, `recoil`, etc. in `package.json`. Local UI state (sidebar collapsed/expanded, dialog open/closed, etc.) uses `useState`/`useReducer`/React Context, per §14.1. TanStack Query (added in frontend task 02) owns all server state. Document this rule in `web/README.md` so later tasks don't reintroduce it.

## Non-Goals / Guardrails
- Do not add TanStack Query, TanStack Table, or any API client here — that is frontend task 02 (Query/auth) and the screen tasks that follow. This task has zero network calls.
- Do not implement authentication, session handling, or `middleware.ts` route protection — frontend task 02.
- Do not build any of the 8 real screens' content — only placeholder routes so navigation doesn't 404.
- Do not add a global client-state library (Redux/Zustand/Jotai) — explicitly rejected by plans/docs/14-dashboard-frontend.md §14.1.
- Do not add charting libraries (visx/Recharts) yet — frontend task 07 introduces `components/charts/`.
- Do not generate the entire shadcn component catalog up front "to save time later" — add primitives as each task needs them; an unused-component pile is dead weight and drifts from actual usage.
- Do not pick a CSS-in-JS library or component library other than shadcn/ui + Tailwind — that choice is already made in the design doc.

## Definition of Done
- `make web-dev` boots the Next.js dev server; visiting `/` renders the dashboard shell with a working sidebar; clicking every nav item routes to its placeholder page without a 404 or console error.
- `make web-build` produces a production build with zero TypeScript errors.
- `make web-lint` passes cleanly.
- Manual verification in a browser: resize to a narrow viewport and confirm the sidebar collapses to a sheet/drawer (using the shadcn `sheet` primitive) rather than breaking layout.
- If exploratory QA turns up issues, record them in a single root-level `QA_REPORT.md` (create if absent), open items only, deleted when fixed — never a checklist inside this task file.

## Common Pitfalls
- Reaching for Zustand/Redux "just for the sidebar-open boolean" — that is exactly the over-engineering trap §14.1 calls out; use `useState`.
- Building real screen content in this task because "the placeholder felt too empty" — scope creep into frontend tasks 03–11's territory; keep placeholders trivial.
- Wiring an API client or fetch call here to "test the shell renders real data" — there is no API client yet; that's frontend task 02.
- Choosing a routing structure that doesn't match the 8-screen sidebar order above — later tasks assume these exact route paths (`/cases`, `/matches`, `/connectors`, `/rules`, `/admin`, `/audit`) and will not re-derive them.
- Installing `next-auth` or any auth package in this task — even though it's on the roadmap for frontend task 02, adding it here without wiring it creates half-configured state the next task has to clean up.
