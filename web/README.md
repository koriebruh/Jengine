# Jengine Web (Frontend)

Next.js (App Router) + TypeScript dashboard for Jengine. See `plans/docs/14-dashboard-frontend.md` for the design doc, `plans/task/frontend/` for the task set.

## Running

From the repo root (single `make` entrypoint, consistent with the Go side):

```
make web-dev     # dev server, http://localhost:3000
make web-build   # production build
make web-lint    # eslint
```

Or directly inside `web/`: `npm run dev` / `npm run build` / `npm run lint`.

## Node version

Pin to Node 24.x LTS (see `package.json` `engines` field) - the version this project was built and verified against.

## Stack

- Next.js App Router + TypeScript (strict mode)
- Tailwind CSS + shadcn/ui (built on Base UI primitives, not Radix - shadcn's newer default)
- `next-themes` for light/dark (the one exception to "no extra state library" below - a small, justified addition per `plans/docs/14-dashboard-frontend.md` §14.1)
- TanStack Query (server state) and TanStack Table (data grids) arrive in frontend task 02 / task 03+, not yet installed

## State management rule

**No Redux, Zustand, Jotai, Recoil, or any other global client-state library.** Local UI state (sidebar open/closed, dialog open/closed, etc.) uses React's own `useState`/`useReducer`/Context. Server state (once frontend task 02 lands) is owned entirely by TanStack Query. This is a deliberate, documented constraint (`plans/docs/14-dashboard-frontend.md` §14.1) - don't reintroduce a global store "just for one boolean."

## Directory conventions

- `app/(dashboard)/` - the authenticated shell route group (no `/dashboard` URL segment). Each screen gets its own subdirectory (`cases/`, `matches/`, `connectors/`, `rules/`, `admin/`, `audit/`) plus the root `page.tsx` for Overview.
- `components/ui/` - shadcn output, generated via `npx shadcn@latest add <component>`. Never hand-edit these to add one-off variants; extend via composition in `components/layout/`, `components/case/`, etc. instead.
- `components/layout/` - the dashboard shell (`sidebar.tsx`, `topbar.tsx`, `nav-items.ts`).
- `lib/` - `utils.ts` (shadcn's `cn()` helper) today; `lib/api/` (the generated Connect-RPC client) arrives in frontend task 02.

## Nav / route map

Fixed by `components/layout/nav-items.ts` - later tasks assume these exact paths, don't rename:

| Screen | Route | Status |
|---|---|---|
| Overview | `/` | placeholder (frontend task 07) |
| Cases | `/cases` | placeholder (frontend task 03) |
| Match Review | `/matches` | placeholder (frontend task 05) |
| Connectors | `/connectors` | placeholder (frontend task 06) |
| Rules | `/rules` | placeholder, marked **V1** (frontend task 08) |
| Admin | `/admin` | placeholder, marked **V1** (frontend task 09) |
| Audit | `/audit` | placeholder, marked **V1** (frontend task 11) |

No `middleware.ts` / auth gating yet - frontend task 02.
