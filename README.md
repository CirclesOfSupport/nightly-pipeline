# nightly-orchestrator

Thin sequence-of-services orchestrator for the Early Alert nightly pipeline.
Calls four existing Cloud Run services in dependency order, status-gating each
before the next. One Cloud Scheduler job drives this service; it replaces the
standalone `contacts-sync-nightly` and `vamc-sync` Scheduler jobs.

## Chain (`POST /run`)

1. **backup-textit-flows** `/backup` — capture TextIt state before any writes.
2. **contacts-sync** `/sync` — TextIt → BQ users full sync (TextIt-wins).
3. **state-vamc-sync** `/sync` — set state + vamc_presumed (Sta#), BQ + TextIt.
4. **vamc-sync** `/sync` — derive vamc_display_name from vamc_presumed.

### Ordering rationale (data dependencies, not preference)

- backup first — snapshot before mutations.
- contacts-sync before state/vamc — the sync is TextIt-wins; running it after
  state/vamc would clobber the fresh values with TextIt's older ones.
- state/vamc before vamc-sync — vamc-sync derives display names from the
  vamc_presumed values state/vamc writes.
- state-vamc-sync's TextIt-writeback EXISTS-filter reads `itdo423_textit_full`,
  which contacts-sync (step 2) freshly rewrote this same run — so it's current.

## Auth

Each child is `--no-allow-unauthenticated`. The orchestrator mints a GCP OIDC
identity token per call (audience = child base URL) via its runtime SA, AND
passes each child's app-password in the body (the children's second auth layer).
**The orchestrator's runtime SA needs `roles/run.invoker` on all four child
services.**

## Status gating

A step PASSES only if the HTTP call is 2xx AND the child's JSON `status` is not
`"error"`. Any failure HALTS the chain by default — a failed backup must not let
writes proceed; a failed contacts-sync must not let state/vamc run on stale data.
Pass `{"continue_on_error": true}` to attempt all steps regardless and report
per-step outcomes (diagnostic only). `/run` returns 200 on full success, 502 if
any step failed, with a per-step `results` array.

## Config — Cloud Run console (NOT repo)

- `ORCH_PASSWORD` — POST-body auth for `/run`.
- `BACKUP_PASSWORD`, `CONTACTS_PASSWORD`, `STATEVAMC_PASSWORD`, `VAMC_PASSWORD` —
  each child's app-password.
- `BACKUP_URL`, `CONTACTS_URL`, `STATEVAMC_URL`, `VAMC_URL` — child base URLs
  (defaults are the live us-east1 URLs).
- `STEP_TIMEOUT` — per-step HTTP timeout, default 3600s.

## Cloud Scheduler

One job hits `/run` with an OIDC token + body `{"password": "<ORCH_PASSWORD>"}`.
Schedule in the 12:30 AM CT band (clear of Meridian's 3–5 AM CT local ETL window
— though that's a local-machine constraint and this is cloud-side; the live
constraint is TextIt API contention with the local backup MVP until it's
retired). DST-aware timezone (America/Chicago), not fixed-UTC cron.

After this is validated, retire `contacts-sync-nightly` and `vamc-sync` Scheduler
jobs and the local TextIt backup MVP (Windows Task Scheduler).
