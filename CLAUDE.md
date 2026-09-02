# Buffalo Construction — Prep-Phase Meeting Tracker (Vendor-Compliance)

> **Continuing this in a new session? Read "Resume Here" first, then `docs/setup.md`.**

## What this is

A tracker for the **Preparatory Meeting** BCI holds with each vendor before that vendor starts
work on a job. For every active project it lists every vendor and answers one question per row:
**has their prep meeting happened — yes or no.**

The meetings are created in Procore from the meeting template at
`.../tools/meetings/create/383995`, retitled with the vendor's name
("Preparatory Meeting - ZIP"), and the vendor is normally added to the meeting's attendee list.
Those two facts — the title and the attendee list — are the two signals the tracker matches on.

---

## ⚡ Resume Here — Current State (2026-08-12)

**Built end-to-end this session, from an empty repo.** The **ingest is now confirmed against live
Procore data** (20-project sample) — see *Answered by the first live runs* below. The gold, mirror
and dashboard layers are still stub-verified only.

```
Procore ─▶ fabric/ingest_vendor_compliance.py ─▶ silver_vendor_*
        ─▶ fabric/build_vendor_gold.py        ─▶ gold_vendor_*
        ─▶ pipeline mirror-vendor-to-sql (4)  ─▶ Safety-Dash SQL  dbo.vendor_*
        ─▶ api/ (SWA managed Functions)       ─▶ dashboard/index.html
```

### It lives in its own Fabric notebook

`Vendor Compliance - Prep Meetings`, two cells (ingest, then gold). **Fully standalone** — it mints
its own Procore token from Key Vault and fetches its own project list, so it has no ordering
dependency on `main_ingestion.py` or the Procore Nightly/Friday notebooks. A failure here can't
take down the safety dashboard's run, and vice versa.

Its only external dependency is `dbo.projects` in the Safety-Dash SQL DB (for the active-project
filter and project names), which the existing Procore→safety mirror chain already maintains.

### Two decisions the user made, and why they shape everything

1. **Vendor roster = both sources, with a toggle.** Both Procore commitments
   (`work_order_contracts` + `purchase_order_contracts`) *and* the project directory
   (`/projects/{id}/vendors`) are ingested. Which one forms the checklist's denominator is the
   live `vendorSource` setting, defaulting to **`either`** (the union).
   **Commitments DO exist** — see *The commitments bug* below; an earlier claim that BCI didn't
   use them was a bug being misread as a fact. The default stays `either` deliberately: it is the
   only value that cannot silently *hide* a vendor, whichever roster a given project keeps current.

2. **This app shares the Safety-Dash Fabric SQL DB.** It does not get its own. Fabric's
   `Sql Usage` meter looks allocation-based (an idle database still bills a flat share of the
   capacity) and the F4 already breaches its interactive-delay threshold ~6× per 13 days — a
   fourth always-on SQL DB would cost real headroom for a tracker read a few times a day. The
   second reason matters more: **`dbo.projects` already lives there**, so "the same active
   projects as the other dashboards" is enforced by using the same rows through the same
   predicate, not by a duplicated definition that drifts.

### The matching engine (`build_vendor_gold.py`) — the heart of this

Two independent signals, emitted as **separate candidate rows** in `gold_vendor_prep_matches` so
the API can weight them live rather than gold collapsing them:

| Method | Rule | Confidence |
|---|---|---|
| `attendee` | The meeting has an attendee whose company **normalizes** to this vendor. Carries `attendee_attended` so the dashboard can insist on *Present* rather than *For Distribution Only*. | Strong — a direct record |
| `title` | The vendor's name appears as a **whole-token run** in the meeting title. | Weaker — text matching. Shown with a "Title match" chip, and can be switched off entirely. |

A vendor/meeting pair can produce **both** rows. That's intended — two independent confirmations
is exactly what you want.

**Why token-run and not substring.** `token_pad()` wraps a normalized string in spaces so
`INSTR` becomes a whole-token test: `" zip "` is found in `" preparatory meeting zip "` but
**not** inside `" preparatory meeting zipper "`. Without that padding a three-letter vendor
abbreviation matches the middle of unrelated words. Verified against the real titles: the
match matrix is a clean diagonal — ZIP/MAKK/Excel/KCC/Precision/Verastegui/East&Westbrook each
hit only their own meeting, and `Excel` does not match `Excelsior Roofing`.

**`title_matchable()` is load-bearing.** A vendor whose entire normalized name is meeting
boilerplate (a company literally named "Meeting", or one under 3 characters) is refused for title
matching — otherwise it matches *every* prep meeting. Verified: without the guard, a vendor named
"Meeting" matches all three sample titles.

### ⚠️ `normalize_company` exists in THREE places and they must stay byte-identical

`fabric/ingest_vendor_compliance.py`, `fabric/build_vendor_gold.py`, and `api/src/normalize.ts`.
The two Python copies **write** the keys stored in `vendor_roster` / `vendor_prep_matches`; the
TypeScript copy normalizes names a **human types** into the tracker (a manual vendor, an override
addressed by name). A silent divergence shows up as *every vendor reading "not held"*, or a
hand-entered vendor appearing as a second, permanently-outstanding company.

The rules are deliberately small so keeping them in sync is cheap:

- lowercase; `&` → `and`; **periods deleted** (not spaced); other punctuation → space; collapse
- strip trailing legal-entity tokens (`llc inc co corp ltd lp llp pllc pc pa …`)
- rescue an already-spaced acronym (`Smith L L C` → `smith`)

**Periods are deleted rather than turned into spaces, and that is a bug fix, not a style choice.**
Spacing them gives `L.L.C.` → `l l c`, three tokens that survive the suffix strip — so
`Smith & Sons, L.L.C.` and `Smith and Sons LLC` became two different vendors. Deleting is still
safe for `St. Louis Construction` because the following space survives.

Verified three-way identical across 26 real-world name shapes.

### Settings — all applied LIVE, never baked into gold

This is why there is deliberately **no `gold_vendor_prep_status` table**. Whether a match counts
depends on four things the safety team can change from a checkbox, plus per-vendor overrides.
Baking the answer into gold would mean a notebook + pipeline run every time someone flips one.
Gold emits candidate matches; the API resolves them.

| Setting | Default | Reasoning behind the default |
|---|---|---|
| `vendorSource` | `either` | Only value that can't hide a vendor. Both rosters are real in this tenant (the directory on ~every project, commitments on the jobs that have subcontracts written), and `either` is the union of them. |
| `allowTitleMatch` | **on** | This is how BCI names these meetings. Off drops most historical matches. |
| `requireVendorPresent` | **off** | The Present / Absent / For Distribution Only radio is frequently left at its default in the field. Switching this on before checking the data makes real meetings read as missed. Applies to attendee matches only — a title match has no attendance to inspect. |
| `requireMeetingHeld` | **off** | Procore's `held` flag is rarely flipped; requiring it makes nearly everything read "not held". |

### Admin access — `allowlist`, two people

`adminMode` defaults to **`allowlist`**. Admins are `cory.zilisch@` and
`justin.houston@buffaloconstruction.com` (`BOOTSTRAP_ADMINS` in `_shared.ts`);
everyone else who signs in is **read-only**. More can be added in Settings.

Sign-in is plain OIDC through SWA — the app registration needs only
**`User.Read`** (delegated) plus a redirect URI, ID tokens, and a client secret.
**Add the `email` optional claim**: the allowlist matches the principal's
address, and a token with no readable email/UPN claim matches nothing.

Three properties worth not breaking:

- **Bootstrap seeds are RECONCILED, not just inserted.** Rows in
  `dbo.vendor_admins` with `added_by = 'bootstrap'` are code-owned: dropping a
  name from `BOOTSTRAP_ADMINS` actually deletes their row on next start, rather
  than leaving a stale seed nobody remembers granting. Rows added through the UI
  carry the granting admin's address and are left alone.
- **`BOOTSTRAP_ADMINS` can't be removed through the API**, and the API refuses
  to remove the last admin. Both return 409 with the reason.
- **`ADMIN_MODE` env var is the break-glass.** Set it to `open` in the SWA
  Configuration blade to override the stored setting with no deploy. It exists
  for exactly one failure: an Entra token arriving with no email claim, where
  the allowlist has nothing to match and the fix would otherwise be a code
  change. `/api/me` reports `emails_seen` so that is diagnosable in one click.

Two deliberate details:

- **In `open` mode, having a principal is enough** — the check does NOT also
  require a readable email claim. Depending on how the Entra app is configured
  `userDetails` and the email claims can come back empty, and gating on an email
  would silently make everyone read-only, which is the exact opposite of what
  `open` is for.
- **`BOOTSTRAP_ADMINS` (in `_shared.ts`) can never be removed through the API**,
  and the API refuses to remove the last remaining admin. An editable admin list
  is an emptiable one, and the only other recovery route here would be a SQL
  console. Both refusals return 409 with the reason, so the UI can say which
  rule was hit rather than failing silently.

### The Metrics tab, and the two numbers it refuses to show

`GET /api/metrics?months=` → adoption snapshot, monthly series, project
leaderboard, most-seen vendors. Charts mirror the Safety Dashboard's
`drawChart`: inline SVG, dashed gridlines, 2px polylines, hover targets, legend.

**No "coverage over time" chart exists, deliberately.** The vendor roster is a
current snapshot mirrored from Procore — there is no record of who was on a
job's roster last March — so a past-month percentage would be measured against a
denominator we do not have. The monthly charts count only things that actually
happened: meetings held, projects participating, vendors credited, how each
meeting was recorded, whether attendance was ticked.

**Coverage % is shown but labelled as what it is.** Its denominator is every
company in the project directory, which includes owners, architects, inspectors
and suppliers. Until those are marked *Not applicable* it reads ~2% and badly
understates reality — so the tile says "coverage of all directory companies" and
a note points at **project adoption** as the trustworthy headline instead.

**Series colours: orange `#FF5F00` then steel `#2A6496`, fixed order, max two
per chart.** That is not aesthetic preference — it is the only pair inside
Buffalo's palette that survives an all-pairs colour-vision check. The obvious
three-way good/ok/bad painting fails outright: orange↔amber is **ΔE 4.6 under
deuteranopia**, so a red-green colourblind reader could not separate "attendee
list" from "unmatched" — the exact comparison the chart exists to make. That is
why "meetings that matched no vendor" is its own chart rather than a third line.
Re-run `scripts/validate_palette.js` before adding any series.

### Procore deep links

| Target | URL |
|---|---|
| Project | `https://app.procore.com/{project_id}/project/home` |
| Meeting | `https://app.procore.com/webclients/host/companies/18895/projects/{project_id}/tools/meetings/`**`details/`**`{meeting_id}` |

⚠️ **The meeting URL needs the `details/` segment.** Deriving it from the create
URL the team uses (`.../tools/meetings/create/383995`) by swapping the id in
gives `.../tools/meetings/{id}`, which 404s — that was the first attempt.
Verified against a real Procore URL. Same shape as the Safety Dashboard's forms
link. Both URLs are built by one function each in `dashboard/index.html`.

### The Review Queue is not decoration

Prep meetings that were logged but **can't be credited to any vendor** get their own tab rather
than being silently dropped. In the sample data that's the meetings titled with a person's name
("FDG Prep phase meeting - Blake Daher") and any sub missing from both Procore rosters. Every row
is either a vendor to add or a title to fix. Quietly discarding them would make the tracker's
percentages look better than the truth.

### Admin escape hatches (auto-created tables, NOT mirrored)

`dbo.vendor_settings`, `dbo.vendor_prep_overrides`, `dbo.vendor_manual_roster`. Kept out of the
mirror pipeline on purpose — a Copy activity's pre-copy `DELETE` would wipe every override nightly.

- **Override** a vendor to `held` / `not_held` / `not_applicable` with a note and author. `held`
  covers "the meeting happened, nobody logged it"; `not_held` dismisses a bad title match;
  `not_applicable` removes the owner/architect/inspector from the *denominator* rather than
  counting them as outstanding forever.
- **Manual vendor** adds a sub that neither Procore roster knows about. Without it the tracker can
  only ever be as complete as Procore's contract/directory hygiene.

### What was verified, and how

- `api/` `tsc --noEmit` clean; both fabric cells pass `py_compile`; all JSON + the workflow YAML
  parse.
- **`node scripts/smoke_dashboard.js` — the dashboard is LOADED, not just parsed.** Serves
  `dashboard/` against a stub API, drives all four tabs and the project drilldown in headless
  Chromium, and fails on any uncaught error, same-origin console error or 4xx. External origins
  (Google Fonts) are ignored on purpose — failing on a blocked CDN would train people to ignore
  the test. Run it before every push that touches `dashboard/index.html`.

  ⚠️ **`node --check` was the only gate, and it shipped a dead dashboard.** Markdown `**` around
  a URL path inside a block comment put a comment terminator *inside* the comment: it ended
  early, left a bare `details` as a live statement, and the page threw
  `ReferenceError: details is not defined` before a single handler bound — nothing rendered,
  nothing clickable. A bare identifier is valid syntax, so the parser was happy and review saw
  a normal-looking comment. **A syntax check cannot catch a runtime error; only running it can.**
  The smoke test was verified by re-introducing both shipped bugs and confirming each fails it.
- **256 emitted SQL statements** captured with the db stubbed, across **all 24 setting
  permutations** (3 vendor sources × 8 flag combinations) plus every write path — checked for
  balanced parentheses, bind parameters used but not bound, and references to CTEs that don't
  exist. Zero problems.
- **Normalization parity** proven three-way (both Python copies == TypeScript) across 26 names.
- **Title matching** proven against the nine real meeting titles from the Procore screenshots —
  clean diagonal, plus explicit negative controls (`Excel` ∌ `Excelsior`, `ZIP` ∌ `Zipper`,
  `Clark Construction` ∌ "Clark FDG assignment").

### ✅ Answered by the first live runs (2026-08, 20-project sample)

Four of the five unknowns are now settled against real data:

| Question | Answer |
|---|---|
| **Is the meeting template id exposed?** | **Yes** — `383995` appeared on 10 meetings. And it is *necessary*: the title heuristic found only **3 of those 10**, because 7 prep meetings have no "prep" in the title. `PREP_MATCH_MODE` now defaults to the union of template + title. (The old `PREP_REQUIRE_TEMPLATE_ID` flag AND-ed them, so enabling it would have returned 3 — the intersection — and made things worse.) |
| **Which vendor roster does BCI maintain?** | **Both.** The directory covers ~every project (~22 companies each, but that includes owner/architect/inspectors). Commitments are real too — see *The commitments bug* below. Keep `vendorSource = either`. |
| **Where is the attendee's company?** | **Nowhere on the attendee.** The record is only `{id, status, login_information:{id, login, name}}`. Procore's UI resolves that column by joining the person to the project directory, so the ingest pulls `/projects/{id}/users` and does the same. Resolution went from **0/60 to 60/60**. An email-domain fallback (`…@jjfloresroofing.co` → "JJ Flores Roofing") covers people missing from the directory, bounded to vendors already on that project. |
| **Does the attendance field parse?** | **Yes** — Procore returns `'Present'`, `'For Distribution Only'`, `'Absent'`, all understood. The 11 "unknown" rows carry **no status field at all** (nobody ticked a box), which is different from an unrecognized value. |

### The commitments bug — and why it read as a fact about the business

Symptom the user hit: switching *vendor source* to **Commitments** emptied every
project's vendor list. The conclusion recorded here at the time — *"BCI does not
use Procore commitments; HTTP 200, zero rows, every project"* — was wrong, and
each step of getting it wrong is worth keeping:

1. `fabric/diagnose_commitments.py` (read-only, 8 endpoint variants per project,
   `prime_contracts` as a control) shows commitments plainly: project **3119932
   returns 20 work orders and 10 purchase orders**, with Procore's `Total` header
   agreeing, on **the exact endpoint the ingest was already calling**.
2. The full 90-project run had in fact ingested **1500 rows** into
   `bronze_vendor_commitments`. So the API call was never the problem.
3. Every one of those rows had an **empty `vendor_name`** — the v1.0 list
   response is slim and doesn't carry `vendor`, and the extraction read only
   `contract["vendor"]`. `silver_vendor_roster` filters
   `WHERE vendor_normalized <> ''`, so all 1500 were dropped.
4. The coverage diagnostic then printed `vendor_rows_commitment: 0`, and **a
   count of rows that had been thrown away was written down as an observation
   about how BCI works.** A run that "succeeds" while discarding its input looks
   exactly like a tenant that has no input.

Fixed by resolving the vendor the same way an attendee's company is —
documented keys → recursive `_deep_company` search → a bounded per-contract
detail call (`COMMITMENT_DETAIL_MAX`, default 600) — plus a one-time per-endpoint
probe of `view=extended`, kept only if it actually yields vendors this default
view didn't. `PULL_COMMITMENTS` is back **on**.

The guard against a repeat is the new **COMMITMENT VENDOR RESOLUTION**
diagnostic: contracts fetched → vendors resolved → roster rows out, side by
side, with the JSON path each vendor was found at. A bronze-in/silver-out gap is
now impossible to mistake for an absence. The coverage diagnostic also refuses
to let a zero stand on its own — it points at that block.

Note also that some contracts are titled `TEMPLATE` / `PO Template` with
`status: Draft`. They are deliberately **not** status-filtered: a template
carries no vendor, so it falls out of the roster on its own, and filtering on
status risks hiding a real sub whose contract hasn't been executed yet.

**Confirmed fixed (2026-09, 88 active projects, 8.5 min, 577 calls):**
1479 contracts fetched → **1407 vendors resolved → 1390 roster rows across 59
projects.** The 72 with no vendor are exactly the TEMPLATE/draft contracts.

**The shape, now that it's known:** the company is at
**`vendor.company.name`** — `vendor` is present but carries no name of its own,
which is why reading `contract["vendor"]["name"]` found nothing. The recursive
search rescued all 1408 of them, but the path is now named explicitly: it keeps
the common case cheap and it returns the **vendor id**, which the deep search
can't. The per-contract detail fallback was measured at **72 calls, 0
recoveries**, so it now stands itself down once the list view has proven it
carries vendors (`detail_worth_trying()`) — kept as the rescue if Procore
changes the shape, without spending ~12% of the nightly API budget confirming
that templates have no vendor.

### Which roster to point the tracker at — the numbers, and the trade

`commitment_only: 0`. **Every commitment vendor is also in the project
directory**, so the commitments roster is a strict *subset*, and `either` is
currently identical to `directory`:

| Source | Vendor rows | Projects covered |
|---|---|---|
| Directory (and therefore `either`) | 2362 | 91 |
| Commitments | 1390 | 59 |

So the choice is a real trade, not a coverage bug:

- **`either` / `directory`** — nothing can be hidden, but the denominator carries
  the 972 companies that are owners, architects, inspectors and suppliers. That
  is what drags coverage to ~2% and why the Metrics tab points at *project
  adoption* as the honest headline instead.
- **`commitment`** — the companies actually under contract, which is much closer
  to "who needs a prep meeting". But the 29 active projects with no commitments
  written show an **empty checklist**, which reads as nothing to do rather than
  as unknown.

Left on **`either`** because an empty checklist is the more dangerous failure.
It is a live setting, so it can be flipped from the Settings tab with no deploy
if the safety team would rather have the tighter denominator; the
`not_applicable` override is the other route, and works per vendor.

**The one finding that matters operationally:** across all 88 active projects
there are **63 prep meetings on 10 projects**. Chick-Fil-A Plainfield (23),
Hunting Creek Snack Shack (18) and AEP Eagle Pass (10) are more than four fifths
of them; **78 of 88 active jobs have never held one.** Adoption is the story
this tracker tells first, and most projects will read 0% until the process
spreads — that is the tracker working, not the data being wrong.

Two smaller ones worth keeping:
- **33 of the 63 prep meetings have no "prep" in the title** and were found only
  by template id `383995`. The title heuristic alone would have missed more than
  half of them. Nothing matched on title *only*, so the union is currently
  costing nothing and catching a lot.
- **19 attendees resolved by email domain rather than the directory join**, and
  1 could not be resolved at all — those people are not in their project's
  Procore directory. Adding them there is the fix; the tracker degrades to title
  matching for them meanwhile.

### ⚠️ What is still NOT verified

Specifically:

1. **The gold + API layers have not run against real rows yet.** Everything above was validated
   at the ingest stage; `build_vendor_gold.py`, the mirror pipeline and the dashboard have only
   been exercised against stubs and synthetic fixtures.
2. ~~The `title_only` match count is unmeasured at scale.~~ **Measured across all 88 active
   projects: 30 both, 33 template-only, 0 title-only.** No meeting has yet matched on title
   alone, so nothing is riding on the weaker signal — but leave `allowTitleMatch` on, since a
   prep meeting built from a different template is exactly what it exists to catch.
3. **`dbo.projects` column assumptions.** Confirmed against `build_gold.py`: it has
   `id, name, project_number, stage, is_active, project_manager, actual_start, projected_finish`
   and **no superintendent column** — the superintendent needs
   `dbo.project_superintendents` → `dbo.superintendents`. An early draft referenced
   `p.superintendent_name` directly, which would have been a *parse* error blanking the entire
   dashboard. Both the `is_active` column and the two superintendent tables are probed once via
   `ensureProjectColumnMeta()` and degrade rather than throw.

---

## Repo layout

```
api/                              SWA managed Azure Functions (v4 node, TypeScript)
  src/index.ts                    registers every functions/*.ts
  src/normalize.ts                the TypeScript copy of normalize_company (see warning above)
  src/cache.ts                    process-level TTL cache; ?fresh=1 bypasses
  src/db/client.ts                mssql pool, SP auth, dead-socket detection + one retry
  src/db/queries.ts               active-project filter, settings, the live resolution query
  src/functions/_shared.ts        ADMIN_EMAILS, requireAdmin, meta() envelope, errorResponse
  src/functions/tracker.ts        GET /api/tracker            project summaries
  src/functions/projectDetail.ts  GET /api/projects/{id}      vendor checklist + meetings
  src/functions/overrides.ts      GET/POST /api/overrides, DELETE /api/overrides/{pid}/{vendor}
  src/functions/manualVendors.ts  POST /api/manual-vendors, DELETE .../{pid}/{vendor}
  src/functions/settings.ts       GET/POST /api/settings      (+ roster coverage numbers)
  src/functions/misc.ts           /api/health, /api/me, /api/sync-status, /api/unmatched-meetings

dashboard/index.html              single file — Projects / Review Queue / Settings + drilldown modal
fabric/ingest_vendor_compliance.py   bronze + silver; prints the four diagnostics
fabric/build_vendor_gold.py          the four gold tables + the matching engine
docs/setup.md                        the deploy runbook — start here for anything operational
```

## API endpoints

| Endpoint | Returns |
|---|---|
| `GET /api/tracker?scope=active\|all` | one row per project: vendor totals, held, outstanding, %, last prep meeting, unmatched count |
| `GET /api/projects/{id}` | that project's vendor checklist (outstanding first) + every prep meeting + the unmatched ones |
| `GET /api/unmatched-meetings?scope=` | the Review Queue |
| `GET /api/settings` · `POST` (admin) | the four settings + live roster-coverage comparison |
| `GET /api/overrides` · `POST` (admin) · `DELETE /{pid}/{vendor}` | the manual overrides |
| `POST /api/manual-vendors` (admin) · `DELETE /{pid}/{vendor}` | vendors not in either Procore roster |
| `GET /api/admins` · `POST` (admin) · `DELETE /{email}` | the in-app admin list |
| `GET /api/me` · `/api/health` · `/api/sync-status` | identity/admin flag, liveness, mirror freshness |

`?fresh=1` on any read bypasses the in-Function cache and returns `Cache-Control: no-store`.

---

## Known gotchas (inherited — all bit one of the sibling repos already)

- **`mssql`/`tedious` cannot connect to `*.datawarehouse.fabric.microsoft.com`.** Fabric **SQL
  Database** (`*.database.fabric.microsoft.com`) only. Not fixable by any driver option.
- **A count of zero is never evidence on its own.** It is what an empty tenant, a
  permission-filtered endpoint, and a row-dropping filter all look like from the outside. Procore
  list endpoints are permission-filtered (this is how private Observations hid from the safety
  dashboard), and a silver `WHERE x <> ''` will discard a whole ingest without failing. Before
  writing a zero down as a fact, check Procore's **`Total` response header** (`Total > 0` with 0
  rows = withheld, not absent) and compare **bronze rows in against silver rows out**. Both
  checks are built into the ingest now; `fabric/diagnose_commitments.py` does the first ad hoc.
- **Never reference a `dbo.projects` column without probing.** The mirror auto-creates the table,
  so its column set follows whichever `build_gold` ran last; naming a missing column is a parse
  error that blanks the whole dashboard. See `ensureProjectColumnMeta()`.
- **`LIKE '%construction%'` is not "course of construction".** It also matches **"Construction
  Hold"**, **"Post Construction"** and **"Awarded Preconstruction"** (no hyphen). All are excluded
  by `NOT_COURSE_OF_CONSTRUCTION`, kept verbatim in sync with Safety-Dash.
- **Fabric SQL DB has no `TRUNCATE TABLE`** (`Msg 22424`) — pipeline pre-copy scripts must use
  `DELETE FROM` or they silently no-op and the destination stacks a fresh snapshot every night.
- **"Auto create table" is create-if-MISSING, so a widened gold table breaks the Copy.**
  `ErrorCode=SqlColumnNameNotExist … Column 'x' does not exist in the target table` means the
  destination was built from an older gold schema and never altered since; the pre-copy `DELETE`
  empties rows, not columns. Fix: `DROP TABLE IF EXISTS dbo.<the one named>;` and re-run — the
  four mirrored tables are pure mirrors, rebuilt in full from the lakehouse. 🛑 Never drop
  `vendor_settings` / `vendor_prep_overrides` / `vendor_manual_roster`: they are API-managed,
  outside the pipeline by design, and hold the only data here that exists nowhere else.
  (Making the pre-copy script a `DROP` would self-heal this, at the cost of rebuilding the table
  nightly and a brief window where reads find nothing — not worth it for a once-per-schema-change
  problem, so the convention stays `DELETE`.)
- **SWA caps a deployment at ~15,000 files**, and the error is the opaque "Failure during content
  distribution". Count files, not bytes; `.funcignore` does *not* shrink what SWA zips. This API
  has 2 runtime deps to stay well clear — check any new dependency's file count first.
- **Idle pools die.** This app is read a few times a day, so its pool sits idle for hours;
  `client.ts` detects connection-shaped errors, resets the pool and retries once.
- **Notebook cell drift** — edits to `fabric/*.py` don't reach pasted notebook cells. Re-paste.

## Next steps

1. Run the ingest and **read the four diagnostics** — they decide the vendor source, whether the
   template-id filter can be tightened, and whether the attendee shape guesses were right.
2. Build gold, create the 4-activity mirror pipeline, stand up the SWA (`docs/setup.md`).
3. Set the vendor source from the coverage numbers.
4. Work the Review Queue down; it's the honest measure of how much the tracker can't see.

**Branch this session:** `claude/vendor-compliance-tracker-ftzygj`.
