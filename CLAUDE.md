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

**Built end-to-end this session, from an empty repo.** Nothing has been run against live Procore
data yet, so the numbers below are all "should" — see *What is unverified* at the end.

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
   live `vendorSource` setting, defaulting to **`either`** (the union). We do not yet know which
   tool BCI actually keeps current — the ingest's **COVERAGE DIAGNOSTIC** and the settings screen
   both answer that empirically once real data lands. `either` is the default because it is the
   only value that cannot silently *hide* a vendor.

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
| `vendorSource` | `either` | Only value that can't hide a vendor. Narrow after reading the coverage numbers. |
| `allowTitleMatch` | **on** | This is how BCI names these meetings. Off drops most historical matches. |
| `requireVendorPresent` | **off** | The Present / Absent / For Distribution Only radio is frequently left at its default in the field. Switching this on before checking the data makes real meetings read as missed. Applies to attendee matches only — a title match has no attendance to inspect. |
| `requireMeetingHeld` | **off** | Procore's `held` flag is rarely flipped; requiring it makes nearly everything read "not held". |

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

- `api/` `tsc --noEmit` clean; both fabric cells pass `py_compile`; dashboard `<script>` passes
  `node --check`; all JSON + the workflow YAML parse.
- **256 emitted SQL statements** captured with the db stubbed, across **all 24 setting
  permutations** (3 vendor sources × 8 flag combinations) plus every write path — checked for
  balanced parentheses, bind parameters used but not bound, and references to CTEs that don't
  exist. Zero problems.
- **Normalization parity** proven three-way (both Python copies == TypeScript) across 26 names.
- **Title matching** proven against the nine real meeting titles from the Procore screenshots —
  clean diagonal, plus explicit negative controls (`Excel` ∌ `Excelsior`, `ZIP` ∌ `Zipper`,
  `Clark Construction` ∌ "Clark FDG assignment").

### ⚠️ What is NOT verified — read before trusting a number

Nothing here has touched live Procore data. Specifically:

1. **The attendee shape is a guess.** BCI's Procore returns attendees with Name / Company /
   Present / Absent / For Distribution Only / Conference (per the screenshot), but the JSON keys
   behind those columns aren't documented and I couldn't call the API. `attendee_company()` and
   `attendance_status()` probe a **list of candidate keys** and every attendee keeps its
   `raw_json`, so a wrong guess is recoverable by extending the list — no re-ingest of anything
   else. The ingest prints an **attendee company coverage** and **attendance status shapes**
   diagnostic precisely to catch this on the first run.
2. **The template id may not be exposed.** `383995` comes from the create URL. Whether the API
   returns it on a meeting record is unknown, so the prep filter is currently the title substring
   `"prep"` — which does match all nine real titles. The ingest reports every template id it saw;
   if `383995` appears, flip `PREP_REQUIRE_TEMPLATE_ID = True` for an exact filter.
3. **Which vendor roster is real** — the whole point of the coverage diagnostic.
4. **`dbo.projects` column assumptions.** Confirmed against `build_gold.py`: it has
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
| `GET /api/me` · `/api/health` · `/api/sync-status` | identity/admin flag, liveness, mirror freshness |

`?fresh=1` on any read bypasses the in-Function cache and returns `Cache-Control: no-store`.

---

## Known gotchas (inherited — all bit one of the sibling repos already)

- **`mssql`/`tedious` cannot connect to `*.datawarehouse.fabric.microsoft.com`.** Fabric **SQL
  Database** (`*.database.fabric.microsoft.com`) only. Not fixable by any driver option.
- **Never reference a `dbo.projects` column without probing.** The mirror auto-creates the table,
  so its column set follows whichever `build_gold` ran last; naming a missing column is a parse
  error that blanks the whole dashboard. See `ensureProjectColumnMeta()`.
- **`LIKE '%construction%'` is not "course of construction".** It also matches **"Construction
  Hold"**, **"Post Construction"** and **"Awarded Preconstruction"** (no hyphen). All are excluded
  by `NOT_COURSE_OF_CONSTRUCTION`, kept verbatim in sync with Safety-Dash.
- **Fabric SQL DB has no `TRUNCATE TABLE`** (`Msg 22424`) — pipeline pre-copy scripts must use
  `DELETE FROM` or they silently no-op and the destination stacks a fresh snapshot every night.
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
