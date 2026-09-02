# Setup & Deploy Runbook

Everything needed to take this repo from "code exists" to "the safety team is using it."
Do these in order — the app returns a clear 503 with instructions until steps 1–3 are done.

---

## 0. What you're standing up

```
Procore  ──(fabric/ingest_vendor_compliance.py)──▶  Lakehouse bronze_/silver_vendor_*
                                                        │
                                (fabric/build_vendor_gold.py)
                                                        ▼
                                              Lakehouse gold_vendor_*
                                                        │
                                    (pipeline: mirror-vendor-to-sql, 4 copies)
                                                        ▼
                             Safety-Dash Fabric SQL DB  dbo.vendor_*
                                                        │
                                              (api/ — SWA managed Functions)
                                                        ▼
                                          dashboard/index.html
```

**The tracker shares the Safety-Dash SQL database.** It does not get its own. Two reasons:
Fabric's `Sql Usage` meter looks allocation-based (an idle DB still bills a flat share) and the
F4 capacity already breaches its interactive-delay threshold several times a fortnight — and
`dbo.projects` already lives there, so joining against it is what makes "the same active projects
as the other dashboards" true by construction rather than by a second definition that drifts.

---

## 1. Run the ingest (its own new Fabric notebook)

**Create a NEW notebook — don't add cells to the existing Procore notebooks.** Suggested name:
**`Vendor Compliance - Prep Meetings`**. Attach it to the same Lakehouse the other Procore
notebooks use, so the mirror pipeline's source tables sit where you'd expect them.

Two cells:

| Cell | Source file |
|---|---|
| 1 | `fabric/ingest_vendor_compliance.py` |
| 2 | `fabric/build_vendor_gold.py` |

**This notebook is fully standalone.** It mints its own Procore token from Key Vault
(`procore-client-id` / `-secret` / `-company-id`) and fetches its own project list, so it does
**not** require `main_ingestion.py` or any other notebook to have run first. Keeping it separate
means a vendor-tracker failure can't take down the safety dashboard's nightly run, and vice versa.

The one thing it *does* rely on is `dbo.projects` in the Safety-Dash SQL DB being populated — that
comes from the existing **Procore Nightly incremental** → **mirror-procore-to-safety** chain, which
already runs. Nothing to do; just be aware that a brand-new project won't appear in the tracker
until that chain has seen it.

First run: leave the defaults. `ACTIVE_PROJECTS_ONLY = True` scopes it to jobs that are still
live — typically ~70 of the ~250 projects since 2024 — because a prep meeting on a job that
finished last year is never scored. Pre-construction and awarded jobs are **kept**: a preparatory
meeting happens *before* the vendor starts work, so those are exactly where they're being held now.

Set `ACTIVE_PROJECTS_ONLY = False` only for a one-off historical backfill. Narrowing the scope is
safe either way — out-of-scope projects keep their existing rows via the project-level merge.

**If it spends most of its time in rate-limit pauses**, the run prints a warning telling you so.
The usual cause is another notebook pulling from Procore at the same time — **Procore Nightly
incremental** and **Procore Friday Full Sweep** share the same hourly API budget. Stagger them, or
prove the run works first with `VENDOR_TEST_LIMIT = 20`.

**It only fetches meeting *details* for meetings whose title matches a prep pattern**, which is
what keeps this to minutes rather than the hours the Safety Dashboard's all-meetings pull takes.

### Read the diagnostics at the bottom — they answer three questions the code can't

| Diagnostic | What to do with it |
|---|---|
| **COVERAGE DIAGNOSTIC** | Tells you whether BCI actually maintains commitments, the project directory, or both. Whichever covers more **projects** is the one your teams keep current — set the tracker's *vendor source* setting to match (step 5). ⚠️ Never read a `0` here on its own: it means "no roster rows", which is what both an empty tenant *and* a broken extraction look like. Read the next block first. |
| **COMMITMENT VENDOR RESOLUTION** | Contracts fetched → vendors resolved → roster rows out. If contracts came back but the vendor count is 0, Procore is returning a contract shape this code doesn't recognize — print a `raw_json` from `bronze_vendor_commitments` (the block tells you how) and send it over. This block exists because exactly that happened: 1500 contracts were ingested, all lost their vendor name, and the resulting zero was mistaken for "BCI has no commitments". |
| **meeting template id exposure** | If template id `383995` shows up, the API exposes it and you can set `PREP_REQUIRE_TEMPLATE_ID = True` for an exact filter instead of the `"prep"` title heuristic. If nothing appears, leave it `False`. |
| **attendee company coverage** | If `missing_company` is high, `attendee_company()` is looking in the wrong key for your tenant. Inspect a `raw_json` row in `bronze_vendor_meeting_attendees` and extend the candidate list. Title matching still works meanwhile, at lower confidence. |
| **attendance status shapes** | If most rows are `unknown`, the Present/Absent/Distribution field is in a shape `attendance_status()` doesn't recognize. Leave *Require the vendor to be marked Present* **off** until this is clean. |

⚠️ **Do not set `INCLUDE_ALL_PROJECTS = False`.** The bronze write is a full overwrite, so
narrowing the scope deletes every out-of-scope project's history. To cheapen the nightly run use
`SKIP_COMPLETED_PROJECTS = True`, which carries skipped projects' rows forward through a
project-level merge.

## 2. Build gold

Run cell 2 (`fabric/build_vendor_gold.py`). Builds four tables and prints the match breakdown.

Pay attention to two of its diagnostics:

- **`title_only`** — vendors whose prep meeting was identified from the meeting title alone,
  because nobody added the sub to the attendee list. Sanity-check a few; this number is also the
  argument for asking supers to fill in attendees.
- **UNMATCHED prep meetings** — meetings that were held and logged but can't be credited to any
  vendor. These also appear in the dashboard's Review Queue.

## 3. Create the mirror pipeline

New Fabric Data Pipeline named **`mirror-vendor-to-sql`**. Four **Add copy data activity** steps
(not Copy Assistant, not Copy Job). Source = the Lakehouse; destination = the **Safety-Dash** SQL
database, schema `dbo`:

| Source (Lakehouse) | Destination (`dbo`) | Pre-copy script |
|---|---|---|
| `gold_vendor_roster` | `vendor_roster` | `IF OBJECT_ID('dbo.vendor_roster','U') IS NOT NULL DELETE FROM dbo.vendor_roster;` |
| `gold_vendor_prep_meetings` | `vendor_prep_meetings` | `IF OBJECT_ID('dbo.vendor_prep_meetings','U') IS NOT NULL DELETE FROM dbo.vendor_prep_meetings;` |
| `gold_vendor_prep_attendees` | `vendor_prep_attendees` | `IF OBJECT_ID('dbo.vendor_prep_attendees','U') IS NOT NULL DELETE FROM dbo.vendor_prep_attendees;` |
| `gold_vendor_prep_matches` | `vendor_prep_matches` | `IF OBJECT_ID('dbo.vendor_prep_matches','U') IS NOT NULL DELETE FROM dbo.vendor_prep_matches;` |

On every activity: **Destination → Advanced → Table option → Auto create table** ✅.

⚠️ **`DELETE FROM`, never `TRUNCATE TABLE`.** Fabric SQL DB rejects TRUNCATE
(`Msg 22424`) and the pre-copy step silently no-ops — which is how the intranet's
`silver_procore_project_users` quietly grew to five stacked snapshots.

### ⚠️ `SqlColumnNameNotExist` — a gold table grew a column

```
ErrorCode=SqlColumnNameNotExist … Column 'title_padded' does not exist in the
target table '[dbo].[vendor_prep_meetings]'.
```

**Auto create table is create-if-MISSING.** It builds the destination the first
time and never touches it again — so once a gold table gains a column, the Copy
keeps writing into last week's schema and fails on the new one. The pre-copy
`DELETE` doesn't help: it empties rows, not columns.

**Fix: drop the one destination table and re-run the pipeline.** Auto-create
rebuilds it from the current gold schema on the next run.

```sql
-- Safe: these four are pure mirrors, rebuilt in full from the lakehouse on
-- every run. They hold nothing the notebook can't regenerate.
DROP TABLE IF EXISTS dbo.vendor_prep_meetings;
```

🛑 **Never drop `dbo.vendor_settings`, `dbo.vendor_prep_overrides` or
`dbo.vendor_manual_roster`.** Those are API-managed, deliberately outside the
pipeline, and hold every manual override and hand-added vendor — the one thing
here that exists nowhere else. If in doubt, drop only tables named in the table
above.

If several activities fail this way at once, drop each named table and re-run —
or, when you want a clean slate after a schema change:

```sql
DROP TABLE IF EXISTS dbo.vendor_roster;
DROP TABLE IF EXISTS dbo.vendor_prep_meetings;
DROP TABLE IF EXISTS dbo.vendor_prep_attendees;
DROP TABLE IF EXISTS dbo.vendor_prep_matches;
```

## 3b. When to run it

Two schedules: the **notebook** (`Vendor Compliance - Prep Meetings` — both cells run in one
execution, ingest then gold) and the **pipeline**. Recommended:

| | When | Runtime |
|---|---|---|
| Notebook | **3:30 AM daily** | ~9 min (8.5 min measured over 88 projects, 577 API calls) |
| `mirror-vendor-to-sql` | **4:00 AM daily** | seconds — the four tables total ~3.5k rows |

Three things fix that window, and only one of them is about this app:

1. **Procore's rate limit is company-wide, not per-app.** The ceiling is ~3,600 requests/hour and
   this ingest spends 577 of them. Overlapping the Safety Dashboard's ingest means the two throttle
   each other, and the reactive backoff turns a 9-minute run into an hour. Start after that run
   has *finished*, not after it has started.
2. **`dbo.projects` has to be fresh first.** The active-project filter and every project name come
   from Safety-Dash's mirror of that table. Run before it and the tracker is scoped to yesterday's
   project list — not broken, but a new job won't appear until the day after.
3. **The F4 capacity already breaches its interactive-delay threshold on weekdays.** Anything
   started in work hours competes with the dashboards people are actually reading.

⚠️ **Adjust 3:30 to fit your own Procore run.** If the big `Procore Data - General, Teams, Safety`
notebook starts at midnight and runs ~3h15m, it clears around 3:15 and 3:30 works. If you move it,
move this. Everything here is a consequence of that job's finish time, not of a magic hour.

**The 30-minute gap is deliberate even though the notebook takes nine.** `build_vendor_gold.py`
uses `CREATE OR REPLACE TABLE`; a Copy activity reading a gold table mid-replace either fails or
mirrors a half-built table, and a *wrong* dashboard is worse than one that is a day stale. If the
ingest ever slows past the gap, widen it rather than trimming it.

Daily is more than the data needs — 63 prep meetings exist across all history — but the run is
cheap enough that a fixed daily slot beats reasoning about staleness. Weekdays-only is a fine
variation; nobody logs a prep meeting on Sunday.

## 4. Create the Static Web App

1. Azure Portal → **Create Static Web App**, plan **Standard** (needed for the Entra auth config).
2. Source = this repo, branch `main`.
   - App location `dashboard`, Api location `api`, Output location *(blank)*.
3. Take the deploy token → GitHub repo → Settings → Secrets → Actions → new secret
   **`AZURE_STATIC_WEB_APPS_API_TOKEN`**. (The workflow uses that exact generic name.)
4. **App settings** (Configuration blade) — the API reads all six:

   | Setting | Value |
   |---|---|
   | `FABRIC_SQL_SERVER` | `54jvo5wifiiejghqbdvjkuwfay-rqmkwr6prz3uvivvev3rm3fgpa.database.fabric.microsoft.com` |
   | `FABRIC_SQL_DATABASE` | `Safety-Dash-31f98b13-1c52-43d1-9634-75a2af0ff017` |
   | `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` / `AZURE_TENANT_ID` | the same service principal the Safety Dashboard uses |
   | `AAD_CLIENT_ID` / `AAD_CLIENT_SECRET` | a new Entra app registration for sign-in (below) |

   ⚠️ `FABRIC_SQL_SERVER` **must** be the `*.database.fabric.microsoft.com` host. The
   `*.datawarehouse.fabric.microsoft.com` analytics endpoint cannot be reached by
   `mssql`/`tedious` at all — it fails with `ESOCKET` about 20ms after the TLS handshake and no
   auth type, driver option or retry fixes it.

5. **Entra app registration** for sign-in.

   | Setting | Value |
   |---|---|
   | Redirect URI (Web) | `https://<your-swa-host>/.auth/login/aad/callback` |
   | Authentication → **ID tokens** | ✅ enabled |
   | API permissions | **`User.Read`** (delegated, Microsoft Graph) — added by default, and it is the *only* one needed |
   | Certificates & secrets | new client secret → `AAD_CLIENT_SECRET` |

   ⚠️ Copy the secret's **Value**, not the Secret **ID** (a GUID) — the GUID gives `AADSTS700054`.

   Sign-in is plain OpenID Connect. `openid` / `profile` / `email` are OIDC scopes, not Graph
   permissions, so there is nothing else to consent to. **But** Buffalo's tenant is set to *"Do not
   allow user consent"*, so even `User.Read` can raise a "Need admin approval" wall on first
   sign-in — IT clicks **Grant admin consent** once on the app registration and it's done for
   everyone.

   **Add the `email` optional claim** (Token configuration → Add optional claim → ID → `email`).
   The admin allowlist matches the signed-in principal's address; if the token carries no readable
   email or UPN claim there is nothing to match and nobody can administer the app. `GET /api/me`
   reports `emails_seen` so you can confirm the claim is arriving.

6. The service principal needs **both** grants in the Fabric portal, or every call returns
   `Login failed for user '<token-identified principal>'`:
   - workspace → **Manage access** → SP as Contributor or Member
   - the Safety-Dash SQL DB item → **⋯ → Manage permissions** → **Read all data** + **Write all data**

   Write is genuinely required here (unlike the Safety Dashboard's read-only API) — this app
   creates and writes its four admin tables.

## 5. Configure the tracker

Sign in as an admin → **Settings**.

**Admin access.** `adminMode` defaults to **`allowlist`**: only
`cory.zilisch@buffaloconstruction.com` and `justin.houston@buffaloconstruction.com` can edit;
everyone else who signs in is read-only. More admins can be added in Settings → Admin access.
Those two are the code-level `BOOTSTRAP_ADMINS` and cannot be removed through the UI, so the list
can never be emptied.

**If you get locked out** — the likeliest cause is an Entra token with no email claim, so the
allowlist has nothing to match. Check `GET /api/me` → `emails_seen`. To recover without a deploy,
add an app setting **`ADMIN_MODE`** = **`open`** in the SWA Configuration blade; it overrides the
stored setting and makes every signed-in user an admin. Remove it once sign-in is fixed.

The settings screen shows the same coverage numbers the ingest printed. Set **vendor source** to
whichever roster covers more projects. Leave the other three at their defaults until you've
checked the data:

| Setting | Default | Why that default |
|---|---|---|
| Vendor source | **Both** | The only value that cannot silently *hide* a vendor. Narrow it once you've seen the coverage numbers. |
| Accept a title match | **On** | This is how BCI names these meetings. Off drops most historical matches. |
| Require vendor marked Present | **Off** | The Present/Absent/Distribution radio is often left at its default in the field. Turn on only once the attendance diagnostic is clean, or real meetings start reading as missed. |
| Require Procore's "held" flag | **Off** | Rarely flipped. On makes almost everything read "not held". |

---

## Admin tables (auto-created, never mirrored)

The API creates these on first use — no migration to run:

| Table | Purpose |
|---|---|
| `dbo.vendor_settings` | The four settings above |
| `dbo.vendor_prep_overrides` | Per-vendor `held` / `not_held` / `not_applicable` with note + author |
| `dbo.vendor_manual_roster` | Vendors added by hand that aren't in either Procore roster |

They are **not** in the mirror pipeline on purpose — a Copy activity's pre-copy `DELETE` would
wipe every override on the next nightly run.

---

## Nightly operating order

The `Vendor Compliance - Prep Meetings` notebook runs **independently** of the Procore/Safety
chain — no ordering dependency between them, so schedule it wherever it fits in the off-hours
window:

1. `Vendor Compliance - Prep Meetings` — cell 1 (ingest), cell 2 (gold)
2. pipeline `mirror-vendor-to-sql`
3. The API auto-deploys on push; hard-refresh the dashboard after a UI change.

Run it after the existing **Procore Nightly incremental** / **mirror-procore-to-safety** chain only
so a newly-added project reaches `dbo.projects` before the tracker looks for it — roughly 2:15 AM
if that chain starts at 2:00. Capacity is shared with the Safety Dashboard, the intranet and the
Permit hub, so keep everything out of business hours.

**Notebook cell drift:** edits to `fabric/*.py` in this repo do **not** reach your pasted notebook
cells. Re-paste after pulling changes.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| 503 "the vendor tables aren't in the Safety-Dash SQL DB yet" | Steps 1–3 haven't completed. The message names the exact cells. |
| `Invalid object name 'dbo.vendor_roster'` in the pipeline | That Copy activity has Auto-create-table **off**, or the destination table name still reads `gold_vendor_roster`. |
| Every vendor reads "not held" | Almost always the normalization keys diverging. `normalize_company` exists in **three** places (both fabric cells and `api/src/normalize.ts`) and they must stay byte-identical — the Python copies write the keys, the TypeScript one reads names humans type. |
| A project shows "No vendors" | Neither Procore roster has companies for it, or the vendor-source setting is filtering them out. Check Settings, or add vendors manually from the project modal. |
| Projects you expect are missing | The tracker shows Course-of-Construction only. Note `LIKE '%construction%'` also matches **"Construction Hold"** and **"Post Construction"**, both deliberately excluded — switch the scope selector to *All projects* to see them. |
| `Login failed for '<token-identified principal>'` | One of the two Fabric grants in step 4.6 was reset. Both are required. |
| Deploy fails "Failure during content distribution" | SWA caps a deployment at ~15,000 files. Count files (`find api/node_modules -type f | wc -l`), don't look at bytes. This API installs 2 runtime deps precisely to stay well under it — check any new dependency's file count before adding it. |
