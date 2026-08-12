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

## 1. Run the ingest (Fabric notebook)

Paste `fabric/ingest_vendor_compliance.py` into a cell in the same notebook as the Procore
ingestion cells. Run **`main_ingestion.py` first** — this cell reuses its auto-refreshing token
manager when present, and bootstraps its own from Key Vault when not.

First run: leave the defaults. It pulls meetings, commitments and directory vendors for every
project dated on/after `PROJECTS_SINCE` (2024-01-01).

**It only fetches meeting *details* for meetings whose title matches a prep pattern**, which is
what keeps this to minutes rather than the hours the Safety Dashboard's all-meetings pull takes.

### Read the diagnostics at the bottom — they answer three questions the code can't

| Diagnostic | What to do with it |
|---|---|
| **COVERAGE DIAGNOSTIC** | Tells you whether BCI actually maintains commitments, the project directory, or both. Whichever covers more **projects** is the one your teams keep current — set the tracker's *vendor source* setting to match (step 5). |
| **meeting template id exposure** | If template id `383995` shows up, the API exposes it and you can set `PREP_REQUIRE_TEMPLATE_ID = True` for an exact filter instead of the `"prep"` title heuristic. If nothing appears, leave it `False`. |
| **attendee company coverage** | If `missing_company` is high, `attendee_company()` is looking in the wrong key for your tenant. Inspect a `raw_json` row in `bronze_vendor_meeting_attendees` and extend the candidate list. Title matching still works meanwhile, at lower confidence. |
| **attendance status shapes** | If most rows are `unknown`, the Present/Absent/Distribution field is in a shape `attendance_status()` doesn't recognize. Leave *Require the vendor to be marked Present* **off** until this is clean. |

⚠️ **Do not set `INCLUDE_ALL_PROJECTS = False`.** The bronze write is a full overwrite, so
narrowing the scope deletes every out-of-scope project's history. To cheapen the nightly run use
`SKIP_COMPLETED_PROJECTS = True`, which carries skipped projects' rows forward through a
project-level merge.

## 2. Build gold

Paste and run `fabric/build_vendor_gold.py`. Builds four tables and prints the match breakdown.

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

Schedule it right after the existing nightly Procore run (the capacity is shared — keep it in the
2–3 AM window, not during work hours).

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

5. **Entra app registration** for sign-in: redirect URI
   `https://<your-swa-host>/.auth/login/aad/callback`, enable **ID tokens**, create a client
   secret, and put the **Secret Value** (the long random string — *not* the Secret ID GUID, which
   produces `AADSTS700054`) into `AAD_CLIENT_SECRET`.

6. The service principal needs **both** grants in the Fabric portal, or every call returns
   `Login failed for user '<token-identified principal>'`:
   - workspace → **Manage access** → SP as Contributor or Member
   - the Safety-Dash SQL DB item → **⋯ → Manage permissions** → **Read all data** + **Write all data**

   Write is genuinely required here (unlike the Safety Dashboard's read-only API) — this app
   creates and writes its four admin tables.

## 5. Configure the tracker

Sign in as an admin (see `ADMIN_EMAILS` in `api/src/functions/_shared.ts`) → **Settings**.

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

1. `main_ingestion.py` (Procore projects — shared with the other dashboards)
2. `ingest_vendor_compliance.py`
3. `build_vendor_gold.py`
4. pipeline `mirror-vendor-to-sql`
5. The API auto-deploys on push; hard-refresh the dashboard after a UI change.

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
