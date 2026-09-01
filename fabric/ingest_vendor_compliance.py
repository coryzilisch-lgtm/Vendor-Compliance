# ============================================================
# Fabric Notebook Cell: VENDOR COMPLIANCE INGESTION (bronze + silver)
# ------------------------------------------------------------
# Feeds the Prep-Phase Meeting Tracker. Four pulls, one project loop, in this
# order (the rosters must be built BEFORE meetings — see #4):
#
#   1. Project people  <- GET /rest/v1.0/projects/{id}/users
#                         The attendee -> company bridge. A meeting attendee
#                         carries ONLY {id, status, login_information:{id, login,
#                         name}} — there is no company on it. Procore's UI fills
#                         that column by joining the person against the project
#                         directory, so this does the same.
#   2. Vendor roster A <- GET /rest/v1.0/work_order_contracts?project_id={id}
#                         + /rest/v1.0/purchase_order_contracts   (commitments)
#   3. Vendor roster B <- GET /rest/v1.1/projects/{id}/vendors     (directory)
#   4. Prep meetings   <- GET /rest/v1.1/projects/{id}/meetings   (list, then
#                         detail ONLY for meetings that look like prep meetings —
#                         this is what keeps the run to minutes rather than the
#                         hours an all-meetings detail pull takes)
#
# BOTH vendor rosters are ingested on purpose. Which one drives the tracker's
# checklist is a dashboard setting (`vendorSource`), not a decision baked in here.
#
# ANSWERED 2026-08 against live data: BCI does NOT use Procore commitments —
# work_order_contracts and purchase_order_contracts both return HTTP 200 with
# zero rows on every project, so that is a real absence and not a permission
# error. **The project directory is the vendor roster.** The commitments pull is
# kept (it is 2 cheap calls) so the day someone starts writing subcontracts in
# Procore, the better denominator appears with no code change.
#
# FULLY STANDALONE. Belongs in its OWN notebook — it mints its own Procore token
# from Key Vault and fetches its own project list, so it does not depend on the
# main Procore ingestion notebook having run. (If it IS pasted into a notebook
# where that cell already ran, it reuses that token manager instead of minting a
# second one.) Keeping it separate means a vendor-tracker failure can never take
# down the safety dashboard's nightly run, and vice versa.
#
# Writes:
#   bronze_vendor_meeting_summaries      bronze_vendor_meeting_details
#   bronze_vendor_meeting_attendees      bronze_vendor_commitments
#   bronze_vendor_directory              bronze_vendor_project_users
#   bronze_vendor_sync_errors
#   silver_vendor_prep_meetings          silver_vendor_prep_attendees
#   silver_vendor_roster
#
# Cell 1 of 2. Cell 2 is build_vendor_gold.py. Then run the mirror pipeline.
# ============================================================

import json
import re
import time
import requests
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import current_timestamp, lit

# ============================================================
# 1. Config
# ============================================================

# ---- Which meetings are "preparatory meetings" ------------------------------
# BCI creates these from the Procore meeting template at
#   .../tools/meetings/create/383995
# but the superintendent retitles each one with the vendor's name, so titles are
# free-text and inconsistent in the real data:
#
#   "Preparatory Meeting - ZIP"                        <- the common shape
#   "Preparatory Meeting- East and Westbrook"          <- no space before the dash
#   "FDG Prep phase meeting - Blake Daher"
#   "Clark FDG assignment - Preparatory Meeting Agenda"
#
# Every one of them contains "prep", so that substring is the filter. It is
# applied to the LIST response, so we only pay for a detail call on meetings that
# already look like prep meetings.
#
# The title alone is NOT sufficient — see PREP_MATCH_MODE below. Confirmed
# against live data: 7 of 10 real prep meetings had no "prep" in the title.
PREP_TITLE_PATTERNS = [r"\bprep"]          # case-insensitive regex, ANY match wins
PREP_MEETING_TEMPLATE_ID = 383995          # from the create URL

# ---- How a meeting is identified as a prep meeting --------------------------
# CONFIRMED 2026-08 against live data: the API **does** expose the template id,
# and it is strictly better than the title heuristic. In a 20-project sample the
# title filter found 3 prep meetings while template 383995 was on 10 — seven
# meetings were created from the prep template but titled something without the
# word "prep" in it.
#
#   "template_or_title"  (default) UNION of both. Catches every meeting off the
#                        prep template, PLUS any prep meeting created some other
#                        way but titled like one. Most complete.
#   "template"           template id only. Most precise; drops a correctly-named
#                        meeting that was built from a different template.
#   "title"              the old heuristic. Kept for a tenant that doesn't
#                        expose the template id.
#
# ⚠️ An earlier version of this had a `PREP_REQUIRE_TEMPLATE_ID` flag that
# AND-ed the two tests. That was backwards — it returned the intersection, so
# switching it on made the result *worse* (3 instead of 10). It's a union.
PREP_MATCH_MODE = "template_or_title"

# Candidate keys that might carry the template id on a meeting record. Procore's
# meetings API is not consistent about this across tenants, so we probe several
# and report which (if any) actually appeared.
TEMPLATE_ID_FIELDS = [
    "meeting_template_id", "template_id", "meeting_template",
    "source_meeting_template_id", "origin_id",
]

THROTTLE_SEC = 0.25
MEETINGS_PER_PAGE = 100
# The project directory runs ~200 companies on a busy job; 1000/page turns that
# from three calls into one. Same trick that gave the incidents ingest its
# biggest speedup.
VENDOR_PAGE_SIZE = 1000
VENDOR_TEST_LIMIT = None       # None = every project in scope; set a number to test

# ---- Rate limiting ----------------------------------------------------------
# Pause only when the budget is genuinely almost gone. Procore's window here
# resets every few seconds, so `remaining` hovers near its floor during a busy
# run — a high threshold makes the ingest sleep on nearly every call for no
# reason. The 429 handler is the real backstop.
RATE_LIMIT_FLOOR     = 3
RATE_LIMIT_MAX_SLEEP = 90      # never block longer than this on one call

_stats = {"calls": 0, "sleep": 0.0, "pauses": 0, "remaining": None}

# Diagnostics collected during the run and printed at the end.
_prep_source   = {}   # how each prep meeting was identified (template vs title)
_company_paths = {}   # which JSON path yielded each attendee's company
_attendee_keys = {}   # top-level keys seen on attendee objects
_endpoint_status = {} # (endpoint, http status) -> count, to tell 403 from empty
_withheld = {}        # endpoint -> projects where Total exceeded the rows returned
_last_total = {"value": None}  # Procore's Total header from the most recent response
_date_key_samples = {}  # date-ish keys actually present on meeting records

# ---- What to pull -----------------------------------------------------------
PULL_MEETINGS    = True
# MEASURED 2026-08 across 90/90 projects: work_order_contracts and
# purchase_order_contracts both return HTTP 200 with ZERO rows. BCI does not
# write subcontracts in Procore. Those two calls per project were 180 of the
# run's 532 calls — 34% of the API budget — producing nothing.
#
# Left OFF by default. Flip it True occasionally (say monthly, or when someone
# says Procore commitments are being adopted) to re-check; the COVERAGE
# DIAGNOSTIC will show them the moment they exist, and the vendorSource setting
# can then switch to the cleaner denominator with no code change.
PULL_COMMITMENTS = False       # work order + purchase order contracts
PULL_DIRECTORY   = True        # project directory vendor list
# The attendee -> company bridge. A meeting attendee carries only a person id
# and an email, so the company has to come from the project directory people
# list. Turning this off leaves title matching as the only signal.
PULL_PROJECT_USERS = True

# ---- Project scope ----------------------------------------------------------
# Mirrors ingest_safety.py: build scope from the FULL project list so completed
# jobs keep their prep-meeting history.
#
# ⚠️ Do NOT set INCLUDE_ALL_PROJECTS = False to cheapen the run. The bronze write
# is a full mode("overwrite"), so narrowing the scope DELETES every out-of-scope
# project's rows. Use SKIP_COMPLETED_PROJECTS instead — it narrows what gets
# re-fetched while carrying skipped projects' rows forward via a project-level
# merge.
INCLUDE_ALL_PROJECTS = True
PROJECTS_SINCE       = "2024-01-01"

# ---- Only pull jobs the tracker can actually display ------------------------
# THIS IS THE SETTING THAT CONTROLS RUNTIME. The tracker is a *current*
# operational checklist: it shows Course-of-Construction projects, and a prep
# meeting on a job that finished last year is never scored. Pulling the full
# 2024-forward history therefore spends ~4 API calls per project on ~170
# projects whose rows nothing will ever read.
#
# True  => keep only projects Procore still marks active AND whose stage isn't
#          finished/pre-construction. Typically cuts ~245 projects to ~70 and
#          the run from hours to minutes.
# False => the full PROJECTS_SINCE window. Use for a one-off historical
#          backfill, or if you later want the tracker's "All projects" scope to
#          have real data behind it.
#
# Safe to flip either way: it only narrows what gets FETCHED. Rows for projects
# outside the scope are preserved by the project-level merge (merge_ids below),
# exactly like SKIP_COMPLETED_PROJECTS.
ACTIVE_PROJECTS_ONLY = True

# Stage strings that mean this job will never need a prep meeting again —
# it is finished, dead, or paused.
#
# Note what is deliberately NOT here: **preconstruction and awarded jobs are
# KEPT.** A preparatory meeting happens *before* the vendor starts work, so
# those are exactly the jobs where prep meetings are being held right now. They
# won't show on the tracker until the stage flips to Course of Construction, but
# by then their meetings are already ingested and the checklist is correct on
# day one instead of a day late.
FINISHED_STAGE_HINTS = [
    "post construction", "post-construction", "postconstruction",
    "closeout", "close out", "complete", "warranty",
    "final payment",             # "Waiting on Final Payment" — work is done
    "hold",                      # "Construction Hold" — paused mid-flight
    "bidding", "lost", "pending",
]

SKIP_COMPLETED_PROJECTS = False
COMPLETED_GRACE_DAYS    = 120

# ---- Targeted re-ingest -----------------------------------------------------
# Non-empty => pull ONLY these projects and merge them back over the existing
# bronze rows (every other project is preserved). Set back to [] when done.
ONLY_PROJECT_IDS = []          # e.g. [3408160, 3078043]

# ---- The general contractor --------------------------------------------------
# Attendees from this company are BCI's own people, never "the vendor". Matched
# case-insensitively after normalization, so "Buffalo Construction, Inc." and
# "Buffalo Construction Inc" both land here.
GC_COMPANY_NAMES = ["buffalo construction"]

run_started_at = datetime.now(timezone.utc).isoformat()

# ============================================================
# 2. Bootstrap auth (reuse the main cell when present)
# ============================================================
_g = globals()
if all(k in _g for k in ["get_token", "auth_headers", "company_id", "PROCORE_API_BASE_URL"]):
    print("Reusing token manager from the main ingestion cell.")
else:
    print("Bootstrapping auth from Key Vault...")
    vault_url = "https://kv-dataplatform-bci.vault.azure.net/"
    client_id     = notebookutils.credentials.getSecret(vault_url, "procore-client-id")
    client_secret = notebookutils.credentials.getSecret(vault_url, "procore-client-secret")
    company_id    = notebookutils.credentials.getSecret(vault_url, "procore-company-id")
    PROCORE_API_BASE_URL   = "https://api.procore.com"
    PROCORE_LOGIN_BASE_URL = "https://login.procore.com"
    REQUEST_TIMEOUT_SECONDS = 120

    _token = {"value": None, "exp": 0.0}

    def get_token(force=False):
        if not force and _token["value"] and time.time() < _token["exp"] - 300:
            return _token["value"]
        resp = requests.post(
            f"{PROCORE_LOGIN_BASE_URL}/oauth/token",
            data={"grant_type": "client_credentials",
                  "client_id": client_id, "client_secret": client_secret},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=REQUEST_TIMEOUT_SECONDS)
        resp.raise_for_status()
        j = resp.json()
        _token["value"] = j["access_token"]
        _token["exp"]   = time.time() + int(j.get("expires_in", 7200))
        print(f"Procore token refreshed; valid ~{int(j.get('expires_in', 7200)) // 60} min")
        return _token["value"]

    def auth_headers():
        return {"Authorization": f"Bearer {get_token()}",
                "Procore-Company-Id": str(company_id),
                "Content-Type": "application/json"}

    get_token()

REQUEST_TIMEOUT_SECONDS = _g.get("REQUEST_TIMEOUT_SECONDS", 120)

# ============================================================
# 3. HTTP helpers (401 refresh, 429 backoff, rate-limit pacing)
# ============================================================
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 5


def safe_json_dumps(value):
    try:
        return json.dumps(value, default=str)
    except Exception:
        return json.dumps(str(value))


def request_json(url, params=None, allow_404=True):
    """Single GET. Returns (payload, status). Never raises on 403/404 — a project
    with the tool disabled is normal and must not abort the whole run."""
    last_exc, response = None, None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            time.sleep(THROTTLE_SEC)
            response = requests.get(url, headers=auth_headers(), params=params,
                                    timeout=REQUEST_TIMEOUT_SECONDS)
            if response.status_code == 401:
                get_token(force=True)
                if attempt < MAX_RETRIES:
                    continue
            if response.status_code == 429:
                reset_ts = int(response.headers.get("X-Rate-Limit-Reset", 0))
                wait = max(10, reset_ts - time.time()) + 3
                # Count this against the pause budget. An earlier version didn't,
                # so a run that spent 21 of its 28 minutes asleep on one 429
                # cheerfully reported "5% of the run" — the single most
                # misleading number the ingest produced.
                _stats["sleep"] += wait
                _stats["pauses"] += 1
                print(f"    429 — hourly budget exhausted, sleeping {wait/60:.1f} min "
                      f"until the window resets...")
                time.sleep(wait)
                continue
            if allow_404 and response.status_code in (403, 404):
                # Tool not enabled / no permission on this project. Not an error.
                return None, response.status_code
            if response.status_code in (500, 502, 503, 504) and attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
                continue
            response.raise_for_status()

            # Proactive pacing so a long run never bursts into continuous 429s.
            #
            # The threshold is deliberately LOW. Procore's window here resets
            # every few seconds, not hourly, so `remaining` sits near its floor
            # for most of a busy run — an earlier version tripped at <= 15 and
            # added a +3s buffer, which made it sleep on nearly every call and
            # turned a ~10 minute run into an all-day one. We now only pause when
            # the budget is genuinely almost gone, sleep exactly to the reset,
            # and let the 429 handler above be the real backstop.
            _last_total["value"] = response.headers.get("Total", response.headers.get("total"))
            remaining = int(response.headers.get("X-Rate-Limit-Remaining", 999))
            reset_ts  = int(response.headers.get("X-Rate-Limit-Reset", 0))
            _stats["calls"] += 1
            _stats["remaining"] = remaining
            if remaining <= RATE_LIMIT_FLOOR and reset_ts > 0:
                wait = min(max(0.0, reset_ts - time.time()) + 1, RATE_LIMIT_MAX_SLEEP)
                _stats["sleep"] += wait
                _stats["pauses"] += 1
                # Only narrate a genuinely long pause; the short ones are normal
                # and printing each one buries the progress lines.
                if wait >= 10:
                    print(f"    rate limit floor ({remaining} left) — sleeping {wait:.0f}s...")
                time.sleep(wait)
            return (response.json() if response.text else None), response.status_code
        except Exception as e:
            last_exc = e
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_SLEEP_SECONDS * attempt)
    raise last_exc


def _note_status(label, status, total=None, returned=None):
    key = (label, status)
    _endpoint_status[key] = _endpoint_status.get(key, 0) + 1
    # Procore's `Total` header is the full server-side count. When it exceeds
    # what we were handed, rows are being WITHHELD by tool permissions rather
    # than being absent — the same failure mode that hid private Observations
    # from the safety dashboard. Recording it here means an empty result can
    # never again be mistaken for an empty dataset.
    if total is not None and returned is not None:
        try:
            t = int(total)
        except (TypeError, ValueError):
            return
        if t > returned:
            _withheld[label] = _withheld.get(label, 0) + 1


def get_paginated(url, params=None, per_page=100, max_pages=100, label=None):
    """Follow pages until a short page comes back. Handles both v1.x bare arrays
    and v2.0 {"data": [...]} envelopes."""
    out, page = [], 1
    base = dict(params or {})
    while page <= max_pages:
        payload, status = request_json(url, {**base, "page": page, "per_page": per_page})
        if label and page == 1:
            chunk0 = payload.get("data") if isinstance(payload, dict) else payload
            _note_status(label, status, _last_total.get("value"),
                         len(chunk0) if isinstance(chunk0, list) else 0)
        if payload is None:
            break
        chunk = payload.get("data") if isinstance(payload, dict) else payload
        if not isinstance(chunk, list):
            chunk = [chunk] if chunk else []
        out.extend(chunk)
        if len(chunk) < per_page:
            break
        page += 1
    return out


def write_delta(rows, table_name, empty_cols=None, merge_project_ids=None):
    """Overwrite `table_name` with `rows`.

    When merge_project_ids is supplied we are in targeted/skip mode: keep every
    row whose project_procore_id is NOT in that list and replace only the ones we
    actually re-fetched. Without it a narrowed scope would silently delete every
    out-of-scope project's history.
    """
    print(f"  writing {table_name}: {len(rows)} rows"
          + (f" (merging {len(merge_project_ids)} projects)" if merge_project_ids else ""))

    if not rows and not merge_project_ids:
        if empty_cols:
            schema = StructType([StructField(c, StringType(), True) for c in empty_cols]
                                + [StructField("_fabric_loaded_at", TimestampType(), True)])
            (spark.createDataFrame([], schema).write.mode("overwrite")
                 .format("delta").option("mergeSchema", "true").saveAsTable(table_name))
        return

    keys = []
    for r in rows:
        for k in r.keys():
            if k not in keys:
                keys.append(k)
    if not keys:
        keys = list(empty_cols or ["project_procore_id"])

    schema = StructType([StructField(str(k), StringType(), True) for k in keys])
    string_rows = [{k: (None if r.get(k) is None else str(r.get(k))) for k in keys} for r in rows]

    new_df = (spark.createDataFrame(string_rows, schema=schema)
              .withColumn("_fabric_loaded_at", current_timestamp())
              .withColumn("_notebook_run_started_at", lit(run_started_at))
              .withColumn("_source_system", lit("procore"))
              .withColumn("_company_id", lit(str(company_id))))

    if merge_project_ids and spark.catalog.tableExists(table_name):
        keep_ids = [str(p) for p in merge_project_ids]
        existing = spark.table(table_name)
        if "project_procore_id" in existing.columns:
            kept = existing.filter(~existing.project_procore_id.isin(keep_ids))
            new_df = kept.unionByName(new_df, allowMissingColumns=True)

    (new_df.write.mode("overwrite").format("delta")
        .option("mergeSchema", "true").saveAsTable(table_name))


# ============================================================
# 4. Field-plucking helpers
#
# Procore returns the same logical field under different keys depending on the
# endpoint version and how the tenant is configured, so everything below probes
# a list of candidates rather than assuming one shape. Every record also keeps
# its raw JSON, so a shape we guessed wrong is recoverable without re-ingesting.
# ============================================================

def pick(d, *keys, default=None):
    """First non-empty value among `keys` on dict `d`."""
    if not isinstance(d, dict):
        return default
    for k in keys:
        v = d.get(k)
        if v not in (None, "", [], {}):
            return v
    return default


def pick_name(value):
    """A Procore association is sometimes a nested object, sometimes a bare
    string. Return the display name either way."""
    if isinstance(value, dict):
        return pick(value, "name", "company_name", "display_name", "title", "label")
    if isinstance(value, str):
        return value
    return None


LEGAL_SUFFIX_TOKENS = {
    "llc", "lc", "inc", "incorporated", "co", "corp", "corporation", "company",
    "ltd", "limited", "lp", "llp", "plc", "pllc", "pc", "pa",
}


def normalize_company(name):
    """Canonical form used to match a vendor to a meeting attendee's company.

    Lowercase, ampersand spelled out, punctuation dropped, whitespace collapsed,
    and trailing legal-entity tokens removed — so "Verastegui Brothers Masonry,
    LLC" and "Verastegui Brothers Masonry" collapse to the same key.

    Deliberately does NOT strip industry words like "construction", "masonry" or
    "services": those are what actually distinguish two vendors from each other,
    and stripping them merges companies that are genuinely different.

    ⚠️ This function is mirrored in build_vendor_gold.py. If you change the rules
    here, change them there too — the gold matcher compares keys produced by
    both, and a silent divergence shows up as every vendor reading "not held".
    """
    if not name:
        return ""
    s = str(name).lower()
    s = s.replace("&", " and ")
    # Periods are DELETED, not turned into spaces, so "L.L.C." collapses to "llc"
    # and gets stripped as a suffix. Turning them into spaces gives "l l c" —
    # three tokens that survive the strip, which made "Smith & Sons, L.L.C." and
    # "Smith and Sons LLC" two different vendors. Safe for "St. Louis": the
    # following space survives.
    s = s.replace(".", "")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = s.split()

    def strip_trailing():
        while tokens and tokens[-1] in LEGAL_SUFFIX_TOKENS:
            tokens.pop()

    strip_trailing()

    # Rescue an already-space-separated acronym ("Smith L L C") the period fix
    # above wouldn't have seen.
    i = len(tokens)
    while i > 0 and len(tokens[i - 1]) == 1:
        i -= 1
    if i < len(tokens) and "".join(tokens[i:]) in LEGAL_SUFFIX_TOKENS:
        del tokens[i:]
        strip_trailing()

    return " ".join(tokens)


GC_NORMALIZED = {normalize_company(n) for n in GC_COMPANY_NAMES}
# Email domains belonging to the general contractor, derived from the same list
# so there is one place to edit.
GC_EMAIL_STEMS = {n.replace(" ", "") for n in GC_NORMALIZED if n}


def is_gc_email(email):
    if not email or "@" not in str(email):
        return False
    domain = str(email).rsplit("@", 1)[1].lower()
    stem = re.sub(r"[^a-z0-9]", "", domain.split(".")[0])
    return any(stem == g or (len(stem) >= 6 and (stem in g or g in stem))
               for g in GC_EMAIL_STEMS)


def is_gc(name):
    """True when this company is Buffalo Construction itself (prefix match, so
    'Buffalo Construction Inc.' and 'Buffalo Construction of Ohio' both hit)."""
    n = normalize_company(name)
    return bool(n) and any(n.startswith(g) for g in GC_NORMALIZED if g)


PREP_TITLE_RE = [re.compile(p, re.I) for p in PREP_TITLE_PATTERNS]


def looks_like_prep_meeting(title):
    t = str(title or "")
    return any(rx.search(t) for rx in PREP_TITLE_RE)


# Every key Procore might put the meeting's date under. The first live run came
# back with meeting_date NULL on all 81 meetings, so "date"/"scheduled_date"/
# "meeting_date" are all absent in this tenant — same class of problem as the
# attendee company. DATE_FIELDS is probed in order and the key that actually
# won is reported by the MEETING DATE diagnostic, so this stops being guesswork.
# CONFIRMED 2026-08: this tenant populates **starts_at** (80 of 81 meetings;
# the 81st fell through to created_at). `date`, `scheduled_date` and
# `meeting_date` — the three the first version probed — are never present, which
# is why every meeting came back dateless. starts_at is listed first now so the
# common case doesn't walk the whole list; created_at stays LAST because it is a
# record-creation timestamp, not when the meeting happened, and is only ever a
# last resort.
DATE_FIELDS = [
    "starts_at", "meeting_date", "date", "scheduled_date", "scheduled_at",
    "datetime", "date_time", "start_date", "start_time", "start_at",
    "occurred_at", "held_at", "actual_start_time", "created_at",
]

_date_fields_used = {}


def meeting_date_of(meeting):
    """The meeting's date, from whichever key this tenant populates."""
    if not isinstance(meeting, dict):
        return None
    for key in DATE_FIELDS:
        v = meeting.get(key)
        if isinstance(v, dict):
            v = pick(v, "date", "value", "start", "start_time")
        if v not in (None, "", [], {}):
            _date_fields_used[key] = _date_fields_used.get(key, 0) + 1
            return v
    _date_fields_used["<none>"] = _date_fields_used.get("<none>", 0) + 1
    return None


def date_like_keys(meeting):
    """Any key on the record whose name or value looks like a date. Used only by
    the diagnostic, so a tenant that puts the date somewhere unexpected reports
    itself instead of needing another round-trip."""
    out = {}
    if not isinstance(meeting, dict):
        return out
    for k, v in meeting.items():
        if isinstance(v, (dict, list)) or v in (None, ""):
            continue
        sv = str(v)
        if re.search(r"(date|time|day|when|start|end)", str(k), re.I) or \
           re.match(r"^\d{4}-\d{2}-\d{2}", sv):
            out[str(k)] = sv[:32]
    return out


def template_id_of(meeting):
    for k in TEMPLATE_ID_FIELDS:
        v = meeting.get(k) if isinstance(meeting, dict) else None
        if isinstance(v, dict):
            v = v.get("id")
        if v not in (None, "", []):
            return str(v)
    return None


# ---- Attendee extraction ----------------------------------------------------
# The Procore meetings UI shows one row per attendee with Name, Company, and a
# radio group: Present / Absent / For Distribution Only / Conference. That
# distinction matters here — a vendor listed "For Distribution Only" was sent the
# agenda, they did not sit in the prep meeting — so we keep the raw status and
# let the gold layer decide (see the `requireVendorPresent` setting).

ATTENDANCE_PRESENT_HINTS      = ("present", "attended", "in person", "in_person")
ATTENDANCE_CONFERENCE_HINTS   = ("conference", "phone", "call", "remote", "video")
ATTENDANCE_ABSENT_HINTS       = ("absent", "not attended", "no show")
ATTENDANCE_DISTRIBUTION_HINTS = ("distribution", "for distribution only", "dist only")


def attendance_status(att):
    """Normalize whatever the API gives into
    present | conference | absent | distribution | unknown.

    Handles both the string form (`attendance: "present"`) and the boolean-flag
    form (`attended: true` / `present: true` / `distribution_only: true`)."""
    if not isinstance(att, dict):
        return "unknown"

    raw = pick(att, "attendance", "attendance_status", "status", "attendance_type",
               "presence", "attendee_type")
    if isinstance(raw, dict):
        raw = pick(raw, "name", "value", "label")
    if isinstance(raw, str):
        r = raw.strip().lower()
        for hint in ATTENDANCE_DISTRIBUTION_HINTS:
            if hint in r:
                return "distribution"
        for hint in ATTENDANCE_CONFERENCE_HINTS:
            if hint in r:
                return "conference"
        for hint in ATTENDANCE_ABSENT_HINTS:
            if hint in r:
                return "absent"
        for hint in ATTENDANCE_PRESENT_HINTS:
            if hint in r:
                return "present"

    # Boolean-flag shape.
    if att.get("distribution_only") is True or att.get("for_distribution_only") is True:
        return "distribution"
    if att.get("conference") is True or att.get("via_conference") is True:
        return "conference"
    if att.get("absent") is True:
        return "absent"
    if att.get("attended") is True or att.get("present") is True:
        return "present"
    if att.get("attended") is False or att.get("present") is False:
        return "absent"
    return "unknown"


COMPANY_KEY_RE = re.compile(r"(company|vendor|business|organization|firm)", re.I)


def _deep_company(node, depth=0):
    """Walk the attendee object looking for anything company-shaped, at any
    depth. Returns (name, dotted_path_where_found).

    This exists because the first live run came back with the company missing on
    17 of 17 attendees while attendance status parsed fine — i.e. the data is in
    there, just not under any key that was guessed. Rather than guess again,
    search, and report the path so the shape gets documented instead of
    re-discovered.
    """
    if depth > 4 or not isinstance(node, (dict, list)):
        return None, None
    if isinstance(node, list):
        for item in node:
            n, p = _deep_company(item, depth + 1)
            if n:
                return n, p
        return None, None
    for key, value in node.items():
        if not COMPANY_KEY_RE.search(str(key)):
            continue
        if isinstance(value, str) and value.strip():
            return value, key
        name = pick_name(value)
        if name:
            return name, f"{key}.name"
    for key, value in node.items():
        if isinstance(value, (dict, list)):
            n, p = _deep_company(value, depth + 1)
            if n:
                return n, f"{key}.{p}"
    return None, None


def attendee_company(att):
    """Company name for an attendee, wherever the API decided to put it.

    Tries the documented shapes first (cheap, predictable), then falls back to a
    recursive search. Records which path produced the answer so the run can
    print it — see the ATTENDEE SHAPE diagnostic.
    """
    if not isinstance(att, dict):
        return None
    for key in ("company", "vendor", "business", "organization"):
        n = pick_name(att.get(key))
        if n:
            _company_paths[key] = _company_paths.get(key, 0) + 1
            return n
    direct = pick(att, "company_name", "vendor_name", "business_name", "organization_name")
    if direct:
        _company_paths["<direct>"] = _company_paths.get("<direct>", 0) + 1
        return direct
    for outer in ("login_information", "user", "person", "contact"):
        sub = att.get(outer)
        if isinstance(sub, dict):
            for key in ("company", "vendor", "business"):
                n = pick_name(sub.get(key))
                if n:
                    _company_paths[f"{outer}.{key}"] = _company_paths.get(f"{outer}.{key}", 0) + 1
                    return n
            n = pick(sub, "company_name", "vendor_name")
            if n:
                _company_paths[f"{outer}.company_name"] = _company_paths.get(f"{outer}.company_name", 0) + 1
                return n
    name, path = _deep_company(att)
    if name:
        _company_paths[f"deep:{path}"] = _company_paths.get(f"deep:{path}", 0) + 1
        return name
    # Not on the attendee object. In BCI's tenant that is the normal case — the
    # company is resolved by the directory join afterwards — so this is a
    # neutral observation, not a failure. The real failure counter is
    # "resolved:unresolved".
    _company_paths["on_object:absent"] = _company_paths.get("on_object:absent", 0) + 1
    return None


def attendee_name(att):
    if not isinstance(att, dict):
        return str(att) if att else None
    n = pick(att, "name", "full_name", "display_name")
    if n:
        return n
    for key in ("login_information", "user", "person", "contact"):
        sub = att.get(key)
        if isinstance(sub, dict):
            n = pick(sub, "name", "full_name", "display_name")
            if n:
                return n
            first = pick(sub, "first_name") or ""
            last  = pick(sub, "last_name") or ""
            if first or last:
                return f"{first} {last}".strip()
    first = pick(att, "first_name") or ""
    last  = pick(att, "last_name") or ""
    return f"{first} {last}".strip() or None


def attendee_person_id(att):
    """The Procore user id behind an attendee, which is the join key into the
    project directory people list."""
    if not isinstance(att, dict):
        return None
    for key in ("login_information", "user", "person", "contact"):
        sub = att.get(key)
        if isinstance(sub, dict) and sub.get("id") is not None:
            return str(sub["id"])
    for key in ("user_id", "login_id", "person_id"):
        if att.get(key) is not None:
            return str(att[key])
    return None


def attendee_email(att):
    if not isinstance(att, dict):
        return None
    for key in ("login_information", "user", "person", "contact"):
        sub = att.get(key)
        if isinstance(sub, dict):
            e = pick(sub, "login", "email", "email_address")
            if e and "@" in str(e):
                return str(e)
    e = pick(att, "email", "email_address", "login")
    return str(e) if e and "@" in str(e) else None


def company_from_email(email, vendor_index):
    """Last-resort resolution: match the email's domain against this project's
    vendor names. robertmartinez@jjfloresroofing.co -> "jjfloresroofing", which
    equals normalize_company("JJ Flores Roofing") with spaces removed.

    `vendor_index` maps that space-stripped key -> the vendor's REAL name, and
    the real name is what comes back. Returning the stripped key instead would
    be silently wrong: it re-normalizes to "jjfloresroofing" (no spaces), which
    never equals the roster's "jj flores roofing", so the match would be thrown
    away by the very join it exists to satisfy.

    Only consulted after the directory join fails, and only against vendors
    already on THIS project, so it cannot invent a company out of nothing.
    """
    if not email or "@" not in email or not vendor_index:
        return None
    domain = email.rsplit("@", 1)[1].lower()
    stem = re.sub(r"[^a-z0-9]", "", domain.split(".")[0])
    if len(stem) < 4:
        return None
    for key, real_name in vendor_index.items():
        if key and (key == stem or (len(stem) >= 6 and (stem in key or key in stem))):
            return real_name
    return None


def extract_attendee_list(detail):
    """Every attendee-ish collection on a meeting detail, de-duplicated."""
    if not isinstance(detail, dict):
        return []
    out = []
    for key in ("attendees", "meeting_attendees", "attendance", "invitees",
                "required_attendees", "actual_attendees"):
        v = detail.get(key)
        if isinstance(v, list):
            out.extend([a for a in v if a])
    seen, deduped = set(), []
    for a in out:
        ident = None
        if isinstance(a, dict):
            ident = a.get("id") or (attendee_name(a), attendee_company(a))
        else:
            ident = str(a)
        key = safe_json_dumps(ident)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(a)
    return deduped


# ============================================================
# 5. Project scope
# ============================================================
def fetch_all_projects():
    """Full project list. filters[by_status]=all so Inactive/completed jobs are
    included — the same fix ingest_safety.py needed."""
    url = f"{PROCORE_API_BASE_URL}/rest/v1.0/projects"
    rows = get_paginated(url, {"company_id": company_id, "filters[by_status]": "all"}, per_page=300)
    if not rows:
        print("  filters[by_status]=all returned nothing — falling back to the default list.")
        rows = get_paginated(url, {"company_id": company_id}, per_page=300)
    return rows


def project_dates(p):
    return [str(p.get(k))[:10] for k in
            ("start_date", "actual_start_date", "completion_date",
             "projected_finish_date", "created_at")
            if p.get(k)]


def in_since_window(p):
    ds = project_dates(p)
    return (not ds) or max(ds) >= PROJECTS_SINCE


def looks_completed(p):
    """Projected finish more than COMPLETED_GRACE_DAYS in the past."""
    ends = [str(p.get(k))[:10] for k in ("projected_finish_date", "completion_date") if p.get(k)]
    if not ends:
        return False
    try:
        end = datetime.strptime(max(ends), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return False
    return (datetime.now(timezone.utc) - end).days > COMPLETED_GRACE_DAYS


def is_finished_stage(p):
    """Stage says this job is done, dead or paused. Reads the stage off the LIST
    response — no per-project detail call needed."""
    stage = str(p.get("project_stage") or p.get("stage") or "").lower()
    return any(h in stage for h in FINISHED_STAGE_HINTS)


def is_live_project(p):
    """Worth spending API calls on: Procore still marks it active and its stage
    isn't finished/dead/paused. Pre-construction and awarded jobs count as live
    — see FINISHED_STAGE_HINTS."""
    if p.get("active") is False:
        return False
    if str(p.get("status") or "").lower() in ("inactive", "closed", "archived"):
        return False
    return not is_finished_stage(p)


print("\nBuilding project scope...")
all_projects = _g.get("all_projects_vendor_cache")
if not all_projects:
    all_projects = fetch_all_projects()

project_names = {p.get("id"): p.get("name") for p in all_projects if p.get("id")}

if INCLUDE_ALL_PROJECTS:
    scope = [p for p in all_projects if in_since_window(p)]
else:
    scope = _g.get("active_projects") or [
        p for p in all_projects
        if p.get("active") is not False
        and str(p.get("status") or "").lower() not in ("inactive", "closed", "archived")]

merge_ids = None

# The runtime lever. Narrowing to live jobs only affects what gets FETCHED —
# out-of-scope projects keep their existing rows through the project-level merge
# below, so this is safe to flip in either direction.
if ACTIVE_PROJECTS_ONLY and not ONLY_PROJECT_IDS:
    before = len(scope)
    scope = [p for p in scope if is_live_project(p)]
    merge_ids = [p.get("id") for p in scope]
    print(f"  ACTIVE_PROJECTS_ONLY: {before} -> {len(scope)} projects "
          f"(dropped {before - len(scope)} finished/dead/inactive; their existing "
          f"rows are preserved).")
    print(f"  ~{len(scope) * 4} API calls for rosters + 1 meetings call per project. "
          f"Set ACTIVE_PROJECTS_ONLY = False for a full historical backfill.")

if ONLY_PROJECT_IDS:
    wanted = {str(i) for i in ONLY_PROJECT_IDS}
    scope = [p for p in scope if str(p.get("id")) in wanted]
    merge_ids = [p.get("id") for p in scope]
    print(f"  TARGETED mode: {len(scope)} project(s).")
elif SKIP_COMPLETED_PROJECTS:
    full = list(scope)
    scope = [p for p in scope if not looks_completed(p)]
    merge_ids = [p.get("id") for p in scope]
    print(f"  re-checking {len(scope)}, skipping {len(full) - len(scope)} completed "
          f"(rows preserved via project-level merge).")

if VENDOR_TEST_LIMIT is not None:
    scope = scope[:VENDOR_TEST_LIMIT]
    merge_ids = [p.get("id") for p in scope]
    print(f"  VENDOR_TEST_LIMIT={VENDOR_TEST_LIMIT} — merge mode on so this test "
          f"run can't wipe the other projects.")

print(f"  {len(all_projects)} projects total; {len(scope)} in scope.\n")

# ============================================================
# 6. The pull
# ============================================================
meeting_summaries, meeting_details, meeting_attendees, project_users = [], [], [], []
commitments, directory_vendors, sync_errors = [], [], []
template_id_hits, attendance_shapes, raw_status_values = {}, {}, {}

_t0, _n = time.time(), len(scope)

for pi, project in enumerate(scope, start=1):
    pid = project.get("id")
    if pid is None:
        continue
    pname = project.get("name")

    # ---- 6a. Project directory PEOPLE (the attendee -> company bridge) -----
    # A meeting attendee record carries only { id, status, login_information:
    # { id, login, name } } — there is NO company on it. Procore's UI fills that
    # "Company" column by joining the person against the project directory, so
    # the tracker has to do the same. One extra call per project buys the single
    # most important matching signal.
    person_company = {}
    project_vendor_norms = {}   # space-stripped key -> the vendor's real name
    if PULL_PROJECT_USERS:
        try:
            rows = get_paginated(f"{PROCORE_API_BASE_URL}/rest/v1.0/projects/{pid}/users",
                                 {"company_id": company_id}, per_page=VENDOR_PAGE_SIZE,
                                 label="project_users")
            for u in rows:
                if not isinstance(u, dict):
                    continue
                vendor = u.get("vendor")
                cname = pick_name(vendor) or pick(u, "company_name", "company")
                if isinstance(cname, dict):
                    cname = pick_name(cname)
                uid = u.get("id")
                email = pick(u, "email_address", "email", "login")
                if uid is not None and cname:
                    person_company[str(uid)] = cname
                project_users.append({
                    "project_procore_id": pid,
                    "person_procore_id": uid,
                    "name": pick(u, "name"),
                    "email": email,
                    "company_name": cname,
                    "company_normalized": normalize_company(cname),
                    "is_employee": u.get("is_employee"),
                    "is_gc": is_gc(cname),
                })
        except Exception as e:
            print(f"  project users failed: project={pid}: {e}")
            sync_errors.append({"project_procore_id": pid, "stage": "project_users",
                                "error": str(e), "created_at": datetime.now(timezone.utc).isoformat()})

    # ---- 6b. Commitments (work orders + purchase orders) -------------------
    if PULL_COMMITMENTS:
        for kind, path in (("work_order", "work_order_contracts"),
                           ("purchase_order", "purchase_order_contracts")):
            try:
                rows = get_paginated(f"{PROCORE_API_BASE_URL}/rest/v1.0/{path}",
                                     {"project_id": pid}, per_page=VENDOR_PAGE_SIZE,
                                     label=path)
                for c in rows:
                    vendor = c.get("vendor") if isinstance(c, dict) else None
                    vname = pick_name(vendor) or pick(c, "vendor_name", "contract_company_name")
                    vid   = vendor.get("id") if isinstance(vendor, dict) else pick(c, "vendor_id")
                    commitments.append({
                        "project_procore_id": pid,
                        "project_name": pname,
                        "contract_procore_id": c.get("id"),
                        "contract_kind": kind,
                        "contract_number": pick(c, "number", "contract_number"),
                        "contract_title": pick(c, "title", "description"),
                        "contract_status": pick(c, "status"),
                        "vendor_procore_id": vid,
                        "vendor_name": vname,
                        "vendor_normalized": normalize_company(vname),
                        "is_gc": is_gc(vname),
                        "executed": c.get("executed"),
                        "created_at": c.get("created_at"),
                        "raw_json": safe_json_dumps(c),
                    })
            except Exception as e:
                print(f"  {kind} failed: project={pid}: {e}")
                sync_errors.append({"project_procore_id": pid, "stage": kind,
                                    "error": str(e), "created_at": datetime.now(timezone.utc).isoformat()})

    # ---- 6c. Project directory vendors -------------------------------------
    if PULL_DIRECTORY:
        try:
            rows = get_paginated(f"{PROCORE_API_BASE_URL}/rest/v1.1/projects/{pid}/vendors",
                                 {"company_id": company_id}, per_page=VENDOR_PAGE_SIZE,
                                 label="project_vendors")
            for v in rows:
                vname = pick(v, "name", "company_name")
                trade = v.get("trade") if isinstance(v, dict) else None
                directory_vendors.append({
                    "project_procore_id": pid,
                    "project_name": pname,
                    "vendor_procore_id": v.get("id"),
                    "vendor_name": vname,
                    "vendor_normalized": normalize_company(vname),
                    "is_gc": is_gc(vname),
                    "trade_name": pick_name(trade),
                    "is_active": v.get("is_active", v.get("active")),
                    "business_type": pick(v, "business_type", "company_type"),
                    "created_at": v.get("created_at"),
                    "raw_json": safe_json_dumps(v),
                })
        except Exception as e:
            print(f"  directory vendors failed: project={pid}: {e}")
            sync_errors.append({"project_procore_id": pid, "stage": "directory_vendors",
                                "error": str(e), "created_at": datetime.now(timezone.utc).isoformat()})
        # Feed the email-domain fallback: this project's vendor names, normalized
        # with spaces removed, so "JJ Flores Roofing" can be matched against the
        # domain in robertmartinez@jjfloresroofing.co.
        project_vendor_norms = {
            normalize_company(v["vendor_name"]).replace(" ", ""): v["vendor_name"]
            for v in directory_vendors
            if v["project_procore_id"] == pid and v.get("vendor_name")
        }
        project_vendor_norms.pop("", None)

    # ---- 6d. Meetings ------------------------------------------------------
    # LAST on purpose: attendee company resolution needs the project-user and
    # vendor maps built by 6a-6c above.
    if PULL_MEETINGS:
        try:
            meetings = get_paginated(
                f"{PROCORE_API_BASE_URL}/rest/v1.1/projects/{pid}/meetings",
                {"company_id": company_id}, per_page=MEETINGS_PER_PAGE,
                label="meetings")

            # Some tenants nest meetings inside series groups.
            flat = []
            for m in meetings:
                if isinstance(m, dict) and isinstance(m.get("meetings"), list):
                    flat.extend(m["meetings"])
                elif isinstance(m, dict):
                    flat.append(m)

            for m in flat:
                title = pick(m, "title", "name")
                tmpl  = template_id_of(m)
                if tmpl:
                    template_id_hits[tmpl] = template_id_hits.get(tmpl, 0) + 1

                by_title    = looks_like_prep_meeting(title)
                by_template = tmpl == str(PREP_MEETING_TEMPLATE_ID)
                if PREP_MATCH_MODE == "template":
                    is_prep = by_template
                elif PREP_MATCH_MODE == "title":
                    is_prep = by_title
                else:                                   # template_or_title
                    is_prep = by_template or by_title
                if is_prep:
                    which = ("both" if (by_template and by_title)
                             else "template_only" if by_template else "title_only")
                    _prep_source[which] = _prep_source.get(which, 0) + 1

                meeting_summaries.append({
                    "project_procore_id": pid,
                    "project_name": pname,
                    "meeting_procore_id": m.get("id"),
                    "title": title,
                    "series_name": pick(m, "series_name", "meeting_series_name") or pick_name(m.get("series")),
                    "number": pick(m, "number", "meeting_number"),
                    "template_id": tmpl,
                    "is_prep_candidate": is_prep,
                    "raw_json": safe_json_dumps(m),
                })

                if not is_prep:
                    continue

                # Only prep candidates get the (expensive) detail call.
                mid = m.get("id")
                if mid is None:
                    continue
                detail, status = request_json(
                    f"{PROCORE_API_BASE_URL}/rest/v1.1/projects/{pid}/meetings/{mid}",
                    {"company_id": company_id})
                if not isinstance(detail, dict):
                    detail = m  # fall back to the list record so the meeting isn't lost

                for _k, _v in date_like_keys(detail).items():
                    _date_key_samples.setdefault(_k, _v)
                d_title = pick(detail, "title", "name") or title
                atts = extract_attendee_list(detail)

                meeting_details.append({
                    "project_procore_id": pid,
                    "project_name": pname,
                    "meeting_procore_id": mid,
                    "title": d_title,
                    "series_name": pick(detail, "series_name") or pick_name(detail.get("series")),
                    "number": pick(detail, "number", "meeting_number"),
                    "template_id": template_id_of(detail) or tmpl,
                    "meeting_date": meeting_date_of(detail),
                    "held_at": pick(detail, "held_at", "actual_date", "actual_start_time"),
                    "held": detail.get("held"),
                    "status": pick(detail, "status", "meeting_status"),
                    "location": pick(detail, "location"),
                    "created_at": detail.get("created_at"),
                    "updated_at": detail.get("updated_at"),
                    "attendee_count": len(atts),
                    "detail_http_status": status,
                    "raw_json": safe_json_dumps(detail),
                })

                for a in atts:
                    if isinstance(a, dict):
                        for k in a.keys():
                            _attendee_keys[k] = _attendee_keys.get(k, 0) + 1
                    st = attendance_status(a)
                    attendance_shapes[st] = attendance_shapes.get(st, 0) + 1
                    _raw_st = a.get("status") if isinstance(a, dict) else None
                    if _raw_st is not None:
                        raw_status_values[str(_raw_st)] = raw_status_values.get(str(_raw_st), 0) + 1

                    # Resolution order, best evidence first:
                    #   1. the attendee object itself (some tenants do embed it)
                    #   2. person id -> project directory people  <-- the real one here
                    #   3. email domain -> a vendor already on this project
                    person_id = attendee_person_id(a)
                    email = attendee_email(a)
                    cname = attendee_company(a)
                    csource = "attendee_object" if cname else None
                    if not cname and person_id:
                        cname = person_company.get(person_id)
                        if cname:
                            csource = "directory_join"
                    if not cname:
                        cname = company_from_email(email, project_vendor_norms)
                        if cname:
                            csource = "email_domain"
                    if not cname and email and is_gc_email(email):
                        # A Buffalo employee who isn't in this project's directory.
                        # They are the GC, never the vendor, so resolving them here
                        # keeps them out of the "unresolved" pile where they'd look
                        # like a data problem.
                        cname = GC_COMPANY_NAMES[0].title()
                        csource = "gc_email_domain"
                    if not cname:
                        csource = "unresolved"
                    _company_paths[f"resolved:{csource}"] = _company_paths.get(f"resolved:{csource}", 0) + 1
                    meeting_attendees.append({
                        "project_procore_id": pid,
                        "meeting_procore_id": mid,
                        "attendee_procore_id": a.get("id") if isinstance(a, dict) else None,
                        "attendee_name": attendee_name(a),
                        "attendee_person_id": person_id,
                        "attendee_email": email,
                        "company_name": cname,
                        "company_normalized": normalize_company(cname),
                        "company_source": csource,
                        "is_gc": is_gc(cname),
                        "attendance_status": st,
                        "raw_json": safe_json_dumps(a),
                    })
        except Exception as e:
            print(f"  meetings failed: project={pid}: {e}")
            sync_errors.append({"project_procore_id": pid, "stage": "meetings",
                                "error": str(e), "created_at": datetime.now(timezone.utc).isoformat()})
    if pi % 10 == 0 or pi == _n:
        el = time.time() - _t0
        eta = (el / pi) * (_n - pi)
        print(f"  [{pi}/{_n}] {el/60:.1f} min elapsed, ~{eta/60:.1f} min left — "
              f"prep={len(meeting_details)} commitments={len(commitments)} "
              f"directory={len(directory_vendors)} | {_stats['calls']} calls, "
              f"{_stats['sleep']/60:.1f} min in rate-limit pauses "
              f"(budget left: {_stats['remaining']})")

print(f"\nTotals: meetings_seen={len(meeting_summaries)} prep_meetings={len(meeting_details)} "
      f"attendees={len(meeting_attendees)} commitments={len(commitments)} "
      f"directory={len(directory_vendors)} errors={len(sync_errors)}")

_elapsed = time.time() - _t0
print(f"API: {_stats['calls']} calls in {_elapsed/60:.1f} min; "
      f"{_stats['pauses']} rate-limit pauses costing {_stats['sleep']/60:.1f} min "
      f"({100*_stats['sleep']/max(_elapsed,1):.0f}% of the run).")
if _stats["sleep"] > _elapsed * 0.4:
    print("  ⚠️  Most of this run was spent waiting on Procore's rate limit. Either")
    print("      another notebook is pulling from Procore at the same time (check the")
    print("      Procore Nightly / Friday Sweep schedules — they share the same API")
    print("      budget), or the scope is still too wide. ACTIVE_PROJECTS_ONLY = True")
    print("      is the biggest lever; VENDOR_TEST_LIMIT = 20 proves the run works")
    print("      before committing to the full scope.")

# ============================================================
# 7. Bronze writes
# ============================================================
print("\n--- bronze ---")
write_delta(meeting_summaries, "bronze_vendor_meeting_summaries",
            empty_cols=["project_procore_id", "meeting_procore_id", "title", "is_prep_candidate"],
            merge_project_ids=merge_ids)
write_delta(meeting_details, "bronze_vendor_meeting_details",
            empty_cols=["project_procore_id", "meeting_procore_id", "title", "meeting_date"],
            merge_project_ids=merge_ids)
write_delta(meeting_attendees, "bronze_vendor_meeting_attendees",
            empty_cols=["project_procore_id", "meeting_procore_id", "attendee_name",
                        "company_name", "company_normalized", "attendance_status"],
            merge_project_ids=merge_ids)
write_delta(commitments, "bronze_vendor_commitments",
            empty_cols=["project_procore_id", "contract_procore_id", "vendor_name",
                        "vendor_normalized", "contract_kind"],
            merge_project_ids=merge_ids)
write_delta(project_users, "bronze_vendor_project_users",
            empty_cols=["project_procore_id", "person_procore_id", "name", "email",
                        "company_name", "company_normalized"],
            merge_project_ids=merge_ids)
write_delta(directory_vendors, "bronze_vendor_directory",
            empty_cols=["project_procore_id", "vendor_procore_id", "vendor_name",
                        "vendor_normalized"],
            merge_project_ids=merge_ids)
if sync_errors:
    write_delta(sync_errors, "bronze_vendor_sync_errors")

# ============================================================
# 8. Silver
# ============================================================
print("\n--- silver ---")

spark.sql("""
CREATE OR REPLACE TABLE silver_vendor_prep_meetings AS
SELECT
    CAST(project_procore_id AS BIGINT)  AS project_procore_id,
    project_name,
    CAST(meeting_procore_id AS BIGINT)  AS meeting_procore_id,
    title,
    series_name,
    CAST(number AS INT)                 AS meeting_number,
    template_id,
    TO_DATE(COALESCE(held_at, meeting_date)) AS meeting_date,
    TO_DATE(meeting_date)               AS scheduled_date,
    TO_DATE(held_at)                    AS held_at,
    CAST(held AS BOOLEAN)               AS held,
    status,
    location,
    CAST(attendee_count AS INT)         AS attendee_count,
    raw_json,
    _fabric_loaded_at
FROM bronze_vendor_meeting_details
WHERE meeting_procore_id IS NOT NULL
""")

spark.sql("""
CREATE OR REPLACE TABLE silver_vendor_prep_attendees AS
SELECT
    CAST(project_procore_id AS BIGINT)  AS project_procore_id,
    CAST(meeting_procore_id AS BIGINT)  AS meeting_procore_id,
    CAST(attendee_procore_id AS BIGINT) AS attendee_procore_id,
    attendee_name,
    attendee_person_id,
    attendee_email,
    company_name,
    company_normalized,
    company_source,
    CAST(is_gc AS BOOLEAN)              AS is_gc,
    attendance_status,
    -- "For Distribution Only" means the agenda was sent, not that anyone sat in
    -- the meeting; conference/phone attendance does count as attending.
    CASE WHEN attendance_status IN ('present', 'conference') THEN true ELSE false END AS attended,
    _fabric_loaded_at
FROM bronze_vendor_meeting_attendees
WHERE meeting_procore_id IS NOT NULL
""")

# One row per (project, vendor), carrying BOTH provenance flags so the dashboard
# can switch its denominator between commitments and the directory without a
# re-ingest — and show the disagreement between them.
spark.sql("""
CREATE OR REPLACE TABLE silver_vendor_roster AS
WITH commit_v AS (
    SELECT
        CAST(project_procore_id AS BIGINT) AS project_procore_id,
        project_name,
        vendor_normalized,
        MAX(vendor_name)                       AS vendor_name,
        MAX(CAST(vendor_procore_id AS BIGINT)) AS vendor_procore_id,
        COUNT(*)                               AS contract_count,
        MAX(contract_kind)                     AS contract_kind,
        MAX(contract_status)                   AS contract_status
    FROM bronze_vendor_commitments
    WHERE vendor_normalized IS NOT NULL AND vendor_normalized <> ''
      AND LOWER(COALESCE(is_gc, 'false')) <> 'true'
    GROUP BY project_procore_id, project_name, vendor_normalized
),
dir_v AS (
    SELECT
        CAST(project_procore_id AS BIGINT) AS project_procore_id,
        project_name,
        vendor_normalized,
        MAX(vendor_name)                       AS vendor_name,
        MAX(CAST(vendor_procore_id AS BIGINT)) AS vendor_procore_id,
        MAX(trade_name)                        AS trade_name
    FROM bronze_vendor_directory
    WHERE vendor_normalized IS NOT NULL AND vendor_normalized <> ''
      AND LOWER(COALESCE(is_gc, 'false')) <> 'true'
    GROUP BY project_procore_id, project_name, vendor_normalized
)
SELECT
    COALESCE(c.project_procore_id, d.project_procore_id) AS project_procore_id,
    COALESCE(c.project_name, d.project_name)             AS project_name,
    COALESCE(c.vendor_normalized, d.vendor_normalized)   AS vendor_normalized,
    COALESCE(c.vendor_name, d.vendor_name)               AS vendor_name,
    COALESCE(c.vendor_procore_id, d.vendor_procore_id)   AS vendor_procore_id,
    d.trade_name,
    (c.vendor_normalized IS NOT NULL) AS from_commitment,
    (d.vendor_normalized IS NOT NULL) AS from_directory,
    COALESCE(c.contract_count, 0)     AS contract_count,
    c.contract_kind,
    c.contract_status,
    current_timestamp()               AS _fabric_loaded_at
FROM commit_v c
FULL OUTER JOIN dir_v d
  ON  c.project_procore_id = d.project_procore_id
  AND c.vendor_normalized  = d.vendor_normalized
""")

# ============================================================
# 9. Diagnostics
#
# These answer the questions the ingest itself can't decide:
#   • which vendor roster is actually maintained in this tenant,
#   • whether the meeting record exposes the template id,
#   • whether attendee company names are being returned at all.
# ============================================================
print("\n" + "=" * 62)
print("COVERAGE DIAGNOSTIC — which vendor roster should drive the tracker?")
print("=" * 62)
spark.sql("""
SELECT
    COUNT(DISTINCT CASE WHEN from_commitment THEN project_procore_id END) AS projects_with_commitments,
    COUNT(DISTINCT CASE WHEN from_directory  THEN project_procore_id END) AS projects_with_directory,
    SUM(CASE WHEN from_commitment THEN 1 ELSE 0 END)                      AS vendor_rows_commitment,
    SUM(CASE WHEN from_directory  THEN 1 ELSE 0 END)                      AS vendor_rows_directory,
    SUM(CASE WHEN from_commitment AND from_directory THEN 1 ELSE 0 END)   AS vendor_rows_both,
    SUM(CASE WHEN from_commitment AND NOT from_directory THEN 1 ELSE 0 END) AS commitment_only,
    SUM(CASE WHEN from_directory AND NOT from_commitment THEN 1 ELSE 0 END) AS directory_only
FROM silver_vendor_roster
""").show(truncate=False)
print("Read this as: whichever source covers more PROJECTS is the one BCI keeps")
print("current. A large `directory_only` count is expected — the project directory")
print("holds the owner, architect and inspectors, who never need a prep meeting.")

print("\n--- MEETING DATE (which key this tenant actually populates) ---")
for k, n in sorted(_date_fields_used.items(), key=lambda kv: -kv[1]):
    print(f"  {k}: {n}")
if _date_fields_used.get("<none>"):
    print(f"  ⚠️  {_date_fields_used['<none>']} meeting(s) had NO recognized date key.")
    print("      Date-ish keys actually present on the meeting records:")
    if _date_key_samples:
        for k, v in sorted(_date_key_samples.items()):
            mark = "  <-- add this to DATE_FIELDS" if k not in DATE_FIELDS else ""
            print(f"        {k} = {v}{mark}")
    else:
        print("        (none at all — the meeting record carries no date; the tracker")
        print("         will show meetings without dates, which is cosmetic only)")

print("\n--- meeting template id exposure ---")
if template_id_hits:
    for tid, n in sorted(template_id_hits.items(), key=lambda kv: -kv[1])[:15]:
        mark = "  <-- the prep template" if tid == str(PREP_MEETING_TEMPLATE_ID) else ""
        print(f"  template_id={tid}: {n} meetings{mark}")
    print("  => the API exposes the template id. PREP_MATCH_MODE = 'template_or_title'")
    print("     (the default) takes the UNION of the template and the title test, which is")
    print("     what you want. Set it to 'template' only if a title-matched meeting from a")
    print("     different template turns out to be noise.")
else:
    print("  none of", TEMPLATE_ID_FIELDS, "appeared on any meeting record.")
    print("  => the API does not expose the originating template; the title match is")
    print("     the only available filter. Leave PREP_REQUIRE_TEMPLATE_ID = False.")

print("\n--- how each prep meeting was identified ---")
for k in ("both", "template_only", "title_only"):
    if k in _prep_source:
        print(f"  {k}: {_prep_source[k]}")
if _prep_source.get("template_only"):
    print(f"  => {_prep_source['template_only']} meeting(s) came from the prep template but")
    print("     do NOT have 'prep' in the title. The title heuristic alone would have")
    print("     missed them — this is why PREP_MATCH_MODE defaults to the union.")
if _prep_source.get("title_only"):
    print(f"  => {_prep_source['title_only']} meeting(s) are titled like prep meetings but were")
    print("     built from a different template. Worth a look: either they belong and the")
    print("     union is doing its job, or someone free-typed a meeting that isn't one.")

print("\n--- endpoint reachability (tells 'no data' apart from 'no permission') ---")
for (label, status), n in sorted(_endpoint_status.items()):
    note = ""
    if status in (403, 404):
        note = ("  <-- NOT a 'no rows' answer: the tool is disabled for this project or the "
                "API user lacks permission on it")
    print(f"  {label:<28} HTTP {status}: {n} project(s){note}")
if any(s in (403, 404) for (lbl, s) in _endpoint_status
       if lbl in ("work_order_contracts", "purchase_order_contracts")):
    print("  ⚠️  Commitments returned 403/404 on some projects. A zero commitment count in")
    print("      the COVERAGE DIAGNOSTIC above may therefore mean 'no access', not 'no")
    print("      contracts' — grant the API service account read on Commitments before")
    print("      concluding BCI doesn't use them.")

if _withheld:
    print("\n  ⚠️  ROWS WITHHELD — Procore's Total header exceeded the rows actually returned:")
    for lbl, n in sorted(_withheld.items(), key=lambda kv: -kv[1]):
        print(f"        {lbl}: on {n} project(s)")
    print("      The data EXISTS and this service account cannot see all of it. That is a tool")
    print("      permission in Procore, not an empty dataset. Same failure mode that hid private")
    print("      Observations from the safety dashboard. Run fabric/diagnose_commitments.py.")

print("\n--- ATTENDEE SHAPE (where the company name actually lives) ---")
print("  top-level keys seen on attendee objects:")
for k, n in sorted(_attendee_keys.items(), key=lambda kv: -kv[1])[:30]:
    print(f"    {k}: {n}")
print("  path that produced the company name:")
for k, n in sorted(_company_paths.items(), key=lambda kv: -kv[1]):
    print(f"    {k}: {n}")
_unresolved = _company_paths.get("resolved:unresolved", 0)
_resolved   = sum(v for k, v in _company_paths.items() if k.startswith("resolved:")) - _unresolved
print(f"  => {_resolved} attendee(s) resolved, {_unresolved} unresolved.")
if _company_paths.get("resolved:email_domain"):
    print(f"  note: {_company_paths['resolved:email_domain']} resolved by EMAIL DOMAIN rather than")
    print("        the directory join — those people aren't in the project directory. Worth a")
    print("        spot-check, and worth adding them to the Procore project directory.")
if _unresolved:
    print(f"  ⚠️  {_unresolved} attendee(s) could not be tied to a company by any route: not on")
    print("      the record, not in the project directory, and the email domain matched no")
    print("      vendor on the job. Those meetings can still match on the title. Sample:")
    try:
        _sample = spark.sql("""
            SELECT raw_json FROM bronze_vendor_meeting_attendees
            WHERE raw_json IS NOT NULL LIMIT 1""").collect()
        if _sample:
            print("\n  SAMPLE ATTENDEE RECORD (copy this back):")
            print("  " + str(_sample[0]["raw_json"])[:1500])
    except Exception as _e:
        print(f"  (couldn't read a sample row: {_e})")

print("\n--- attendance status shapes seen ---")
print("  raw `status` values returned by Procore:")
for v, n in sorted(raw_status_values.items(), key=lambda kv: -kv[1]):
    known = attendance_status({"status": v})
    mark = "" if known != "unknown" else "   <-- NOT recognized; add a hint for it"
    print(f"    {v!r} -> {known}{mark}")
for st, n in sorted(attendance_shapes.items(), key=lambda kv: -kv[1]):
    print(f"  {st}: {n}")
_unknown = attendance_shapes.get("unknown", 0)
if _unknown:
    print(f"  ({_unknown} attendee(s) carried NO status field at all — nobody ticked a box for")
    print("   them. That is different from an unrecognized value; every status string Procore")
    print("   actually returned is listed above and is understood.)")
if _unknown > sum(attendance_shapes.values()) * 0.5:
    print("  ⚠️  Most attendees came back 'unknown' — the attendance field is in a")
    print("     shape attendance_status() doesn't recognize. Inspect a raw_json row")
    print("     in bronze_vendor_meeting_attendees and extend the hint lists.")

print("\n--- attendee company coverage (the primary matching signal) ---")
spark.sql("""
SELECT
    COUNT(*)                                                          AS attendee_rows,
    SUM(CASE WHEN company_normalized IS NULL OR company_normalized = ''
             THEN 1 ELSE 0 END)                                       AS missing_company,
    SUM(CASE WHEN is_gc THEN 1 ELSE 0 END)                            AS buffalo_attendees,
    SUM(CASE WHEN NOT is_gc AND company_normalized <> '' THEN 1 ELSE 0 END) AS vendor_attendees
FROM silver_vendor_prep_attendees
""").show(truncate=False)
print("If `missing_company` is high, attendee_company() is looking in the wrong")
print("place — inspect raw_json and extend it. Title matching still works, but at")
print("lower confidence.")

print("\n--- prep meetings per project (top 15) ---")
spark.sql("""
SELECT project_procore_id, MAX(project_name) AS project_name, COUNT(*) AS prep_meetings
FROM silver_vendor_prep_meetings
GROUP BY project_procore_id
ORDER BY prep_meetings DESC
""").show(15, truncate=False)

print("\nVendor compliance ingestion complete. Next: build_vendor_gold.py")
