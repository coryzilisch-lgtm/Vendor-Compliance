# ============================================================
# Fabric Notebook Cell: VENDOR COMPLIANCE GOLD
# ------------------------------------------------------------
# Turns the silver vendor/meeting tables into the four tables the Prep-Phase
# Meeting Tracker mirrors down to the Safety-Dash Fabric SQL Database:
#
#   gold_vendor_roster        -> dbo.vendor_roster         (the checklist rows)
#   gold_vendor_prep_meetings -> dbo.vendor_prep_meetings  (the meetings held)
#   gold_vendor_prep_attendees-> dbo.vendor_prep_attendees (who was in the room)
#   gold_vendor_prep_matches  -> dbo.vendor_prep_matches   (vendor <-> meeting)
#
# ── Why there is no gold_vendor_prep_status table ────────────────────────────
# The obvious fifth table — "is this vendor done: yes/no" — is deliberately NOT
# built here. Whether a match counts depends on three settings the safety team
# can change from the dashboard (which vendor roster is authoritative, whether
# the vendor had to be marked Present, whether a title-only match is good
# enough), plus per-vendor admin overrides. Baking the answer into gold would
# mean a full notebook + pipeline run every time someone flips a checkbox.
# So gold emits the CANDIDATE MATCHES and the API resolves them live.
#
# Run AFTER ingest_vendor_compliance.py.
# ============================================================

import re
from pyspark.sql.types import StringType, BooleanType
from pyspark.sql import functions as F

# ============================================================
# 1. Config
# ============================================================

# Vendors that never need a preparatory meeting, matched on the NORMALIZED name.
# These are companies that appear in the Procore project directory but do not put
# crews on the job: the owner, the architect, testing agencies, the GC itself.
# This is the blunt, global instrument — the dashboard also has a per-project
# "not applicable" toggle for one-off cases, which is usually the better tool.
EXCLUDED_VENDOR_NORMALIZED = {
    "buffalo construction",
}

# Substring patterns (on the normalized name) for vendor-shaped rows that are
# structurally never subcontractors. Applied as a starts-with/contains test and
# reported in the diagnostics so you can see exactly what got filtered.
EXCLUDED_VENDOR_PATTERNS = [
    "testing laborator", "geotechnical engineer",
]

# Title matching is a fallback for meetings where the vendor never got added to
# the attendee list. It needs a floor on length or a two-letter vendor name would
# match noise inside an unrelated title.
TITLE_MATCH_MIN_CHARS = 3

# Words that are pure meeting boilerplate. A "vendor" whose entire normalized
# name is boilerplate can't be title-matched — it would hit every meeting.
TITLE_STOPWORDS = {
    "meeting", "prep", "preparatory", "phase", "agenda", "assignment",
    "the", "and", "of", "for", "fdg", "project", "job", "site",
}

print("Building vendor compliance gold tables...")

# ============================================================
# 2. Normalization — MUST match ingest_vendor_compliance.py
# ============================================================
LEGAL_SUFFIX_TOKENS = {
    "llc", "lc", "inc", "incorporated", "co", "corp", "corporation", "company",
    "ltd", "limited", "lp", "llp", "plc", "pllc", "pc", "pa",
}


def normalize_company(name):
    """Canonical company key. Byte-for-byte the same rules as the ingest cell's
    normalize_company() — the two produce keys that are joined against each
    other, so any divergence shows up as every vendor reading 'not held'."""
    if not name:
        return ""
    s = str(name).lower()
    s = s.replace("&", " and ")
    # Periods DELETED, not spaced — see the matching comment in the ingest cell.
    # "L.L.C." must collapse to "llc" or it survives the suffix strip as three
    # separate tokens and splits one vendor into two.
    s = s.replace(".", "")
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    tokens = s.split()

    def strip_trailing():
        while tokens and tokens[-1] in LEGAL_SUFFIX_TOKENS:
            tokens.pop()

    strip_trailing()

    i = len(tokens)
    while i > 0 and len(tokens[i - 1]) == 1:
        i -= 1
    if i < len(tokens) and "".join(tokens[i:]) in LEGAL_SUFFIX_TOKENS:
        del tokens[i:]
        strip_trailing()

    return " ".join(tokens)


def token_pad(name):
    """Wrap a normalized string in spaces so a plain substring test becomes a
    whole-token-run test: ' zip ' is found in ' preparatory meeting zip ' but NOT
    inside ' preparatory meeting zipper '. That one space is what stops a short
    vendor abbreviation from matching the middle of an unrelated word."""
    n = normalize_company(name)
    return f" {n} " if n else ""


def title_matchable(name):
    """False when a vendor name is too short or is entirely meeting boilerplate,
    either of which would make title matching fire on everything."""
    n = normalize_company(name)
    if len(n.replace(" ", "")) < TITLE_MATCH_MIN_CHARS:
        return False
    return any(t not in TITLE_STOPWORDS for t in n.split())


def vendor_excluded(name):
    n = normalize_company(name)
    if not n:
        return True
    if n in EXCLUDED_VENDOR_NORMALIZED:
        return True
    return any(p in n for p in EXCLUDED_VENDOR_PATTERNS)


spark.udf.register("norm_company", normalize_company, StringType())
spark.udf.register("token_pad", token_pad, StringType())
spark.udf.register("title_matchable", title_matchable, BooleanType())
spark.udf.register("vendor_excluded", vendor_excluded, BooleanType())


def table_exists(name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        return False


for required in ("silver_vendor_roster", "silver_vendor_prep_meetings",
                 "silver_vendor_prep_attendees"):
    if not table_exists(required):
        raise RuntimeError(
            f"{required} is missing — run ingest_vendor_compliance.py first.")

# ============================================================
# 3. gold_vendor_roster — the checklist rows
#
# One row per (project, vendor). Carries BOTH provenance flags so the dashboard's
# `vendorSource` setting can switch the denominator between commitments and the
# project directory with no re-ingest, and can show where the two disagree.
# ============================================================
spark.sql("""
CREATE OR REPLACE TABLE gold_vendor_roster AS
SELECT
    r.project_procore_id                          AS project_id,
    r.project_name,
    r.vendor_normalized,
    r.vendor_name,
    r.vendor_procore_id                           AS vendor_id,
    r.trade_name,
    r.from_commitment,
    r.from_directory,
    r.contract_count,
    r.contract_kind,
    r.contract_status,
    vendor_excluded(r.vendor_name)                AS is_excluded_vendor,
    title_matchable(r.vendor_name)                AS title_matchable,
    current_timestamp()                           AS _fabric_loaded_at
FROM silver_vendor_roster r
WHERE r.vendor_normalized IS NOT NULL
  AND r.vendor_normalized <> ''
""")

# ============================================================
# 4. gold_vendor_prep_meetings / gold_vendor_prep_attendees
# ============================================================
spark.sql("""
CREATE OR REPLACE TABLE gold_vendor_prep_meetings AS
SELECT
    m.project_procore_id     AS project_id,
    m.project_name,
    m.meeting_procore_id     AS meeting_id,
    m.title,
    m.series_name,
    m.meeting_number,
    m.template_id,
    m.meeting_date,
    m.scheduled_date,
    m.held_at,
    COALESCE(m.held, false)  AS held,
    m.status,
    m.location,
    m.attendee_count,
    -- Vendor-side attendees only: the count that says whether anyone from
    -- outside Buffalo Construction was actually in the room.
    COALESCE(a.vendor_attendees, 0)          AS vendor_attendee_count,
    COALESCE(a.vendor_attendees_present, 0)  AS vendor_attendees_present,
    current_timestamp()      AS _fabric_loaded_at
FROM silver_vendor_prep_meetings m
LEFT JOIN (
    SELECT
        meeting_procore_id,
        SUM(CASE WHEN NOT COALESCE(is_gc, false) AND COALESCE(company_normalized,'') <> ''
                 THEN 1 ELSE 0 END)                                   AS vendor_attendees,
        SUM(CASE WHEN NOT COALESCE(is_gc, false) AND COALESCE(company_normalized,'') <> ''
                  AND attended THEN 1 ELSE 0 END)                     AS vendor_attendees_present
    FROM silver_vendor_prep_attendees
    GROUP BY meeting_procore_id
) a ON a.meeting_procore_id = m.meeting_procore_id
""")

spark.sql("""
CREATE OR REPLACE TABLE gold_vendor_prep_attendees AS
SELECT
    project_procore_id  AS project_id,
    meeting_procore_id  AS meeting_id,
    attendee_procore_id AS attendee_id,
    attendee_name,
    company_name,
    company_normalized,
    is_gc,
    attendance_status,
    attended,
    current_timestamp() AS _fabric_loaded_at
FROM silver_vendor_prep_attendees
""")

# ============================================================
# 5. gold_vendor_prep_matches — the matching engine
#
# Two independent signals, emitted as separate candidate rows so the API can
# weight them live rather than us collapsing them here:
#
#   'attendee'  The meeting has an attendee whose company normalizes to this
#               vendor. STRONGEST signal — it is a direct record that the vendor
#               was on the invite, and it carries `attendee_attended` so the
#               dashboard can insist on Present rather than
#               "For Distribution Only".
#
#   'title'     The vendor's name appears as a whole-token run in the meeting
#               title ("Preparatory Meeting - ZIP"). This is how BCI actually
#               labels these meetings, so it catches every meeting where nobody
#               remembered to add the sub to the attendee list — but it is text
#               matching, so it is reported at lower confidence and can be
#               turned off entirely (`allowTitleMatch`).
#
# A vendor/meeting pair can produce BOTH rows. That is intended: two independent
# confirmations is exactly what you want to see.
# ============================================================
spark.sql("""
CREATE OR REPLACE TABLE gold_vendor_prep_matches AS
WITH roster AS (
    SELECT project_id, vendor_normalized, vendor_name, title_matchable
    FROM gold_vendor_roster
),
meetings AS (
    SELECT project_id, meeting_id, title, meeting_date, held,
           token_pad(title) AS title_padded
    FROM gold_vendor_prep_meetings
),
-- Signal 1: attendee company. Collapsed to one row per (meeting, company) with
-- MAX(attended) so a vendor who sent two people, one Present and one For
-- Distribution Only, correctly reads as having attended.
attendee_match AS (
    SELECT
        a.meeting_id,
        a.company_normalized       AS vendor_normalized,
        MAX(a.attended)            AS attendee_attended,
        MAX(a.attendee_name)       AS attendee_name,
        MAX(a.attendance_status)   AS attendance_status
    FROM gold_vendor_prep_attendees a
    WHERE NOT COALESCE(a.is_gc, false)
      AND COALESCE(a.company_normalized, '') <> ''
    GROUP BY a.meeting_id, a.company_normalized
)
SELECT
    m.project_id,
    r.vendor_normalized,
    r.vendor_name,
    m.meeting_id,
    m.title            AS meeting_title,
    m.meeting_date,
    m.held,
    'attendee'         AS match_method,
    am.attendee_attended,
    am.attendee_name   AS matched_attendee_name,
    am.attendance_status,
    current_timestamp() AS _fabric_loaded_at
FROM meetings m
JOIN roster        r  ON r.project_id = m.project_id
JOIN attendee_match am ON am.meeting_id = m.meeting_id
                      AND am.vendor_normalized = r.vendor_normalized

UNION ALL

SELECT
    m.project_id,
    r.vendor_normalized,
    r.vendor_name,
    m.meeting_id,
    m.title            AS meeting_title,
    m.meeting_date,
    m.held,
    'title'            AS match_method,
    CAST(NULL AS BOOLEAN) AS attendee_attended,
    CAST(NULL AS STRING)  AS matched_attendee_name,
    CAST(NULL AS STRING)  AS attendance_status,
    current_timestamp() AS _fabric_loaded_at
FROM meetings m
JOIN roster r ON r.project_id = m.project_id
WHERE r.title_matchable
  AND m.title_padded <> ''
  AND INSTR(m.title_padded, token_pad(r.vendor_name)) > 0
""")

# ============================================================
# 6. Diagnostics
# ============================================================
print("\n" + "=" * 64)
print("ROW COUNTS")
print("=" * 64)
for t in ("gold_vendor_roster", "gold_vendor_prep_meetings",
          "gold_vendor_prep_attendees", "gold_vendor_prep_matches"):
    print(f"  {t}: {spark.table(t).count()}")

print("\n--- match methods ---")
spark.sql("""
SELECT match_method,
       COUNT(*)                                        AS match_rows,
       COUNT(DISTINCT CONCAT(project_id, '|', vendor_normalized)) AS vendors_matched,
       COUNT(DISTINCT meeting_id)                      AS meetings_involved
FROM gold_vendor_prep_matches
GROUP BY match_method
ORDER BY match_rows DESC
""").show(truncate=False)

print("--- how many vendors are confirmed by BOTH signals? ---")
spark.sql("""
SELECT
    SUM(CASE WHEN methods = 2 THEN 1 ELSE 0 END) AS both_signals,
    SUM(CASE WHEN methods = 1 AND has_attendee THEN 1 ELSE 0 END) AS attendee_only,
    SUM(CASE WHEN methods = 1 AND NOT has_attendee THEN 1 ELSE 0 END) AS title_only
FROM (
    SELECT project_id, vendor_normalized,
           COUNT(DISTINCT match_method)                                  AS methods,
           MAX(CASE WHEN match_method = 'attendee' THEN true ELSE false END) AS has_attendee
    FROM gold_vendor_prep_matches
    GROUP BY project_id, vendor_normalized
)
""").show(truncate=False)
print("`title_only` is the number of vendors whose prep meeting was identified")
print("from the meeting TITLE alone — nobody added the sub to the attendee list.")
print("Those are the rows to sanity-check first; they are also the argument for")
print("asking supers to fill in attendees.")

# The meetings nothing matched. These are the real blind spot: a prep meeting
# was held and logged, but it cannot be credited to any vendor on the roster —
# usually a vendor missing from the roster source, or a meeting titled with a
# person's name ("FDG Prep phase meeting - Blake Daher") and no vendor attendee.
print("\n--- UNMATCHED prep meetings (surfaced in the dashboard's Review tab) ---")
spark.sql("""
SELECT m.project_id, m.project_name, m.meeting_id, m.title, m.meeting_date,
       m.vendor_attendee_count
FROM gold_vendor_prep_meetings m
LEFT JOIN (SELECT DISTINCT meeting_id FROM gold_vendor_prep_matches) x
       ON x.meeting_id = m.meeting_id
WHERE x.meeting_id IS NULL
ORDER BY m.meeting_date DESC
""").show(25, truncate=False)

# The unmatched meetings name real subcontractors ("Escar Construction",
# "K&B Electric", "Patriot Pipeline") that have no row in Procore's project
# VENDOR directory — so the tracker's denominator can't see them and their prep
# meeting can never be credited. But those companies DO show up on the project
# PEOPLE list, because someone from them was given project access.
#
# This is quantified, not acted on. Folding people-companies into the roster
# would silently change every project's denominator, which is the safety team's
# call, not a side effect of a notebook run. If the number below is large, the
# options are: add the missing companies to Procore's project directory (best),
# add them per-project in the tracker's admin UI, or ask for a vendorSource
# option that includes them.
if table_exists("bronze_vendor_project_users"):
    print("\n--- companies with PEOPLE on a project but NO vendor-directory row ---")
    spark.sql("""
    WITH people AS (
        SELECT DISTINCT
            CAST(project_procore_id AS BIGINT) AS project_id,
            company_normalized,
            MAX(company_name) OVER (PARTITION BY company_normalized) AS company_name
        FROM bronze_vendor_project_users
        WHERE COALESCE(company_normalized, '') <> ''
          AND LOWER(COALESCE(is_gc, 'false')) <> 'true'
    )
    SELECT p.company_name,
           COUNT(DISTINCT p.project_id) AS projects_present_as_people
    FROM people p
    LEFT JOIN gold_vendor_roster r
           ON r.project_id = p.project_id
          AND r.vendor_normalized = p.company_normalized
    WHERE r.vendor_normalized IS NULL
    GROUP BY p.company_name
    ORDER BY projects_present_as_people DESC
    """).show(20, truncate=False)
    spark.sql("""
    WITH people AS (
        SELECT DISTINCT CAST(project_procore_id AS BIGINT) AS project_id, company_normalized
        FROM bronze_vendor_project_users
        WHERE COALESCE(company_normalized, '') <> ''
          AND LOWER(COALESCE(is_gc, 'false')) <> 'true'
    )
    SELECT COUNT(*) AS company_project_pairs_missing_from_roster,
           COUNT(DISTINCT company_normalized) AS distinct_companies
    FROM people p
    LEFT JOIN gold_vendor_roster r
           ON r.project_id = p.project_id AND r.vendor_normalized = p.company_normalized
    WHERE r.vendor_normalized IS NULL
    """).show(truncate=False)
    print("These are companies working on the job whose prep meeting the tracker")
    print("currently CANNOT credit, because they aren't on the checklist at all.")

print("--- vendors filtered out as never-needs-a-prep-meeting ---")
spark.sql("""
SELECT vendor_name, COUNT(DISTINCT project_id) AS projects
FROM gold_vendor_roster
WHERE is_excluded_vendor
GROUP BY vendor_name
ORDER BY projects DESC
""").show(25, truncate=False)

print("\nVendor gold build complete.")
print("Next: run the `mirror-vendor-to-sql` pipeline (4 Copy activities), then")
print("hard-refresh the dashboard.")
