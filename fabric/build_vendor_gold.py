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

# ── Signal 3: name-variant title matching ────────────────────────────────────
# MEASURED on the first full build: 10 prep meetings matched nothing, and the
# residual analysis classified 8 of them as "the vendor IS on the roster, under a
# longer legal name". The titles use the trading name, the roster uses what's on
# the contract:
#
#     title "… - HIVE"              roster "Hive Energy Solutions LLC"
#     title "… - Patriot Pipeline"  roster "Patriot Pipeline Albuquerque"
#     title "… - L&G Construction"  roster "L&G Concrete Construction, Inc"   (infix!)
#     title "…- H&W LandWorks"      roster "H&W Landwork KY LLC"              (plural)
#     title "… - K&B Electric"      roster "K&B Electrical Services, Inc"     (stem)
#
# The strict token-run test refuses all of these, correctly — it demands the
# vendor's tokens contiguous AND complete. So this is a THIRD, weaker signal
# rather than a loosening of the second one: every word the title says about the
# company must appear somewhere in the vendor's name, and the match must be
# UNIQUE on that project. Uniqueness is what makes it safe — it is precisely
# what stops "ABC" crediting both "ABC Construction" and "ABC Plumbing", and it
# already earns its keep on real data ("Patriot Pipeline" must not also match
# "Patriot Plumbing Solutions", which sits on the same job).
#
# ⚠️ These are emitted as CANDIDATES and are OFF by default in the API
# (`allowNameVariantMatch`). One of the eight is "KEN HOUSTON - Preparatory
# Meeting Agenda" against a vendor "Ken Houston Electric LLC" — and a meeting
# titled with a PERSON's name is a known pattern here. Auto-crediting that would
# mark a vendor compliant on the strength of a coincidence, which is the one
# error this tracker must not make. They surface as suggestions in the Review
# Queue instead.

# Corporate filler. Deliberately EXCLUDES trade words (electric, plumbing,
# concrete, roofing, pipeline) — those are exactly what tells "Patriot Pipeline"
# apart from "Patriot Plumbing", and treating them as noise would undo the
# uniqueness guarantee.
GENERIC_NAME_WORDS = {
    "construction", "contracting", "contractors", "contractor",
    "services", "service", "solutions", "group", "enterprises", "industries",
    "systems", "associates", "builders", "supply", "supplies", "products",
    "international", "national", "development", "developers", "holdings",
    "partners", "management", "usa", "inc", "llc",
}

# Words to strip off a MEETING TITLE before asking what company it names.
MEETING_BOILERPLATE = {
    "preparatory", "preparation", "prep", "phase", "pre", "contract", "precon",
    "meeting", "meetings", "agenda", "minutes", "kickoff", "kick", "off",
    "for", "the", "and", "with", "of", "on", "at", "a", "to",
    "site", "job", "project", "review", "notes", "call", "zoom", "teams",
}

# A token is "the same word" as another when one is an INFLECTION of the other:
# same stem plus a known word ending. Nothing else.
#
# ⚠️ The first version said "a prefix of it at 5+ characters", which is not the
# same claim at all — it made `Excel` match `Excelsior`, destroying the exact
# negative control the strict token-run rule was built around. A unit test on
# the real vendor names caught it. Any future loosening here must keep the
# Excel/Excelsior and ZIP/Zipper controls green.
VARIANT_MIN_STEM = 5
VARIANT_ENDINGS = ("s", "es", "al", "als", "al services", "ing", "ings",
                   "ed", "er", "ers", "or", "ors", "ion", "ions", "ics", "ial")


def _same_word(a, b):
    if a == b:
        return True
    lo, hi = (a, b) if len(a) <= len(b) else (b, a)
    if not hi.startswith(lo):
        return False
    # A plural is fine at any length ("h&w landwork" -> "landworks"); anything
    # else has to be a real stem, so a 3-letter abbreviation can't grow a word.
    extra = hi[len(lo):]
    if extra == "s":
        return True
    return len(lo) >= VARIANT_MIN_STEM and extra in VARIANT_ENDINGS


def title_names_tokens(title):
    """The words in a meeting title that might be a company name."""
    return [t for t in normalize_company(title or "").split()
            if t and t not in MEETING_BOILERPLATE and not t.isdigit()]


def variant_match(title, vendor_name):
    """True when the company named in the title is plausibly this vendor under a
    longer or inflected name. Conjunctive on purpose: EVERY word the title says
    must be in the vendor's name, so a shared generic word can never carry a
    match on its own ("Escar Construction" must not match "Espana Construction").
    """
    t_all = title_names_tokens(title)
    v_all = normalize_company(vendor_name or "").split()
    if not t_all or not v_all:
        return False
    for tok in t_all:
        if not any(_same_word(tok, v) for v in v_all):
            return False
    # ...and the agreement can't be only on filler words.
    t_dist = [t for t in t_all if t not in GENERIC_NAME_WORDS]
    v_dist = [v for v in v_all if v not in GENERIC_NAME_WORDS]
    if not t_dist or not v_dist:
        return False
    return any(_same_word(a, b) for a in t_dist for b in v_dist)


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
spark.udf.register("variant_match", variant_match, BooleanType())


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
    -- Normalized + space-padded title, so the API can run the same whole-token
    -- title test live for manually-added vendors without re-implementing
    -- normalize_company in T-SQL.
    token_pad(m.title)       AS title_padded,
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
),
-- Meetings already credited by a stronger signal. Signal 3 is deliberately
-- restricted to what is left over.
matched_meetings AS (
    SELECT DISTINCT m.meeting_id
    FROM meetings m
    JOIN roster r ON r.project_id = m.project_id
    JOIN attendee_match am ON am.meeting_id = m.meeting_id
                          AND am.vendor_normalized = r.vendor_normalized
    UNION
    SELECT DISTINCT m.meeting_id
    FROM meetings m
    JOIN roster r ON r.project_id = m.project_id
    WHERE r.title_matchable
      AND m.title_padded <> ''
      AND INSTR(m.title_padded, token_pad(r.vendor_name)) > 0
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

UNION ALL

-- Signal 3: the same company under a longer legal name. Applied ONLY to
-- meetings that matched nothing at all, so it can never disturb a match the two
-- stronger signals already made, and only where exactly ONE vendor on the
-- project fits — the uniqueness gate is what makes it safe.
SELECT
    v.project_id,
    v.vendor_normalized,
    v.vendor_name,
    v.meeting_id,
    v.title            AS meeting_title,
    v.meeting_date,
    v.held,
    'title_variant'    AS match_method,
    CAST(NULL AS BOOLEAN) AS attendee_attended,
    CAST(NULL AS STRING)  AS matched_attendee_name,
    CAST(NULL AS STRING)  AS attendance_status,
    current_timestamp() AS _fabric_loaded_at
FROM (
    SELECT c.*, COUNT(*) OVER (PARTITION BY c.meeting_id) AS fits
    FROM (
        SELECT m.project_id, m.meeting_id, m.title, m.meeting_date, m.held,
               r.vendor_normalized, r.vendor_name
        FROM meetings m
        JOIN roster r ON r.project_id = m.project_id
        WHERE r.title_matchable
          AND m.meeting_id NOT IN (SELECT meeting_id FROM matched_meetings)
          AND variant_match(m.title, r.vendor_name)
    ) c
) v
WHERE v.fits = 1
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

# ------------------------------------------------------------------
# WHY didn't each of those match?
#
# The unmatched titles plainly name subcontractors — "Escar Construction",
# "K&B Electric", "Patriot Pipeline", "H&W LandWorks". "They're missing from the
# roster" is the obvious explanation and it may well be right, but it is a
# GUESS, and guessing from a table like the one above is precisely how the
# commitments bug got written down as a fact about the business. There are three
# very different causes and they need different fixes:
#
#   A. the roster carries the vendor and the title contains its whole name in
#      order -> the match SHOULD have fired, and that is a bug in this file;
#   B. the roster name is a superset/variant of what's in the title ("Patriot
#      Pipeline Solutions" vs "...- Patriot Pipeline") -> the token-run test
#      requires the vendor's tokens CONTIGUOUS AND COMPLETE, so it correctly
#      refuses, and the question becomes whether to relax it;
#   C. no vendor on that project shares a single word with the title -> the sub
#      really is absent from the roster, and someone has to add it.
#
# So: strip the meeting boilerplate off the title (title_names_tokens, shared
# with the variant matcher above so the diagnostic and the rule can't disagree)
# and show what the project's roster actually offers against the residual.
#
# ⚠️ Score on DISTINCTIVE words only. The first version counted any shared token
# and so reported "Escar Construction" as a near miss of "Espana Construction" —
# the only word they share is "construction". That is a genuinely-absent vendor
# (cause C) dressed up as a near miss (cause B), i.e. the diagnostic quietly
# talking the reader out of the one row that needed action.
_residual_tokens = title_names_tokens

print("\n--- WHY those didn't match (title residual vs that project's roster) ---")
try:
    _unmatched = spark.sql("""
    SELECT m.project_id, m.project_name, m.meeting_id, m.title, m.title_padded
    FROM gold_vendor_prep_meetings m
    LEFT JOIN (SELECT DISTINCT meeting_id FROM gold_vendor_prep_matches) x
           ON x.meeting_id = m.meeting_id
    WHERE x.meeting_id IS NULL
    ORDER BY m.meeting_date DESC
    """).collect()

    _roster_by_project = {}
    for _r in spark.sql(
        "SELECT project_id, vendor_name, vendor_normalized FROM gold_vendor_roster"
    ).collect():
        _roster_by_project.setdefault(_r["project_id"], []).append(
            (_r["vendor_name"], _r["vendor_normalized"]))

    _causes = {"A_should_have_matched": 0, "B_name_variant_suggested": 0,
               "B_ambiguous": 0, "C_vendor_not_on_roster": 0, "D_no_vendor_in_title": 0}

    for _m in _unmatched:
        _res = _residual_tokens(_m["title"] or "")
        # Distinctive words only. Scoring on ANY shared token reported "Escar
        # Construction" as a near miss of "Espana Construction" on the strength
        # of the word "construction" alone.
        _res_dist = set(t for t in _res if t not in GENERIC_NAME_WORDS)
        _cands, _variants = [], []
        for _vname, _vnorm in _roster_by_project.get(_m["project_id"], []):
            _vt = [t for t in (_vnorm or "").split() if t not in MEETING_BOILERPLATE]
            if not _vt:
                continue
            if variant_match(_m["title"], _vname):
                _variants.append(_vname)
            _hit = sum(1 for t in _vt
                       if t not in GENERIC_NAME_WORDS
                       and any(_same_word(t, r) for r in _res_dist))
            if _hit:
                _contig = token_pad(_vnorm) in (_m["title_padded"] or "")
                _cands.append((_hit / len(_vt), _hit, _contig, _vname, len(_vt)))
        _cands.sort(reverse=True)

        if not _res:
            _cause, _detail = "D_no_vendor_in_title", "the title names no company at all"
        elif _cands and _cands[0][0] >= 1.0 and _cands[0][2]:
            _cause = "A_should_have_matched"
            _detail = f"⚠ BUG — '{_cands[0][3]}' is on the roster and fully in the title"
        elif len(_variants) == 1:
            _cause = "B_name_variant_suggested"
            _detail = (f"same company under a longer name: '{_variants[0]}' "
                       "-> suggested in the Review Queue")
        elif len(_variants) > 1:
            _cause = "B_ambiguous"
            _detail = ("AMBIGUOUS - the title fits " + str(len(_variants)) + " vendors ("
                       + ", ".join(f"'{v}'" for v in _variants[:3])
                       + "); refusing to guess between them")
        elif not _cands:
            _cause = "C_vendor_not_on_roster"
            _detail = "NO vendor on this project shares a distinctive word with the title"
        else:
            _cause = "C_vendor_not_on_roster"
            _detail = ("no roster name contains everything the title says; closest is "
                       + ", ".join(f"'{c[3]}' ({c[1]} of its {c[4]} words)" for c in _cands[:2]))
        _causes[_cause] += 1
        print(f"  [{_cause[0]}] {(_m['title'] or '')[:52]:<52} project {_m['project_id']}")
        print(f"        title names : {' '.join(_res) or '(nothing)'}")
        print(f"        {_detail}")

    print("\n  cause tally:", ", ".join(f"{k}={v}" for k, v in _causes.items()))
    if _causes["A_should_have_matched"]:
        print("  ⚠️  Any 'A' is a matching bug in this file, not a data problem — the vendor")
        print("      is on the roster and its full name is in the title. Send me the row.")
    if _causes["B_name_variant_suggested"]:
        print(f"  {_causes['B_name_variant_suggested']} row(s) are the same company under a longer")
        print("      legal name. gold emits these as `title_variant` CANDIDATES; they do NOT count")
        print("      until an admin turns on 'Name-variant title match' in Settings, because one of")
        print("      them can be a meeting titled with a PERSON's name that happens to match a")
        print("      vendor. Confirm them from the Review Queue instead.")
    if _causes["B_ambiguous"]:
        print(f"  {_causes['B_ambiguous']} row(s) fit more than one vendor and are deliberately")
        print("      left unmatched — this is the uniqueness gate doing its job.")
    if _causes["C_vendor_not_on_roster"]:
        print("  'C' rows are real work in Procore: the sub isn't in the project's vendor")
        print("      directory (nor under any contract), so the tracker cannot credit them.")
        print("      Add them in Procore, or as a manual vendor in the tracker's admin UI.")
except Exception as _e:                                     # never fail the build on a diagnostic
    print(f"  (could not run the residual analysis: {_e})")

# Are those subs at least present as PEOPLE on the job?
#
# ⚠️ An earlier version of this comment stated they were — "those companies DO
# show up on the project PEOPLE list, because someone from them was given
# project access". The query below returned ZERO on the first real run, so that
# was another hypothesis written down as a finding. What it means is stronger
# and worse: the missing subs are in neither Procore list, so nothing short of
# adding them to Procore (or a manual vendor row) can make their prep meeting
# countable.
#
# Kept because a non-zero here is a cheap, no-judgement fix. Folding
# people-companies into the roster would silently change every project's
# denominator, which is the safety team's call, not a side effect of a run.
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
