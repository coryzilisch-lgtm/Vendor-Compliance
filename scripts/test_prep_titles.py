"""looks_like_prep_meeting() against the real meeting titles in this tenant.

The safety team's rule is literal: only PREPARATORY meetings belong in the
tracker. Template id 383995 does not carry that distinction — pre-contract,
kick-off and coordination meetings are created from the same template and
retitled — so the title is the rule, and this is what pins it down.
"""
import ast
import re
import sys

SRC = open("/home/user/Vendor-Compliance/fabric/ingest_vendor_compliance.py").read()
tree = ast.parse(SRC)
ns = {"re": re}
for node in tree.body:
    if isinstance(node, ast.Assign) and any(
            getattr(t, "id", "") in ("PREP_TITLE_PATTERNS", "PREP_TITLE_EXCLUDE",
                                     "PREP_TITLE_RE", "PREP_EXCLUDE_RE",
                                     "_prep_excluded_titles")
            for t in node.targets):
        exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    if isinstance(node, ast.FunctionDef) and node.name == "looks_like_prep_meeting":
        exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
prep = ns["looks_like_prep_meeting"]

KEEP = [
    # Real titles from the live data that ARE preparatory meetings.
    "Preparatory Meeting - ZIP",
    "Preparatory Meeting- East and Westbrook",
    "Preparatory Meeting Agenda - Site",
    "Preparatory Meeting Agenda Escar Construction",
    "Preparatory Meeting Agenda- H&W LandWorks",
    "Preparatory Meeting - K&B Electric",
    "KEN HOUSTON - Preparatory Meeting Agenda",
    "Prep Phase Meeting Agenda",
    "FDG Prep phase meeting - Blake Daher",
    "Clark FDG assignment - Preparatory Meeting Agenda",
]

DROP = [
    # Real titles the template id was wrongly sweeping in.
    "Pre-Contract Meeting Agenda - HIVE",
    "Pre-Contract Meeting Agenda - Montesdeoca",
    "Pre-Contract Meeting Agenda - Patriot Pipeline",
    "Pre-Contract Meeting Agenda - L&G Construction",
    "Pre Contract Meeting - Acme",
    "Kick Off Meeting - Acme Electric",
    "Kickoff Meeting Agenda",
    "Coordination Meeting - MEP",
    "Preconstruction Meeting",
    "Pre-Construction Meeting Agenda",
    "OAC Meeting 14",
    "Owner Architect Contractor Meeting",
    "Weekly Progress Meeting",
    "Toolbox Talk - Fall Protection",
    # Never had "prep" in them at all, so the include test alone stops these.
    "Weekly Subcontractor Meeting",
    "Punch List Walk",
]

fail = 0
print("=== must be KEPT (real preparatory meetings) ===")
for t in KEEP:
    ok = prep(t)
    fail += not ok
    print(f"{'ok  ' if ok else 'FAIL'}  {t}")

print("\n=== must be DROPPED (a different kind of meeting) ===")
for t in DROP:
    ok = not prep(t)
    fail += not ok
    print(f"{'ok  ' if ok else 'FAIL'}  {t}")

# The exclusion list is applied to the whole title, so a vendor whose NAME
# contains an excluded word would be dropped with it. Not seen in this tenant,
# and the ingest prints every excluded title — but assert the tradeoff is real
# so nobody rediscovers it as a mystery.
edge = "Preparatory Meeting - Coordination Systems Inc"
if prep(edge):
    print(f"\nFAIL  expected the known tradeoff: {edge} is dropped by the 'coordination' pattern")
    fail += 1
else:
    print(f"\nok    KNOWN TRADEOFF, not a bug: '{edge}'")
    print("      is dropped because the exclusion runs on the whole title. Visible in the")
    print("      PREP TITLE EXCLUSIONS diagnostic if a vendor is ever named like this.")

print("\nempty/None titles:", prep(None), prep(""))
fail += bool(prep(None)) + bool(prep(""))
sys.exit(1 if fail else 0)
