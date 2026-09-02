"""variant_match() against the REAL unmatched meetings from the first full gold
build, plus the negative controls that decide whether it is safe to enable."""
import ast, re, sys

SRC = open("/home/user/Vendor-Compliance/fabric/build_vendor_gold.py").read()
tree = ast.parse(SRC)
ns = {"re": re}
for node in tree.body:
    if isinstance(node, ast.Assign) and any(
            getattr(t, "id", "") in ("LEGAL_SUFFIX_TOKENS", "GENERIC_NAME_WORDS",
                                     "MEETING_BOILERPLATE", "VARIANT_MIN_STEM", "VARIANT_ENDINGS",
                                     "TITLE_STOPWORDS", "TITLE_MATCH_MIN_CHARS")
            for t in node.targets):
        exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
    if isinstance(node, ast.FunctionDef) and node.name in (
            "normalize_company", "_same_word", "title_names_tokens", "variant_match"):
        exec(compile(ast.Module([node], []), "<x>", "exec"), ns)
vm = ns["variant_match"]

# Every unmatched meeting from the real build, with the roster it sits next to.
REAL = [
    ("Pre-Contract Meeting Agenda - HIVE",             "Hive Energy Solutions LLC",    True),
    ("Pre-Contract Meeting Agenda - Montesdeoca",      "Montesdeoca Excavation",       True),
    ("Pre-Contract Meeting Agenda - Patriot Pipeline", "Patriot Pipeline Albuquerque", True),
    ("Pre-Contract Meeting Agenda - L&G Construction", "L&G Concrete Construction, Inc", True),
    ("Preparatory Meeting Agenda- H&W LandWorks",      "H&W Landwork KY LLC",          True),
    ("Preparatory Meeting - K&B Electric",             "K&B Electrical Services, Inc", True),
    ("KEN HOUSTON - Preparatory Meeting Agenda",       "Ken Houston Electric LLC",     True),
    # The one the old scorer called a near miss on the strength of the single
    # word "construction". It is a genuinely absent vendor and must NOT match.
    ("Preparatory Meeting Agenda Escar Construction",  "Espana Construction, LLC",     False),
    # Titles naming no company at all must match nothing.
    ("Prep Phase Meeting Agenda",                      "Hive Energy Solutions LLC",    False),
    ("Preparatory Meeting Agenda - Site",              "Montesdeoca Excavation",       False),
]

# The other vendors sitting on those same jobs — the uniqueness gate is only
# meaningful if these stay false.
CONTROLS = [
    ("Pre-Contract Meeting Agenda - Patriot Pipeline", "Patriot Plumbing Solutions LLC", False),
    ("Pre-Contract Meeting Agenda - L&G Construction", "G Cortes Site Control, LLC",     False),
    ("Preparatory Meeting - K&B Electric",             "B Sign Group, Inc.",             False),
    ("Preparatory Meeting Agenda - ABC",               "ABC Construction",               True),
    ("Preparatory Meeting Agenda - ABC",               "ABC Plumbing",                   True),
    # Different trades sharing a first word must never fuse.
    ("Preparatory Meeting Agenda - Patriot Pipeline",  "Patriot Roofing",                False),
    # A title naming only filler must not match a company made of filler.
    ("Preparatory Meeting Agenda - Construction",      "Construction Services Group",    False),
    # The strict rule's own negative controls still hold under the looser one.
    ("Preparatory Meeting Agenda - Excel",             "Excelsior Roofing",              False),
    ("Preparatory Meeting Agenda - ZIP",               "Zipper Systems",                 False),
]

fail = 0
print("=== real unmatched meetings ===")
for title, vendor, expected in REAL:
    got = vm(title, vendor)
    ok = got == expected
    fail += not ok
    print(f"{'ok  ' if ok else 'FAIL'}  {title[:44]:<44} x {vendor[:30]:<30} -> {got}")

print("\n=== controls (other vendors on the same jobs) ===")
for title, vendor, expected in CONTROLS:
    got = vm(title, vendor)
    ok = got == expected
    fail += not ok
    note = "  (both true on purpose: the UNIQUENESS gate rejects this meeting)" \
        if vendor.startswith("ABC") else ""
    print(f"{'ok  ' if ok else 'FAIL'}  {title[:44]:<44} x {vendor[:30]:<30} -> {got}{note}")

# The ABC pair is the whole argument for the uniqueness gate: both fit, so the
# SQL must emit neither. Assert the ambiguity is real rather than assuming it.
abc = [vm("Preparatory Meeting Agenda - ABC", v) for v in ("ABC Construction", "ABC Plumbing")]
if sum(abc) != 2:
    print("FAIL  the ABC ambiguity no longer reproduces — the uniqueness gate is untested")
    fail += 1
else:
    print("\nok    ABC fits 2 vendors -> uniqueness gate suppresses that meeting")

sys.exit(1 if fail else 0)
