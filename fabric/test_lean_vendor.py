"""Pure-Python checks for the lean-nightly decisions in ingest_vendor_compliance.py.

No Spark, no network: the decision functions are lifted out of the notebook
source by name with `ast`, so the test runs against the real code, not a copy.

    python3 -I fabric/test_lean_vendor.py
"""
import ast
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
SRC_PATH = os.path.join(HERE, "ingest_vendor_compliance.py")
SRC = open(SRC_PATH, encoding="utf-8").read()


def lift(*names):
    """Return a module holding only the named top-level functions from the notebook."""
    tree = ast.parse(SRC)
    picked = [n for n in tree.body
              if isinstance(n, ast.FunctionDef) and n.name in names]
    missing = set(names) - {n.name for n in picked}
    assert not missing, f"functions not found in notebook: {missing}"
    mod = types.ModuleType("lean_vendor_bits")
    code = compile(ast.Module(body=picked, type_ignores=[]), SRC_PATH, "exec")
    exec(code, mod.__dict__)
    return mod


bits = lift("roster_refresh_tonight", "roster_fetch_for", "fetch_projects_every_status")

failures = []


def check(cond, msg):
    if cond:
        print(f"  ok   {msg}")
    else:
        failures.append(msg)
        print(f"  FAIL {msg}")


# ---------------------------------------------------------------- roster refresh
print("roster_refresh_tonight")
r = bits.roster_refresh_tonight
check(r(4, 4) is True, "Friday with ROSTER_REFRESH_WEEKDAY=4 refreshes everything")
check(r(0, 4) is False, "Monday with ROSTER_REFRESH_WEEKDAY=4 does not")
check(r(0, None) is True, "ROSTER_REFRESH_WEEKDAY=None refreshes every night (old behaviour)")
check(r(0, 4, backfill=True) is True, "BACKFILL_ALL_HISTORY forces a full refresh")
check(r(0, 4, only_ids=[123]) is True, "ONLY_PROJECT_IDS forces a full refresh (of that scope)")
check(r(0, 4, only_ids=[]) is False, "an empty ONLY_PROJECT_IDS does not")

print("roster_fetch_for (per project, on a non-refresh night)")
f = bits.roster_fetch_for
known = {"101", 102}            # bronze_vendor_directory holds projects 101 and 102
check(f(101, False, known) is False, "a project already in bronze keeps its stored rows")
check(f(102, False, known) is False, "int/str pid mismatch between scope and bronze is tolerated")
check(f(103, False, known) is True, "a project new to scope is fetched")
check(f(101, False, known, only_ids=[101]) is True, "a targeted project is fetched even if known")
check(f(101, True, known) is True, "a refresh night fetches a known project")
check(f(103, False, set()) is True, "empty bronze (first run) fetches everyone")

# ---------------------------------------------------------- Active + Inactive merge
print("fetch_projects_every_status")


class FakePager:
    """Stands in for get_paginated: answers by filters[by_status]."""

    def __init__(self, answers):
        self.answers = answers
        self.calls = []

    def __call__(self, url, params, page_size):
        self.calls.append(dict(params))
        return self.answers.get(params.get("filters[by_status]", "<none>"), [])


active = [{"id": 1, "name": "A", "active": True}, {"id": 2, "name": "B", "active": True}]
inactive = [{"id": 2, "name": "B-dup"}, {"id": 3, "name": "C"}]

pager = FakePager({"Active": active, "Inactive": inactive})
rows = bits.fetch_projects_every_status(pager, "https://x/projects", 18895, 300)
ids = sorted(p["id"] for p in rows)
check(ids == [1, 2, 3], f"Active + Inactive merged by id -> {ids}")
check(len(pager.calls) == 2 and {c["filters[by_status]"] for c in pager.calls} == {"Active", "Inactive"},
      "exactly two requests, capitalised Active and Inactive (never lowercase all)")
check(all(c["company_id"] == 18895 for c in pager.calls), "company_id carried on both")
by_id = {p["id"]: p for p in rows}
check(by_id[2]["name"] == "B", "a project in both lists keeps the Active record")
check(by_id[3].get("active") is False, "an Inactive-only project is marked active=False")
check(by_id[1].get("active") is True, "an Active project's own flag is untouched")

# filter ignored: Inactive returns the same set as Active -> still one copy each, warns
pager2 = FakePager({"Active": active, "Inactive": list(active)})
rows2 = bits.fetch_projects_every_status(pager2, "u", 1, 300)
check(sorted(p["id"] for p in rows2) == [1, 2], "ignored filter does not duplicate projects")

# both empty -> falls back to the default list
pager3 = FakePager({"Active": [], "Inactive": [], "<none>": [{"id": 9}]})
rows3 = bits.fetch_projects_every_status(pager3, "u", 1, 300)
check([p["id"] for p in rows3] == [9], "both filters empty -> default list fallback")

# one side raising does not lose the other
class RaisingPager(FakePager):
    def __call__(self, url, params, page_size):
        if params.get("filters[by_status]") == "Inactive":
            raise RuntimeError("boom")
        return super().__call__(url, params, page_size)

rows4 = bits.fetch_projects_every_status(RaisingPager({"Active": active}), "u", 1, 300)
check(sorted(p["id"] for p in rows4) == [1, 2], "an exception on Inactive keeps the Active list")

# -------------------------------------------------- Bug B: the tuple-unpack fix
print("commitment detail unpack (Bug B)")


def request_json_stub(url, params=None):
    return ({"vendor": {"company": {"name": "Zeta Roofing"}}}, 200)


# The old code: the whole tuple landed in `detail`, so the dict check could never pass.
detail_old = request_json_stub("u")
check(not isinstance(detail_old, dict), "old shape: (payload, status) is not a dict -> no vendor ever recovered")
# The new code unpacks it.
detail_new, _dstatus = request_json_stub("u")
check(isinstance(detail_new, dict) and detail_new["vendor"]["company"]["name"] == "Zeta Roofing",
      "new shape: payload is the dict, vendor reachable")
check("detail, _dstatus = request_json(" in SRC and "detail = request_json(" not in SRC,
      "notebook source unpacks the detail call (and the old assignment is gone)")

# ------------------------------------------------------- raise None after 3 x 429
print("request_json gives up with a real exception")
check("raise last_exc or RuntimeError(" in SRC and "\n    raise last_exc\n" not in SRC,
      "request_json ends with `raise last_exc or RuntimeError(...)`, never `raise None`")

# ----------------------------------------------- write guards (merge-list helpers)
print("write guards")
wb = lift("_merge_list", "_drop_failed")
# _merge_list reads the notebook global `scope` when base is None and something failed
wb.scope = [{"id": 1}, {"id": 2}, {"id": 3}]
check(wb._merge_list([1, 2, 3], {2}) == [1, 3], "a failed project is dropped from the merge list")
check(wb._merge_list(None, set()) is None, "full overwrite stays a full overwrite when nothing failed")
check(wb._merge_list(None, {3}) == [1, 2], "full overwrite with a failure becomes a merge that spares it")
rows = [{"project_procore_id": 1, "x": 1}, {"project_procore_id": 2, "x": 2}]
check(wb._drop_failed(rows, {2}) == [rows[0]], "partial rows of a failed project are discarded")
check(wb._drop_failed(rows, set()) is rows, "nothing failed -> rows untouched")
check("if ids is not None and not ids and not rows:" in SRC,
      "write_delta_merged skips the empty-rows + empty-merge-list case (the table wipe)")

# ------------------------------------------------------------- config + wiring
print("config + wiring")
check("THROTTLE_SEC = 0.1\n" in SRC, "THROTTLE_SEC is 0.1")
check("ROSTER_REFRESH_WEEKDAY = 4\n" in SRC, "ROSTER_REFRESH_WEEKDAY = 4 (Friday)")
check("RUN_DIAGNOSTICS = True\n" in SRC and "if not RUN_DIAGNOSTICS:" in SRC, "RUN_DIAGNOSTICS guards section 9")
check('"filters[by_status]": "all"' not in SRC, "lowercase by_status=all is gone")
check(SRC.index("\n                ensure_project_users()\n") > SRC.index("hit = cached_meeting(mid, m, pid, pname)"),
      "project users are fetched only after a cache miss, right before the detail call")
check("if PULL_COMMITMENTS and _fetch_roster:" in SRC and "if PULL_DIRECTORY and _fetch_roster:" in SRC,
      "commitments and directory pulls are gated on the roster decision")
check("re-read ONLY because the list record carries no" in SRC,
      "DETAIL-CALL REUSE names the no-updated_at re-reads")

print()
if failures:
    print(f"{len(failures)} FAILED")
    sys.exit(1)
print("all checks passed")
