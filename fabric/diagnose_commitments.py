# ============================================================
# Fabric Notebook Cell: COMMITMENTS DIAGNOSTIC (read-only)
# ------------------------------------------------------------
# Settles one question: when Procore's commitment endpoints return HTTP 200 with
# ZERO rows, is that because BCI writes no subcontracts in Procore — or because
# the API service account cannot SEE them?
#
# Those two look identical from the outside, and the tracker's vendor source
# depends on which one is true. An earlier run concluded "BCI has no
# commitments" from empty 200s alone. That was over-claiming: Procore list
# endpoints are permission-filtered, and this codebase already has the scar —
# private Observations came back 200-with-fewer-rows until the service account
# was granted Admin on that tool.
#
# The decisive signal is Procore's **Total response header**: it reports the
# full server-side count for the query. Total > 0 with 0 rows returned means the
# data exists and is being withheld. Total == 0 means it genuinely isn't there.
#
# READ-ONLY. Writes nothing, changes nothing. Run it against a project you KNOW
# has subcontracts in Procore.
# ============================================================

import json
import time
import requests

# ---- SET THIS ---------------------------------------------------------------
# One or more project ids where you can SEE a subcontract / purchase order in
# the Procore UI. That is the whole point: we compare what you can see against
# what the API returns. Leave empty to have it probe the first few active jobs,
# which is much weaker evidence.
PROBE_PROJECT_IDS = []      # e.g. [3421126, 3176472]

SHOW_RAW_BODY_CHARS = 600   # per endpoint, for the first project only

# ============================================================
# Auth — reuse the ingest cell's manager if present
# ============================================================
_g = globals()
if all(k in _g for k in ["get_token", "auth_headers", "company_id", "PROCORE_API_BASE_URL"]):
    print("Reusing token manager from an earlier cell.\n")
else:
    print("Bootstrapping auth from Key Vault...\n")
    vault_url = "https://kv-dataplatform-bci.vault.azure.net/"
    client_id     = notebookutils.credentials.getSecret(vault_url, "procore-client-id")
    client_secret = notebookutils.credentials.getSecret(vault_url, "procore-client-secret")
    company_id    = notebookutils.credentials.getSecret(vault_url, "procore-company-id")
    PROCORE_API_BASE_URL   = "https://api.procore.com"
    PROCORE_LOGIN_BASE_URL = "https://login.procore.com"

    _token = {"value": None, "exp": 0.0}

    def get_token(force=False):
        if not force and _token["value"] and time.time() < _token["exp"] - 300:
            return _token["value"]
        r = requests.post(f"{PROCORE_LOGIN_BASE_URL}/oauth/token",
                          data={"grant_type": "client_credentials",
                                "client_id": client_id, "client_secret": client_secret},
                          timeout=60)
        r.raise_for_status()
        j = r.json()
        _token["value"] = j["access_token"]
        _token["exp"] = time.time() + int(j.get("expires_in", 7200))
        return _token["value"]

    def auth_headers():
        return {"Authorization": f"Bearer {get_token()}",
                "Procore-Company-Id": str(company_id),
                "Content-Type": "application/json"}

    get_token()

CID = str(company_id)

# ============================================================
# Endpoint variants
#
# The ingest only ever tried the first two. If BCI's commitments live behind a
# different shape or version, this is where that shows up.
# ============================================================
def variants(pid):
    return [
        ("v1.0 work_order_contracts (what the ingest uses)",
         f"/rest/v1.0/work_order_contracts", {"project_id": pid}),
        ("v1.0 purchase_order_contracts (what the ingest uses)",
         f"/rest/v1.0/purchase_order_contracts", {"project_id": pid}),
        ("v1.0 work_order_contracts + company_id",
         f"/rest/v1.0/work_order_contracts", {"project_id": pid, "company_id": CID}),
        ("v1.0 nested under project",
         f"/rest/v1.0/projects/{pid}/work_order_contracts", {}),
        ("v1.1 work_order_contracts",
         f"/rest/v1.1/work_order_contracts", {"project_id": pid}),
        ("v2.0 company/project work_order_contracts",
         f"/rest/v2.0/companies/{CID}/projects/{pid}/work_order_contracts", {}),
        ("v1.0 commitments (alias some tenants expose)",
         f"/rest/v1.0/commitments", {"project_id": pid}),
        # Prime contracts are a different tool, but if THESE come back populated
        # while commitments don't, the account can read financials generally and
        # the gap is specific to commitments — which points at tool permission,
        # not at an absent dataset.
        ("v1.0 prime_contracts (control — different financial tool)",
         f"/rest/v1.0/prime_contracts", {"project_id": pid}),
    ]


def probe(label, path, params, show_body=False):
    url = f"{PROCORE_API_BASE_URL}{path}"
    try:
        r = requests.get(url, headers=auth_headers(),
                         params={**params, "per_page": 100}, timeout=90)
    except Exception as e:
        print(f"  {label:<52} EXCEPTION {e}")
        return

    # Procore reports the full server-side count here. It is the whole reason
    # this diagnostic exists: Total > 0 alongside 0 returned rows means the rows
    # exist and are being withheld from this account.
    total = r.headers.get("Total", r.headers.get("total"))
    n = None
    if r.status_code == 200 and r.text:
        try:
            body = r.json()
            rows = body.get("data") if isinstance(body, dict) else body
            n = len(rows) if isinstance(rows, list) else ("obj" if rows else 0)
        except Exception:
            n = "unparseable"

    verdict = ""
    if r.status_code in (401, 403):
        verdict = "  <-- PERMISSION DENIED"
    elif r.status_code == 404:
        verdict = "  <-- endpoint not available in this tenant"
    elif r.status_code == 200 and n == 0 and total not in (None, "", "0"):
        verdict = f"  <-- ⚠ WITHHELD: server says Total={total} but returned 0 rows"
    elif r.status_code == 200 and n == 0:
        verdict = "  <-- genuinely empty (Total=0/absent)"
    elif r.status_code == 200 and isinstance(n, int) and n > 0:
        verdict = "  <-- ✅ DATA FOUND"

    print(f"  {label:<52} HTTP {r.status_code}  rows={n}  Total={total}{verdict}")

    if show_body and r.text:
        print(f"      body: {r.text[:SHOW_RAW_BODY_CHARS]}")


# ============================================================
# Scope
# ============================================================
if PROBE_PROJECT_IDS:
    pids = list(PROBE_PROJECT_IDS)
    print(f"Probing {len(pids)} project(s) you nominated.\n")
else:
    print("⚠️  PROBE_PROJECT_IDS is empty — probing the first 3 active projects.")
    print("    Set it to a project where you can SEE a subcontract in Procore;")
    print("    an empty result on a job with no subs proves nothing.\n")
    r = requests.get(f"{PROCORE_API_BASE_URL}/rest/v1.0/projects",
                     headers=auth_headers(),
                     params={"company_id": CID, "per_page": 100}, timeout=90)
    r.raise_for_status()
    pids = [p.get("id") for p in r.json()[:3] if p.get("id")]

for i, pid in enumerate(pids):
    print("=" * 100)
    print(f"PROJECT {pid}")
    print("=" * 100)
    for label, path, params in variants(pid):
        probe(label, path, params, show_body=(i == 0))
    print()

# ============================================================
# What the account itself can see
# ============================================================
print("=" * 100)
print("API SERVICE ACCOUNT")
print("=" * 100)
try:
    me = requests.get(f"{PROCORE_API_BASE_URL}/rest/v1.0/me",
                      headers=auth_headers(), timeout=60).json()
    print(f"  {me.get('login')}  (id {me.get('id')})")
except Exception as e:
    print(f"  couldn't read /me: {e}")

print("""
HOW TO READ THIS
----------------
✅ DATA FOUND on any row      -> the ingest is calling the wrong endpoint. Tell me
                                 which variant worked and I'll switch to it.
⚠ WITHHELD (Total > 0, 0 rows) -> the data exists and this account cannot see it.
                                 Grant the service account access to the
                                 Commitments tool in Procore (Company Admin ->
                                 permission template for that user), then re-run.
403 / 401                      -> the tool is not granted at all. Same fix.
genuinely empty everywhere,
including on a project where
you can SEE a subcontract      -> that would be surprising; send me the output.
genuinely empty, and prime_contracts
also empty                     -> the account likely has no financial-tool access
                                 at all, which is a permission story, not a data one.
""")
