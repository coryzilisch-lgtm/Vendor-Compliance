# WAF exclusions: Vendor Compliance tracker

> Generated from the code on 2026-10-30. **Update this file in the same PR as any change to a
> route, method, query parameter, custom header, cookie, content type, upload, body shape or
> non-browser caller** (see `.claude/skills/waf-compatibility/SKILL.md`). The company-wide
> write-up covering all five apps, with the consolidated rule list IT applies, is
> https://claude.ai/artifact/Q6EaFGC4UZ5CtvxjKJdzMA.

## How to read this

- **Part A** rows are deterministic: they follow from the code and are applied before cutover.
- **Part B** rows are candidates: the rule fires on the *value* a person types later, which code
  cannot predict. They are confirmed from the WAF log while the policy runs in Detection mode,
  then promoted.
- Exclusion rows are `match variable | operator | selector | scope | why`, which is the shape
  Application Gateway takes (Azure CLI `az network application-gateway waf-policy managed-rule
  exclusion add`, or the portal's Managed rules > Exclusions blade).

## Settings common to every Buffalo app (applied on the gateway, not in this repo)

| Setting | Value | Why |
| --- | --- | --- |
| Host header to backend | the custom domain, never rewritten to `*.azurestaticapps.net` | SWA picks the site and builds the Entra redirect from it |
| Health probe | `/`, accept 200-399 | `/` answers 302 to sign-in when there is no cookie |
| Backend request timeout | 120 s (default 20 s) | long requests; SWA itself kills a Function at 45 s |
| Rule set | DRS 2.1, anomaly threshold 5, per-rule exclusions | |
| Request body inspection | on, first 128 KB | |
| Max request body size enforcement | **off** | raw-body uploads up to 30 MB across the apps; a raw body is not a "file upload" to the WAF |
| `RequestCookieNames` StartsWith `StaticWebAppsAuth` | global | SWA session cookies are opaque base64 |
| `RequestArgNames` Equals (one entry per name) `id_token, code, state, session_state, post_login_redirect_uri` | global | the `/.auth/login/aad/callback` form post and sign-in redirect |

## This app

No custom domain is registered in the repo (resource `blue-hill-02eed4f0f`). Two deploy
workflows exist with different tokens; confirm in the portal that there is one live hostname.

### Request surface

| Item | Detail |
| --- | --- |
| Front end | `dashboard/index.html` only; Google Fonts direct from the browser |
| Routes | 15 handlers under `/api/*`: 10 GET, 4 POST, 3 DELETE. All signed-in; writes need an admin |
| Methods the browser uses | GET, POST, DELETE (`/api/admin-users/{email}`) |
| Content types | `application/json` only; DELETE has no body |
| Custom request headers | none |
| Cookies | SWA auth cookie only |
| Query parameters | `scope`, `fresh`, `months`, `q` (typed person name, 2+ chars) |
| Path parameters | `{id}` numeric; `{email}` percent-encoded; `{vendor}` a normalised vendor name on two DELETE routes the dashboard does not call |
| Largest body | under 2 KB |
| Machine callers | none |

### Part A

Nothing beyond the common settings.

### Part B

| JSON key or location | Endpoint | Content | Rules to expect |
| --- | --- | --- | --- |
| `note` | `POST /api/overrides` | one-line note, e.g. `held on site 4/2, not logged in Procore` | 942xxx |
| `vendor_name` | `POST /api/manual-vendors` | company names with `&`, commas, periods, apostrophes | 942xxx |
| `q` query | `GET /api/people` | a typed name fragment | 942xxx on apostrophes |
| `{vendor}` path segment | the two DELETE routes | a name in the URL path | 920xxx, 942xxx |
| DELETE method | `/api/admin-users/{email}` | | 911100 if it fires |
