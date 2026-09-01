import { getSettings, isAdminEmail } from '../db/queries.js';
import { HttpRequest, HttpResponseInit } from '@azure/functions';

// ─── Identity & authorization (Entra ID via Static Web Apps) ────────────────
// SWA terminates Entra sign-in and injects the signed-in user on every request
// as the base64-JSON `x-ms-client-principal` header. READ access is enforced by
// the route config in staticwebapp.config.json (allowedRoles: ["authenticated"])
// before a request reaches the function, so reads only need a signed-in user.
// WRITE endpoints additionally gate on the admin allowlist below.

/**
 * BOOTSTRAP admins — permanent, and the recovery path.
 *
 * The live admin list is `dbo.vendor_admins`, editable from Settings. These
 * addresses are seeded into it on first use and can never be removed through
 * the API, so an in-app edit can't leave the tracker with nobody able to
 * administer it. Changing who is permanent means changing this list and
 * redeploying; changing who is an admin day-to-day is done in the UI.
 *
 * Note this only matters when `adminMode` is 'allowlist'. It currently defaults
 * to 'open' — every signed-in user can edit — because Entra app roles aren't
 * assigned yet. The SWA route already requires authentication, so 'open' means
 * "anyone at BCI who can reach the app", not "anyone".
 */
export const BOOTSTRAP_ADMINS: string[] = [
  'julia.hoff@buffaloconstruction.com',
  'justin.houston@buffaloconstruction.com',
  'robert.burns@buffaloconstruction.com',
  'matthew.frazier@buffaloconstruction.com',
  'cory.zilisch@buffaloconstruction.com',
];

export type ClientPrincipal = {
  identityProvider?: string;
  userId?: string;
  userDetails?: string;
  userRoles?: string[];
  claims?: Array<{ typ?: string; val?: string }>;
};

export function getClientPrincipal(request: HttpRequest): ClientPrincipal | null {
  const header = request.headers.get('x-ms-client-principal');
  if (!header) return null;
  try {
    return JSON.parse(Buffer.from(header, 'base64').toString('utf8')) as ClientPrincipal;
  } catch {
    return null;
  }
}

/** Every email/UPN-ish value on the principal, lowercased. Entra usually puts
 *  the email in userDetails; common email/upn claims are scanned as a fallback. */
function principalEmails(p: ClientPrincipal): string[] {
  const out: string[] = [];
  if (p.userDetails) out.push(p.userDetails);
  for (const c of p.claims ?? []) {
    if (c.val && /(emailaddress|^email$|preferred_username|upn)$/i.test(c.typ ?? '')) {
      out.push(c.val);
    }
  }
  return out.map((e) => e.trim().toLowerCase()).filter(Boolean);
}

/** Emails on the signed-in principal, or [] when unauthenticated. */
export function requestEmails(request: HttpRequest): string[] {
  const p = getClientPrincipal(request);
  return p ? principalEmails(p) : [];
}

/**
 * Resolve admin rights for a request.
 *
 * In 'open' mode being SIGNED IN is sufficient — deliberately not "signed in
 * AND has a readable email claim". Depending on how the Entra app is
 * configured, `userDetails` and the email claims can come back empty, and
 * gating on an email would silently make everyone read-only, which is exactly
 * the opposite of what 'open' is for. The SWA route already refuses anonymous
 * requests before they reach us, so a principal is the real signal.
 */
async function resolveAdmin(p: ClientPrincipal | null): Promise<boolean> {
  if (!p) return false;
  const settings = await getSettings();
  if (settings.adminMode === 'open') return true;
  return isAdminEmail(principalEmails(p), BOOTSTRAP_ADMINS);
}

export async function isAdmin(request: HttpRequest): Promise<boolean> {
  return resolveAdmin(getClientPrincipal(request));
}

/** Best display identity for audit columns on writes. */
export function actorEmail(request: HttpRequest): string {
  const p = getClientPrincipal(request);
  return (p && principalEmails(p)[0]) || 'unknown';
}

/**
 * Write-side gate: 401 if not signed in, 403 if signed in but not an admin.
 * Async because the allowlist lives in the database, not in code.
 */
export async function requireAdmin(request: HttpRequest): Promise<HttpResponseInit | null> {
  const p = getClientPrincipal(request);
  if (!p) return { status: 401, jsonBody: { error: 'Sign-in required' } };
  if (!(await resolveAdmin(p))) {
    return {
      status: 403,
      jsonBody: {
        error:
          'Admin access required — editing the tracker is restricted to the admin list ' +
          '(Settings → Admin access).',
      },
    };
  }
  return null;
}

/** Wrap a payload in the dashboard's { data, meta } envelope. */
export function meta(
  data: unknown,
  extra?: Record<string, unknown>,
  ttlSec = 3600,
): HttpResponseInit {
  const arr = Array.isArray(data) ? data : [data];
  return {
    headers:
      ttlSec > 0
        ? { 'Cache-Control': `public, max-age=${ttlSec}, stale-while-revalidate=3600` }
        : { 'Cache-Control': 'no-store' },
    jsonBody: { data, meta: { count: arr.length, ...extra } },
  };
}

/** True when the caller asked to bypass every cache layer. */
export function isFresh(request: HttpRequest): boolean {
  return request.query.get('fresh') === '1';
}

/** Map errors to a useful response, with hints for the common mirror/config issues. */
export function errorResponse(err: unknown): HttpResponseInit {
  const message = (err as Error)?.message ?? String(err);
  console.error('[api] Error:', message);

  if (/Invalid object name/i.test(message)) {
    return {
      status: 503,
      jsonBody: {
        error:
          `${message} — the vendor tables aren't in the Safety-Dash SQL DB yet. ` +
          `Run fabric/ingest_vendor_compliance.py, then fabric/build_vendor_gold.py, ` +
          `then the mirror-vendor-to-sql pipeline. See docs/setup.md.`,
      },
    };
  }
  if (/Login failed|AZURE_CLIENT|FABRIC_SQL|token/i.test(message)) {
    return {
      status: 500,
      jsonBody: {
        error:
          `${message} — check the SWA app settings (AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / ` +
          `AZURE_TENANT_ID, FABRIC_SQL_SERVER / FABRIC_SQL_DATABASE) and that the service ` +
          `principal still has Contributor on the workspace AND Read/Write all data on the SQL DB item.`,
      },
    };
  }
  return { status: 500, jsonBody: { error: message } };
}
