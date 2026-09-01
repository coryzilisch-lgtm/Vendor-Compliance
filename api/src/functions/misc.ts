import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { getSettings, getSyncStatus, getUnmatchedMeetings, settingsHealth } from '../db/queries.js';
import { errorResponse, getClientPrincipal, isAdmin, meta, requestEmails } from './_shared.js';

/** GET /api/health — liveness only; deliberately does not touch SQL. */
app.http('health', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'health',
  handler: async (): Promise<HttpResponseInit> =>
    meta({ status: 'ok', timestamp: new Date().toISOString() }, undefined, 0),
});

/**
 * GET /api/me — drives the dashboard's admin vs view-only state.
 *
 * ⚠️ This handler MUST NOT throw. It used to be pure header parsing, but
 * resolving admin rights now reads the settings table, and an unhandled throw
 * here 500s — which the dashboard catches and treats as "not an admin". The
 * result was a silent demotion to viewer with no way to tell whether you were
 * genuinely unlisted or the database was simply unreachable. It now always
 * answers, and says WHY the answer is what it is.
 */
app.http('me', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'me',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    const p = getClientPrincipal(request);
    if (!p) {
      return {
        status: 401,
        jsonBody: {
          error:
            'Not signed in — Static Web Apps did not attach an x-ms-client-principal header. ' +
            'Check that the Entra identity provider is configured and that /api/* requires ' +
            'the "authenticated" role in staticwebapp.config.json.',
        },
      };
    }

    const emails = requestEmails(request);
    let admin = false;
    let mode: string | null = null;
    let failure: string | null = null;

    try {
      const settings = await getSettings();
      mode = settings.adminMode;
      admin = await isAdmin(request);
    } catch (err) {
      failure = (err as Error)?.message ?? String(err);
      console.error('[me] admin resolution failed:', failure);
    }

    // Everything the "why am I a viewer?" question needs, without a log dive.
    const why = failure
      ? `Admin could not be resolved: ${failure}`
      : mode === 'open'
        ? 'Admin mode is "open" — every signed-in user can edit.'
        : admin
          ? 'Your address is on the admin list.'
          : emails.length
            ? `Admin mode is "allowlist" and none of ${emails.join(', ')} is on it.`
            : 'Admin mode is "allowlist" and the sign-in token carried no email/UPN claim to match ' +
              'against. Add the "email" optional claim to the Entra app registration, or switch ' +
              'admin mode back to "open".';

    return meta(
      {
        email: p.userDetails ?? null,
        identityProvider: p.identityProvider ?? null,
        is_admin: admin,
        admin_mode: mode,
        emails_seen: emails,
        reason: why,
        settings_degraded: settingsHealth(),
      },
      undefined,
      0,
    );
  },
});

/** GET /api/sync-status — when the Fabric pipeline last landed vendor data. */
app.http('syncStatus', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'sync-status',
  handler: async (): Promise<HttpResponseInit> => {
    try {
      return meta(await getSyncStatus(), undefined, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});

/**
 * GET /api/unmatched-meetings?scope=active|all
 *
 * Prep meetings that were logged but couldn't be credited to any vendor. These
 * are real meetings doing no work in the tracker — usually one titled with a
 * person's name rather than a company, or a sub missing from both Procore
 * rosters. Every one is either a vendor to add or a title to fix.
 */
app.http('unmatchedMeetings', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'unmatched-meetings',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const scope = request.query.get('scope') === 'all' ? 'all' : 'active';
      return meta(await getUnmatchedMeetings(scope), { scope }, 600);
    } catch (err) {
      return errorResponse(err);
    }
  },
});
