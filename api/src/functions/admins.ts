import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { addAdmin, getSettings, listAdmins, removeAdmin } from '../db/queries.js';
import { cacheBust } from '../cache.js';
import { BOOTSTRAP_ADMIN_LIST, actorEmail, errorResponse, meta, requireAdmin } from './_shared.js';

/**
 * GET    /api/admins            the admin list + the current admin mode
 * POST   /api/admins            { email }        add
 * DELETE /api/admins/{email}                     remove
 *
 * While `adminMode` is 'open' every signed-in user can edit, so this list has no
 * effect yet — it is being maintained now so that flipping to 'allowlist' later
 * is a single setting change rather than a scramble.
 */
app.http('admins', {
  methods: ['GET', 'POST'],
  authLevel: 'anonymous',
  route: 'admins',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      if (request.method === 'GET') {
        const [admins, settings] = await Promise.all([
          listAdmins(BOOTSTRAP_ADMIN_LIST()),
          getSettings(),
        ]);
        return meta(admins, { adminMode: settings.adminMode }, 0);
      }

      const denied = await requireAdmin(request);
      if (denied) return denied;

      const body = (await request.json()) as Record<string, unknown>;
      const email = String(body.email ?? '').trim().toLowerCase();
      // Loose on purpose — Entra UPNs aren't always classic email shapes, and a
      // strict pattern here would reject valid accounts. The list is only
      // consulted for exact matches against a signed-in principal, so a typo
      // grants nobody anything; it just sits there doing nothing.
      if (!email || !email.includes('@')) {
        return { status: 400, jsonBody: { error: 'A valid email address is required.' } };
      }

      await addAdmin(email, actorEmail(request), BOOTSTRAP_ADMIN_LIST());
      cacheBust('tracker:');
      return meta(await listAdmins(BOOTSTRAP_ADMIN_LIST()), { added: email }, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});

app.http('adminDelete', {
  methods: ['DELETE'],
  authLevel: 'anonymous',
  route: 'admins/{email}',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const denied = await requireAdmin(request);
      if (denied) return denied;

      const email = decodeURIComponent(request.params.email ?? '');
      if (!email) return { status: 400, jsonBody: { error: 'email is required.' } };

      // removeAdmin refuses to delete a bootstrap account or the last admin;
      // surface its reason rather than a bare failure, because both refusals are
      // deliberate and the user needs to know which one they hit.
      const result = await removeAdmin(email, BOOTSTRAP_ADMIN_LIST());
      if (!result.ok) return { status: 409, jsonBody: { error: result.error } };

      cacheBust('tracker:');
      return meta(await listAdmins(BOOTSTRAP_ADMIN_LIST()), { removed: email }, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});
