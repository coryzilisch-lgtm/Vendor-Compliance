import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { getSyncStatus, getUnmatchedMeetings } from '../db/queries.js';
import { errorResponse, getClientPrincipal, isAdmin, meta } from './_shared.js';

/** GET /api/health — liveness only; deliberately does not touch SQL. */
app.http('health', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'health',
  handler: async (): Promise<HttpResponseInit> =>
    meta({ status: 'ok', timestamp: new Date().toISOString() }, undefined, 0),
});

/** GET /api/me — drives the dashboard's admin vs view-only state. */
app.http('me', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'me',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    const p = getClientPrincipal(request);
    if (!p) return { status: 401, jsonBody: { error: 'Not signed in' } };
    return meta(
      {
        email: p.userDetails ?? null,
        identityProvider: p.identityProvider ?? null,
        is_admin: await isAdmin(request),
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
