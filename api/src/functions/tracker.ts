import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { getProjectSummaries, getSettings, getSyncStatus } from '../db/queries.js';
import { cacheGet, cacheSet } from '../cache.js';
import { errorResponse, isFresh, meta } from './_shared.js';

/**
 * GET /api/tracker?scope=active|all
 *
 * The landing view: one row per project with its prep-meeting completion.
 * Deliberately does NOT include the per-vendor rows — with the directory vendor
 * source a busy job carries ~200 companies, and shipping every one of them for
 * every project would be a multi-megabyte payload for a screen that only renders
 * the counts. The vendor checklist comes from /api/projects/{id} on drilldown.
 */
app.http('tracker', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'tracker',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const scope = request.query.get('scope') === 'all' ? 'all' : 'active';
      const fresh = isFresh(request);
      const key = `tracker:${scope}`;

      if (!fresh) {
        const hit = cacheGet<HttpResponseInit>(key);
        if (hit) return hit;
      }

      const [projects, settings, sync] = await Promise.all([
        getProjectSummaries(scope),
        getSettings(),
        getSyncStatus().catch(() => ({})),
      ]);

      const totals = projects.reduce(
        (acc, p) => {
          acc.vendors += p.vendor_total;
          acc.held += p.vendor_held;
          acc.outstanding += p.vendor_outstanding;
          acc.unmatched += p.unmatched_meeting_count;
          return acc;
        },
        { vendors: 0, held: 0, outstanding: 0, unmatched: 0 },
      );

      const response = meta(
        projects,
        {
          scope,
          settings,
          sync,
          totals: {
            ...totals,
            projects: projects.length,
            pct_complete: totals.vendors ? Math.round((100 * totals.held) / totals.vendors) : null,
          },
        },
        fresh ? 0 : 3600,
      );

      if (!fresh) cacheSet(key, 900, response);
      return response;
    } catch (err) {
      return errorResponse(err);
    }
  },
});
