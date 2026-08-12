import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { getProjectDetail, getSettings } from '../db/queries.js';
import { cacheGet, cacheSet } from '../cache.js';
import { errorResponse, isFresh, meta } from './_shared.js';

/**
 * GET /api/projects/{id}
 *
 * One project's full picture: the vendor checklist (outstanding first), every
 * prep meeting logged against the job, and the meetings that couldn't be
 * credited to a vendor.
 */
app.http('projectDetail', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'projects/{id}',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const id = Number(request.params.id);
      if (!Number.isFinite(id)) {
        return { status: 400, jsonBody: { error: 'A numeric Procore project id is required.' } };
      }

      const fresh = isFresh(request);
      const key = `project:${id}`;
      if (!fresh) {
        const hit = cacheGet<HttpResponseInit>(key);
        if (hit) return hit;
      }

      const [detail, settings] = await Promise.all([getProjectDetail(id), getSettings()]);
      if (!detail.project) {
        return { status: 404, jsonBody: { error: `Project ${id} isn't in the mirror.` } };
      }

      const held = detail.vendors.filter((v) => v.status === 'held').length;
      const tracked = detail.vendors.filter((v) => v.status !== 'not_applicable').length;

      const response = meta(
        detail.vendors,
        {
          project: detail.project,
          meetings: detail.meetings,
          unmatched: detail.unmatched,
          settings,
          summary: {
            vendor_total: tracked,
            vendor_held: held,
            vendor_outstanding: tracked - held,
            vendor_not_applicable: detail.vendors.length - tracked,
            pct_complete: tracked ? Math.round((100 * held) / tracked) : null,
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
