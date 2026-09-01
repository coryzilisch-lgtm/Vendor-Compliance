import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { getMetrics } from '../db/queries.js';
import { cacheGet, cacheSet } from '../cache.js';
import { errorResponse, isFresh, meta } from './_shared.js';

/**
 * GET /api/metrics?months=12
 *
 * Company-wide prep-meeting metrics. Everything here is either a present-day
 * snapshot or a count of things that actually happened in a month — see the
 * note on getMetrics about why coverage-over-time is deliberately absent.
 */
app.http('metrics', {
  methods: ['GET'],
  authLevel: 'anonymous',
  route: 'metrics',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const months = Number(request.query.get('months')) || 12;
      const fresh = isFresh(request);
      const key = `metrics:${months}`;
      if (!fresh) {
        const hit = cacheGet<HttpResponseInit>(key);
        if (hit) return hit;
      }
      const data = await getMetrics(months);
      const response = meta(data, undefined, fresh ? 0 : 3600);
      if (!fresh) cacheSet(key, 900, response);
      return response;
    } catch (err) {
      return errorResponse(err);
    }
  },
});
