import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { getRosterCoverage, getSettings, saveSettings } from '../db/queries.js';
import { cacheBust } from '../cache.js';
import { actorEmail, errorResponse, meta, requireAdmin } from './_shared.js';

/**
 * GET  /api/settings  current resolution settings + the roster coverage numbers
 *                     that tell an admin which vendorSource to pick
 * POST /api/settings  { vendorSource?, requireVendorPresent?, allowTitleMatch?,
 *                       requireMeetingHeld? }
 *
 * These change how every row is resolved, so the write busts the whole cache.
 */
app.http('settings', {
  methods: ['GET', 'POST'],
  authLevel: 'anonymous',
  route: 'settings',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      if (request.method === 'GET') {
        const [settings, coverage] = await Promise.all([
          getSettings(),
          getRosterCoverage().catch(() => ({})),
        ]);
        return meta(settings, { coverage }, 0);
      }

      const denied = requireAdmin(request);
      if (denied) return denied;

      const body = (await request.json()) as Record<string, unknown>;
      const settings = await saveSettings(body, actorEmail(request));

      cacheBust('tracker:');
      cacheBust('project:');
      return meta(settings, undefined, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});
