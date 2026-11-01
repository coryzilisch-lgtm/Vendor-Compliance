import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { getRosterCoverage, getSettings, saveSettings } from '../db/queries.js';
import { cacheGet, cacheSet } from '../cache.js';
import { bustAfterAdminWrite, currentVersions } from '../versions.js';
import { actorEmail, errorResponse, meta, requireAdmin } from './_shared.js';

/**
 * The roster coverage comparison joins vendor_roster to projects on every
 * Settings tab visit. The roster moves once a night and nothing an admin does
 * changes it, so 15 minutes in the process cache loses nothing. Failure is an
 * empty object, as before, and is never cached.
 */
async function rosterCoverageCached(): Promise<Record<string, unknown>> {
  const hit = cacheGet<Record<string, unknown>>('coverage');
  if (hit) return hit;
  try {
    const coverage = await getRosterCoverage();
    cacheSet('coverage', 900, coverage);
    return coverage;
  } catch {
    return {};
  }
}

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
        const [settings, coverage, versions] = await Promise.all([
          getSettings(),
          rosterCoverageCached(),
          currentVersions(),
        ]);
        return meta(settings, { coverage, versions }, 0);
      }

      const denied = await requireAdmin(request);
      if (denied) return denied;

      const body = (await request.json()) as Record<string, unknown>;
      const settings = await saveSettings(body, actorEmail(request));

      bustAfterAdminWrite();
      return meta(settings, { versions: await currentVersions() }, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});
