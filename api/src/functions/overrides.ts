import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { clearOverride, listOverrides, saveOverride } from '../db/queries.js';
import { cacheBust } from '../cache.js';
import { actorEmail, errorResponse, meta, requireAdmin } from './_shared.js';
import { normalizeCompany } from '../normalize.js';

const VALID = new Set(['held', 'not_held', 'not_applicable']);

/**
 * GET    /api/overrides   list every manual override (audit trail)
 * POST   /api/overrides   { project_id, vendor_normalized | vendor_name,
 *                           status, note?, meeting_date? }
 *                         status 'clear' removes the override.
 *
 * This is how the tracker stays honest when Procore doesn't match reality: a
 * prep meeting held but never logged, a title match the matcher got wrong, or a
 * company that never needed a meeting in the first place.
 */
app.http('overrides', {
  methods: ['GET', 'POST'],
  authLevel: 'anonymous',
  route: 'overrides',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      if (request.method === 'GET') {
        return meta(await listOverrides(), undefined, 0);
      }

      const denied = requireAdmin(request);
      if (denied) return denied;

      const body = (await request.json()) as Record<string, unknown>;
      const projectId = Number(body.project_id);
      if (!Number.isFinite(projectId)) {
        return { status: 400, jsonBody: { error: 'project_id is required.' } };
      }

      // Accept either the normalized key (from the UI) or a raw name (from a
      // script), normalizing with the same rules the matcher uses so a
      // hand-entered override actually lines up with a roster row.
      const vendorNormalized =
        String(body.vendor_normalized ?? '').trim() ||
        normalizeCompany(String(body.vendor_name ?? ''));
      if (!vendorNormalized) {
        return { status: 400, jsonBody: { error: 'vendor_normalized or vendor_name is required.' } };
      }

      const status = String(body.status ?? '');
      if (status === 'clear') {
        await clearOverride(projectId, vendorNormalized);
      } else if (VALID.has(status)) {
        await saveOverride(projectId, vendorNormalized, status as 'held', {
          note: (body.note as string) ?? null,
          meetingDate: (body.meeting_date as string) ?? null,
          actor: actorEmail(request),
        });
      } else {
        return {
          status: 400,
          jsonBody: { error: `status must be one of held, not_held, not_applicable, clear.` },
        };
      }

      cacheBust('tracker:');
      cacheBust(`project:${projectId}`);
      return meta({ ok: true, project_id: projectId, vendor_normalized: vendorNormalized }, undefined, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});

/** DELETE /api/overrides/{projectId}/{vendorNormalized} */
app.http('overrideDelete', {
  methods: ['DELETE'],
  authLevel: 'anonymous',
  route: 'overrides/{projectId}/{vendor}',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const denied = requireAdmin(request);
      if (denied) return denied;

      const projectId = Number(request.params.projectId);
      const vendor = decodeURIComponent(request.params.vendor ?? '');
      if (!Number.isFinite(projectId) || !vendor) {
        return { status: 400, jsonBody: { error: 'projectId and vendor are required.' } };
      }
      await clearOverride(projectId, vendor);
      cacheBust('tracker:');
      cacheBust(`project:${projectId}`);
      return meta({ ok: true }, undefined, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});
