import { app, HttpRequest, HttpResponseInit } from '@azure/functions';
import { addManualVendor, removeManualVendor } from '../db/queries.js';
import { cacheBust } from '../cache.js';
import { actorEmail, errorResponse, meta, requireAdmin } from './_shared.js';
import { normalizeCompany } from '../normalize.js';

/**
 * POST /api/manual-vendors  { project_id, vendor_name, trade?, note? }
 *
 * Adds a vendor to a project's checklist that neither Procore roster knows
 * about — a sub working under another trade's contract, or one on site before
 * the paperwork caught up. Without this the tracker can only ever be as complete
 * as Procore's contract/directory hygiene.
 */
app.http('manualVendors', {
  methods: ['POST'],
  authLevel: 'anonymous',
  route: 'manual-vendors',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const denied = await requireAdmin(request);
      if (denied) return denied;

      const body = (await request.json()) as Record<string, unknown>;
      const projectId = Number(body.project_id);
      const vendorName = String(body.vendor_name ?? '').trim();
      if (!Number.isFinite(projectId) || !vendorName) {
        return { status: 400, jsonBody: { error: 'project_id and vendor_name are required.' } };
      }

      const vendorNormalized = normalizeCompany(vendorName);
      if (!vendorNormalized) {
        return {
          status: 400,
          jsonBody: { error: 'vendor_name normalized to nothing — it needs at least one letter or digit.' },
        };
      }

      await addManualVendor(projectId, vendorName, vendorNormalized, {
        trade: (body.trade as string) ?? null,
        note: (body.note as string) ?? null,
        actor: actorEmail(request),
      });

      cacheBust('tracker:');
      cacheBust('metrics:');
      cacheBust(`project:${projectId}`);
      return meta({ ok: true, project_id: projectId, vendor_normalized: vendorNormalized }, undefined, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});

/** DELETE /api/manual-vendors/{projectId}/{vendorNormalized} */
app.http('manualVendorDelete', {
  methods: ['DELETE'],
  authLevel: 'anonymous',
  route: 'manual-vendors/{projectId}/{vendor}',
  handler: async (request: HttpRequest): Promise<HttpResponseInit> => {
    try {
      const denied = await requireAdmin(request);
      if (denied) return denied;

      const projectId = Number(request.params.projectId);
      const vendor = decodeURIComponent(request.params.vendor ?? '');
      if (!Number.isFinite(projectId) || !vendor) {
        return { status: 400, jsonBody: { error: 'projectId and vendor are required.' } };
      }
      await removeManualVendor(projectId, vendor);
      cacheBust('tracker:');
      cacheBust('metrics:');
      cacheBust(`project:${projectId}`);
      return meta({ ok: true }, undefined, 0);
    } catch (err) {
      return errorResponse(err);
    }
  },
});
