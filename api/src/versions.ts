/**
 * Data versions for the browser cache, and the one place an admin write
 * invalidates every read cache.
 *
 * The tracker, the Review Queue, the Metrics tab and every project drilldown
 * are computed from two kinds of input:
 *   - the nightly mirror (vendor_roster, vendor_prep_meetings, vendor_prep_matches…),
 *     which changes once a day, around 4 AM, when the pipeline lands;
 *   - the live admin tables (overrides, manual vendors, settings), which change
 *     a few times a day at most.
 * So a result is valid until one of those moves. `currentVersions()` names both:
 *
 *   mirror  the newest `_fabric_loaded_at` across the mirrored tables (ISO string,
 *           '' when unknown). Read through the 300 s process cache, so asking for
 *           it costs Fabric one small query per instance per five minutes.
 *   admin   the time of the last admin write THIS INSTANCE handled ('0' before
 *           any). It is in-process on purpose: it is free, and a database-backed
 *           counter would cost a query on exactly the path this exists to spare.
 *           The cost is that another Function instance can report an older admin
 *           version and serve its own in-memory copy until that copy's 15-minute
 *           TTL. That is the same staleness the process cache already had; the
 *           browser cache adds none.
 *
 * The dashboard keys its localStorage copies on both and skips the request
 * entirely while they match. `?fresh=1` still bypasses every layer.
 */
import { cacheBust, cacheGet, cacheSet } from './cache.js';
import { getSyncStatus } from './db/queries.js';

const SYNC_KEY = 'sync-status';
const SYNC_TTL_SEC = 300;

let lastAdminWriteAt = 0;

/** When the Fabric pipeline last landed vendor data; 300 s process cache. */
export async function syncStatusCached(fresh = false): Promise<Record<string, unknown>> {
  if (!fresh) {
    const hit = cacheGet<Record<string, unknown>>(SYNC_KEY);
    if (hit) return hit;
  }
  const status = await getSyncStatus();
  cacheSet(SYNC_KEY, SYNC_TTL_SEC, status);
  return status;
}

/** The newest mirror timestamp in a sync-status row, as an ISO string. */
export function mirrorVersionOf(status: Record<string, unknown>): string {
  let max = 0;
  for (const key of ['meetings_loaded_at', 'roster_loaded_at']) {
    const t = new Date(status[key] as string | number | Date).getTime();
    if (Number.isFinite(t) && t > max) max = t;
  }
  return max ? new Date(max).toISOString() : '';
}

export function adminVersion(): string {
  return lastAdminWriteAt ? String(lastAdminWriteAt) : '0';
}

export type Versions = { mirror: string; admin: string };

/** Both versions, for the `meta.versions` envelope field. Never throws. */
export async function currentVersions(): Promise<Versions> {
  const status = await syncStatusCached().catch(() => ({}) as Record<string, unknown>);
  return { mirror: mirrorVersionOf(status), admin: adminVersion() };
}

/**
 * Every admin write ends here: drop every derived read cache on this instance
 * (all of them, or just one project's drilldown) and advance the admin version
 * so browsers refetch. Settings, overrides, manual vendors and the admin list
 * all call it; a new write path that forgets to is a stale-tracker bug.
 */
export function bustAfterAdminWrite(projectId?: number): void {
  lastAdminWriteAt = Math.max(Date.now(), lastAdminWriteAt + 1);
  cacheBust('tracker:');
  cacheBust('metrics:');
  cacheBust('unmatched:');
  cacheBust('coverage');
  cacheBust(Number.isFinite(projectId) ? `project:${projectId}` : 'project:');
}
