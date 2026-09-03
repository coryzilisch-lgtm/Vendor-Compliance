import { db } from './client.js';

/* ============================================================================
 * ACTIVE PROJECTS
 *
 * These rules are a deliberate, verbatim port of Safety-Dash's
 * api/src/db/queries.ts. The tracker reads the SAME dbo.projects table in the
 * SAME Fabric SQL DB, so "the active projects we identify in the other
 * dashboards" is enforced by using the same predicate against the same rows —
 * not by a second definition that drifts a release later.
 *
 * ⚠️ If Safety-Dash changes its stage rules, change them here too. The two lists
 * below are the whole definition.
 * ==========================================================================*/

let projectMetaProbed = false;
let projectsHasIsActive = false;
let hasSuperTables = false;
let meetingsHaveTitlePadded = false;

/**
 * The mirror pipeline AUTO-CREATES dbo.projects, so its column set follows
 * whichever build_gold ran last. Naming a column that isn't there is a SQL
 * *parse* error, which fails the whole query and blanks the dashboard — so
 * probe once and degrade instead of assuming.
 *
 * Also probes for the superintendent tables. Note that dbo.projects does NOT
 * carry a superintendent name (it has project_manager, not superintendent) —
 * the mapping lives in dbo.project_superintendents joined to dbo.superintendents.
 * Those are Safety-Dash's tables in this shared DB; if that pipeline hasn't run,
 * the tracker still works and just shows no superintendent.
 */
export async function ensureProjectColumnMeta(): Promise<void> {
  if (projectMetaProbed) return;
  try {
    const { rows } = await db.query<{
      has_active: number | null; has_super: number | null; has_padded: number | null;
    }>(
      `SELECT COL_LENGTH('dbo.projects','is_active')                    AS has_active,
              OBJECT_ID('dbo.project_superintendents','U')              AS has_super,
              COL_LENGTH('dbo.vendor_prep_meetings','title_padded')     AS has_padded`,
    );
    projectsHasIsActive = rows[0]?.has_active != null;
    hasSuperTables = rows[0]?.has_super != null;
    meetingsHaveTitlePadded = rows[0]?.has_padded != null;
  } catch {
    projectsHasIsActive = false;
    hasSuperTables = false;
    meetingsHaveTitlePadded = false;
  }
  if (hasSuperTables) {
    try {
      const { rows } = await db.query<{ has_names: number | null }>(
        `SELECT OBJECT_ID('dbo.superintendents','U') AS has_names`,
      );
      hasSuperTables = rows[0]?.has_names != null;
    } catch {
      hasSuperTables = false;
    }
  }
  projectMetaProbed = true;
}

/**
 * Superintendent name for a project, as a scalar subquery so a project with two
 * supers can never fan the row out. Degrades to NULL when Safety-Dash's tables
 * aren't present. Requires ensureProjectColumnMeta() first.
 */
function superintendentNameExpr(alias = 'p'): string {
  if (!hasSuperTables) return `CAST(NULL AS NVARCHAR(256))`;
  return `(SELECT TOP 1 s.name
             FROM dbo.project_superintendents ps
             JOIN dbo.superintendents s ON s.id = ps.superintendent_id
            WHERE ps.project_id = ${alias}.id
            ORDER BY s.name)`;
}

/**
 * True when Procore marks the project Inactive. CAST first, THEN COALESCE — the
 * reverse order makes SQL Server pick INT type precedence and blow up converting
 * 'true'/'false' if the mirror typed the column NVARCHAR instead of BIT.
 */
function inactiveProject(alias = 'p'): string {
  return `LOWER(COALESCE(CAST(${alias}.is_active AS NVARCHAR(10)), '1')) IN ('0','false')`;
}

/**
 * Stage strings that contain the word "construction" but are NOT live
 * course-of-construction work. Every one is a trap for a naive
 * `LIKE '%construction%'`: "Awarded Preconstruction" (no hyphen),
 * "Construction Hold" (paused mid-flight), "Post Construction" (already done).
 */
const NOT_COURSE_OF_CONSTRUCTION = [
  '%pre-construction%', '%preconstruction%', '%pre construction%',
  '%post-construction%', '%postconstruction%', '%post construction%',
  '%hold%',
  '%closeout%', '%close out%', '%complete%',
];

/** Active course-of-construction projects only. Requires ensureProjectColumnMeta(). */
export function activeStageFilter(alias = 'p'): string {
  const s = `LOWER(COALESCE(${alias}.stage, ''))`;
  const base = `${s} LIKE '%construction%'
      AND ${NOT_COURSE_OF_CONSTRUCTION.map((p) => `${s} NOT LIKE '${p}'`).join('\n      AND ')}`;
  return projectsHasIsActive ? `${base}\n      AND NOT (${inactiveProject(alias)})` : base;
}

/* ============================================================================
 * SETTINGS
 *
 * Applied LIVE at query time, never baked into gold — flipping one of these is
 * a checkbox in the dashboard, not a notebook + pipeline run.
 * ==========================================================================*/

export type VendorSource = 'commitment' | 'directory' | 'either';

/**
 * Who may edit the tracker.
 *
 *   'open'       every signed-in user is an admin. The current default, because
 *                Entra app roles aren't assigned yet — the SWA route already
 *                requires authentication, so this is "anyone at BCI who can
 *                reach the app", not "anyone on the internet".
 *   'allowlist'  only addresses in dbo.vendor_admins (plus the code-level
 *                bootstrap list, which is permanent). Switch to this once Entra
 *                roles are in place.
 */
export type AdminMode = 'open' | 'allowlist';

export type Settings = {
  /** Which Procore roster defines "every vendor on the project".
   *  Default 'either' (the union) on purpose: it is the only value that cannot
   *  silently HIDE a vendor. Run the coverage diagnostic at the bottom of
   *  ingest_vendor_compliance.py, then narrow this to whichever source BCI
   *  actually keeps current. */
  vendorSource: VendorSource;
  /** Require the vendor's attendee to be marked Present/Conference rather than
   *  merely listed. Default OFF: the Present / Absent / For Distribution Only
   *  radio group is frequently left at its default in the field, so switching
   *  this on before checking the data will under-count real meetings.
   *  Applies to attendee matches only — a title match carries no attendance. */
  requireVendorPresent: 0 | 1;
  /** Count a meeting whose title names the vendor even when the vendor was never
   *  added to the attendee list. Default ON — that is how BCI labels these
   *  meetings, and turning it off drops most historical matches. */
  allowTitleMatch: 0 | 1;
  /** Count a meeting whose title names the vendor under a SHORTER trading name
   *  than the roster's legal one ("… - HIVE" vs "Hive Energy Solutions LLC").
   *  Gold emits these as `title_variant` candidates, only where exactly one
   *  vendor on the project fits.
   *
   *  Default ON, at the safety team's request: the vendor is often not in
   *  Procore's attendee list at all, and "Preparatory Meeting - K&B Electric"
   *  is how they record who the meeting was with. Without this, a roster entry
   *  of "K&B Electrical Services, Inc" never meets its own meeting.
   *
   *  It is still the weakest of the three signals and is labelled "Name
   *  variant" wherever it decides a row, because a false "held" is the one
   *  error worth avoiding here — it can put a crew on site without the meeting
   *  that was supposed to precede them, where a false "outstanding" only costs
   *  someone a second look. The uniqueness gate in gold is the main protection:
   *  a title fragment that fits two vendors on the project credits neither.
   *
   *  ⚠️ Worth one look on the Review Queue: "KEN HOUSTON - Preparatory Meeting
   *  Agenda" fits a vendor "Ken Houston Electric LLC", and titling a meeting
   *  with a person's name is a habit here. If that one is a coincidence,
   *  override it to not_held rather than turning the whole signal off. */
  allowNameVariantMatch: 0 | 1;
  /** Require Procore's `held` flag. Default OFF: the flag is rarely flipped, so
   *  requiring it makes almost everything read "not held". */
  requireMeetingHeld: 0 | 1;
  /** See AdminMode. Default 'open' until Entra roles are assigned. */
  adminMode: AdminMode;
};

export const DEFAULT_SETTINGS: Settings = {
  vendorSource: 'either',
  requireVendorPresent: 0,
  allowTitleMatch: 1,
  allowNameVariantMatch: 1,
  requireMeetingHeld: 0,
  adminMode: 'allowlist',
};

let settingsTableReady = false;

async function ensureSettingsTable(): Promise<void> {
  if (settingsTableReady) return;
  await db.query(`
    IF OBJECT_ID('dbo.vendor_settings','U') IS NULL
    CREATE TABLE dbo.vendor_settings (
      setting_key   NVARCHAR(64)  NOT NULL PRIMARY KEY,
      setting_value NVARCHAR(256) NULL,
      updated_by    NVARCHAR(256) NULL,
      updated_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );`);
  settingsTableReady = true;
}

function coerceSettings(raw: Record<string, string | null>): Settings {
  const bit = (k: keyof Settings): 0 | 1 => {
    const v = raw[k];
    if (v === '0') return 0;
    if (v === '1') return 1;
    return DEFAULT_SETTINGS[k] as 0 | 1;
  };
  const src = raw.vendorSource;
  const vendorSource: VendorSource =
    src === 'commitment' || src === 'directory' || src === 'either'
      ? src
      : DEFAULT_SETTINGS.vendorSource;
  const am = raw.adminMode;
  const adminMode: AdminMode =
    am === 'open' || am === 'allowlist' ? am : DEFAULT_SETTINGS.adminMode;
  return {
    vendorSource,
    requireVendorPresent: bit('requireVendorPresent'),
    allowTitleMatch: bit('allowTitleMatch'),
    allowNameVariantMatch: bit('allowNameVariantMatch'),
    requireMeetingHeld: bit('requireMeetingHeld'),
    adminMode,
  };
}

/* getSettings is now on the hot path — every request resolves admin rights
   through it — so memoize briefly. The capacity is shared with three other
   apps; a settings SELECT per request is exactly the kind of avoidable load
   that shows up as interactive delay. Writes bust it immediately, so a setting
   change is still visible on the next request. */
let settingsCache: { value: Settings; expires: number } | null = null;
let lastGoodSettings: Settings | null = null;
let settingsDegraded: string | null = null;
const SETTINGS_TTL_MS = 30_000;

export function bustSettingsCache(): void {
  settingsCache = null;
}

/** Non-null when the last settings read failed and defaults/stale values are in
 *  use. Surfaced by GET /api/me so a degraded state is visible, not silent. */
export function settingsHealth(): string | null {
  return settingsDegraded;
}

/**
 * Settings, with a deliberate fail-soft.
 *
 * This is on the hot path for EVERY request (admin rights resolve through it),
 * so a transient failure here used to take out /api/me entirely and — because
 * the dashboard treats a failed /api/me as "not an admin" — silently demoted
 * everyone to viewer. Now: serve the last good value if we have one, else the
 * documented defaults, and record why.
 *
 * On the security of defaulting to adminMode 'open' when the table is
 * unreadable: if this table can't be read, it can't be written either, so every
 * write path behind the admin UI fails anyway. The fallback grants the ability
 * to see buttons that will not work — not the ability to change anything.
 * ⚠️ If adminMode is ever switched to 'allowlist' as the production norm,
 * change DEFAULT_SETTINGS.adminMode to match so the failure mode follows it.
 */
export async function getSettings(): Promise<Settings> {
  if (settingsCache && Date.now() < settingsCache.expires) return settingsCache.value;
  try {
    return await readSettings();
  } catch (err) {
    const msg = (err as Error)?.message ?? String(err);
    settingsDegraded = msg;
    console.error('[settings] read failed — using ' +
      (lastGoodSettings ? 'last known good values' : 'defaults') + ':', msg);
    return lastGoodSettings ?? { ...DEFAULT_SETTINGS };
  }
}

async function readSettings(): Promise<Settings> {
  await ensureSettingsTable();
  const { rows } = await db.query<{ setting_key: string; setting_value: string | null }>(
    `SELECT setting_key, setting_value FROM dbo.vendor_settings`,
  );
  const raw: Record<string, string | null> = {};
  for (const r of rows) raw[r.setting_key] = r.setting_value;
  const value = coerceSettings(raw);
  settingsCache = { value, expires: Date.now() + SETTINGS_TTL_MS };
  lastGoodSettings = value;
  settingsDegraded = null;
  return value;
}

export async function saveSettings(
  patch: Partial<Record<keyof Settings, unknown>>,
  actor: string,
): Promise<Settings> {
  await ensureSettingsTable();
  // Validate before writing so a bad value can never reach the query builder,
  // which interpolates these into SQL.
  const allowed: Record<string, (v: unknown) => string | null> = {
    vendorSource: (v) =>
      v === 'commitment' || v === 'directory' || v === 'either' ? String(v) : null,
    requireVendorPresent: (v) => (String(v) === '1' ? '1' : String(v) === '0' ? '0' : null),
    allowTitleMatch: (v) => (String(v) === '1' ? '1' : String(v) === '0' ? '0' : null),
    allowNameVariantMatch: (v) => (String(v) === '1' ? '1' : String(v) === '0' ? '0' : null),
    requireMeetingHeld: (v) => (String(v) === '1' ? '1' : String(v) === '0' ? '0' : null),
    adminMode: (v) => (v === 'open' || v === 'allowlist' ? String(v) : null),
  };
  for (const [key, value] of Object.entries(patch)) {
    const validate = allowed[key];
    if (!validate) continue;
    const clean = validate(value);
    if (clean === null) continue;
    await db.query(
      `MERGE dbo.vendor_settings AS t
       USING (SELECT @k AS setting_key) AS s ON t.setting_key = s.setting_key
       WHEN MATCHED THEN UPDATE SET setting_value = @v, updated_by = @actor, updated_at = SYSUTCDATETIME()
       WHEN NOT MATCHED THEN INSERT (setting_key, setting_value, updated_by)
            VALUES (@k, @v, @actor);`,
      { k: key, v: clean, actor },
    );
  }
  bustSettingsCache();
  return getSettings();
}

/* ============================================================================
 * ADMINS
 *
 * The list is editable in-app, but the code-level BOOTSTRAP_ADMINS in
 * functions/_shared.ts are permanent and cannot be removed through the API.
 * That is the lockout guard: an admin list you can edit is an admin list you
 * can empty, and this app's only other recovery route would be a SQL console.
 * ==========================================================================*/

let adminTableReady = false;

async function ensureAdminTable(bootstrap: string[]): Promise<void> {
  if (adminTableReady) return;
  await db.query(`
    IF OBJECT_ID('dbo.vendor_admins','U') IS NULL
    CREATE TABLE dbo.vendor_admins (
      email      NVARCHAR(256) NOT NULL PRIMARY KEY,
      added_by   NVARCHAR(256) NULL,
      added_at   DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );`);
  // Seed the bootstrap accounts, so the allowlist is never empty.
  for (const email of bootstrap) {
    await db.query(
      `IF NOT EXISTS (SELECT 1 FROM dbo.vendor_admins WHERE email = @e)
       INSERT INTO dbo.vendor_admins (email, added_by) VALUES (@e, 'bootstrap');`,
      { e: email.toLowerCase() },
    );
  }

  // ...and RECONCILE. Rows whose added_by is 'bootstrap' are code-owned: if a
  // name is dropped from BOOTSTRAP_ADMINS it must actually lose access, not
  // linger as a stale seed nobody remembers granting. Rows added through the UI
  // carry the granting admin's address instead and are deliberately untouched.
  const keep = bootstrap.map((e) => e.toLowerCase());
  await db.query(
    `DELETE FROM dbo.vendor_admins
      WHERE added_by = 'bootstrap'
        AND email NOT IN (${keep.map((_, i) => `@k${i}`).join(',') || `''`});`,
    Object.fromEntries(keep.map((e, i) => [`k${i}`, e])),
  );

  adminTableReady = true;
}

export async function listAdmins(bootstrap: string[]): Promise<Record<string, unknown>[]> {
  await ensureAdminTable(bootstrap);
  const { rows } = await db.query<{ email: string }>(
    `SELECT email, added_by, added_at FROM dbo.vendor_admins ORDER BY email`,
  );
  const boot = new Set(bootstrap.map((e) => e.toLowerCase()));
  return rows.map((r) => ({ ...r, is_bootstrap: boot.has(String(r.email).toLowerCase()) }));
}

export async function addAdmin(email: string, actor: string, bootstrap: string[]): Promise<void> {
  await ensureAdminTable(bootstrap);
  await db.query(
    `IF NOT EXISTS (SELECT 1 FROM dbo.vendor_admins WHERE email = @e)
     INSERT INTO dbo.vendor_admins (email, added_by) VALUES (@e, @actor);`,
    { e: email.trim().toLowerCase(), actor },
  );
}

/**
 * Remove an admin. Refuses two cases outright rather than letting someone lock
 * the tracker's administration away:
 *   - a bootstrap account (permanent by design, the recovery path)
 *   - the last remaining admin
 */
export async function removeAdmin(
  email: string,
  bootstrap: string[],
): Promise<{ ok: boolean; error?: string }> {
  await ensureAdminTable(bootstrap);
  const target = email.trim().toLowerCase();

  if (bootstrap.map((e) => e.toLowerCase()).includes(target)) {
    return {
      ok: false,
      error:
        `${email} is a built-in admin and can't be removed here — that account is the ` +
        `recovery path if the list is ever emptied.`,
    };
  }

  const { rows } = await db.query<{ n: number }>(`SELECT COUNT(*) AS n FROM dbo.vendor_admins`);
  if ((rows[0]?.n ?? 0) <= 1) {
    return {
      ok: false,
      error: 'Refusing to remove the last admin — someone has to be able to edit the tracker.',
    };
  }

  await db.query(`DELETE FROM dbo.vendor_admins WHERE email = @e`, { e: target });
  return { ok: true };
}

/** Is this signed-in address allowed to edit? Honors the adminMode setting. */
export async function isAdminEmail(emails: string[], bootstrap: string[]): Promise<boolean> {
  const settings = await getSettings();
  if (settings.adminMode === 'open') return emails.length > 0;

  const lower = emails.map((e) => e.toLowerCase());
  if (lower.some((e) => bootstrap.map((b) => b.toLowerCase()).includes(e))) return true;
  if (!lower.length) return false;

  await ensureAdminTable(bootstrap);
  const { rows } = await db.query<{ n: number }>(
    `SELECT COUNT(*) AS n FROM dbo.vendor_admins
      WHERE email IN (${lower.map((_, i) => `@e${i}`).join(',')})`,
    Object.fromEntries(lower.map((e, i) => [`e${i}`, e])),
  );
  return (rows[0]?.n ?? 0) > 0;
}

/* ============================================================================
 * ADMIN-MANAGED TABLES (auto-created, never mirrored from gold)
 * ==========================================================================*/

let adminTablesReady = false;

async function ensureAdminTables(): Promise<void> {
  if (adminTablesReady) return;

  // Per-vendor override of the computed answer.
  //   'held'           the meeting happened but Procore doesn't show it
  //   'not_held'       dismiss a match the matcher got wrong
  //   'not_applicable' this company never needs a prep meeting on this job
  //                    (the owner, an inspector, a materials supplier) — removed
  //                    from the denominator rather than counted as outstanding
  await db.query(`
    IF OBJECT_ID('dbo.vendor_prep_overrides','U') IS NULL
    CREATE TABLE dbo.vendor_prep_overrides (
      id                BIGINT IDENTITY(1,1) PRIMARY KEY,
      project_id        BIGINT        NOT NULL,
      vendor_normalized NVARCHAR(256) NOT NULL,
      status            NVARCHAR(32)  NOT NULL,
      meeting_date      DATE          NULL,
      note              NVARCHAR(1000) NULL,
      created_by        NVARCHAR(256) NULL,
      created_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );`);
  await db.query(`
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'UX_vendor_prep_overrides'
                     AND object_id = OBJECT_ID('dbo.vendor_prep_overrides'))
    CREATE UNIQUE INDEX UX_vendor_prep_overrides
        ON dbo.vendor_prep_overrides (project_id, vendor_normalized);`);

  // Vendors that belong on the checklist but aren't in either Procore roster —
  // the escape hatch for a sub working under someone else's contract, or one
  // added to the job before the paperwork caught up.
  await db.query(`
    IF OBJECT_ID('dbo.vendor_manual_roster','U') IS NULL
    CREATE TABLE dbo.vendor_manual_roster (
      id                BIGINT IDENTITY(1,1) PRIMARY KEY,
      project_id        BIGINT        NOT NULL,
      vendor_normalized NVARCHAR(256) NOT NULL,
      vendor_name       NVARCHAR(256) NOT NULL,
      trade_name        NVARCHAR(256) NULL,
      note              NVARCHAR(1000) NULL,
      created_by        NVARCHAR(256) NULL,
      created_at        DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    );`);
  await db.query(`
    IF NOT EXISTS (SELECT 1 FROM sys.indexes
                   WHERE name = 'UX_vendor_manual_roster'
                     AND object_id = OBJECT_ID('dbo.vendor_manual_roster'))
    CREATE UNIQUE INDEX UX_vendor_manual_roster
        ON dbo.vendor_manual_roster (project_id, vendor_normalized);`);

  adminTablesReady = true;
}

/* ============================================================================
 * THE RESOLUTION QUERY
 *
 * Builds the vendor checklist live from:
 *   dbo.vendor_roster        (+ dbo.vendor_manual_roster)   the denominator
 *   dbo.vendor_prep_matches                                  the candidate matches
 *   dbo.vendor_prep_overrides                                the human's last word
 * with the settings above deciding which matches count.
 * ==========================================================================*/

/** Which roster rows form the denominator, per the vendorSource setting. */
function vendorSourcePredicate(s: Settings, alias = 'r'): string {
  switch (s.vendorSource) {
    case 'commitment':
      return `CAST(${alias}.from_commitment AS NVARCHAR(10)) IN ('1','true','True')`;
    case 'directory':
      return `CAST(${alias}.from_directory AS NVARCHAR(10)) IN ('1','true','True')`;
    default:
      return '1 = 1';
  }
}

/**
 * Live title matching for manually-added vendors.
 *
 * Depends on `title_padded` — the normalized, space-padded meeting title that
 * build_vendor_gold emits so the same whole-token test can run here without
 * re-implementing normalize_company in T-SQL. It's a newer column, so it is
 * PROBED: a mirror built before that change simply doesn't get live title
 * matching for manual vendors, rather than failing the whole query with an
 * invalid-column parse error and blanking the dashboard.
 */
function manualTitleMatchBranch(): string {
  if (!meetingsHaveTitlePadded) return '';
  return `
      UNION ALL

      -- Manual vendor, matched on the meeting title. CHARINDEX over the padded
      -- title is the T-SQL equivalent of the notebook's INSTR(token_pad(...))
      -- test: ' zip ' matches ' preparatory meeting zip ' but never ' zipper '.
      SELECT
          CAST(mv.project_id AS BIGINT),
          CAST(mv.vendor_normalized AS NVARCHAR(256)),
          CAST(mtg.meeting_id AS BIGINT),
          CAST(mtg.title AS NVARCHAR(4000)),
          CAST(mtg.meeting_date AS DATE),
          CAST(mtg.held AS NVARCHAR(10)),
          CAST('title' AS NVARCHAR(16)),
          CAST(NULL AS NVARCHAR(10)),
          CAST(NULL AS NVARCHAR(256)),
          CAST(NULL AS NVARCHAR(64))
      FROM dbo.vendor_manual_roster mv
      JOIN proj ON proj.project_id = mv.project_id
      JOIN dbo.vendor_prep_meetings mtg ON mtg.project_id = mv.project_id
      WHERE mtg.title_padded IS NOT NULL
        AND mtg.title_padded <> ''
        AND LEN(REPLACE(mv.vendor_normalized, ' ', '')) >= 3
        AND CHARINDEX(' ' + mv.vendor_normalized + ' ', mtg.title_padded) > 0`;
}

/** Which match rows count, per the settings. */
function matchPredicate(s: Settings, alias = 'm'): string {
  const parts: string[] = [];
  if (!s.allowTitleMatch) parts.push(`${alias}.match_method = 'attendee'`);
  // Name-variant candidates are always emitted by gold and never count unless
  // this is explicitly turned on. Redundant when allowTitleMatch is off, and
  // stated anyway — a reader should not have to derive "variants are excluded"
  // from a different setting's clause.
  if (!s.allowNameVariantMatch) parts.push(`${alias}.match_method <> 'title_variant'`);
  if (s.requireVendorPresent) {
    // Attendance is only knowable for attendee matches; a title match has no
    // attendee row to inspect, so it passes this gate on its own merits.
    parts.push(
      `(${alias}.match_method <> 'attendee'
        OR CAST(${alias}.attendee_attended AS NVARCHAR(10)) IN ('1','true','True'))`,
    );
  }
  if (s.requireMeetingHeld) {
    parts.push(`CAST(${alias}.held AS NVARCHAR(10)) IN ('1','true','True')`);
  }
  return parts.length ? parts.join('\n        AND ') : '1 = 1';
}

/**
 * The meetings that ARE credited to some vendor under the current settings.
 *
 * ⚠️ Use this, never `SELECT DISTINCT meeting_id FROM dbo.vendor_prep_matches`.
 * That table holds CANDIDATES, including `title_variant` rows that are off by
 * default — so the raw form would drop a meeting out of the Review Queue while
 * crediting nobody for it. The meeting would then be invisible: not counted as
 * held, and not shown as needing attention. "Unmatched" has to mean "no match
 * that COUNTS", or the tracker's blind spot grows silently.
 */
function countedMeetingIds(s: Settings): string {
  return `SELECT DISTINCT mm.meeting_id
            FROM dbo.vendor_prep_matches mm
           WHERE ${matchPredicate(s, 'mm')}`;
}

/**
 * The shared CTE block: the resolved vendor checklist for whichever projects the
 * caller's `projectFilter` selects. Emits one row per (project, vendor) with the
 * final status and the evidence behind it.
 */
function vendorStatusCTEs(s: Settings, projectFilter: string): string {
  return `
  proj AS (
      SELECT p.id AS project_id, p.name AS project_name, p.stage,
             p.project_number, p.project_manager,
             ${superintendentNameExpr('p')} AS superintendent_name
      FROM dbo.projects p
      WHERE ${projectFilter}
  ),
  roster AS (
      SELECT r.project_id,
             r.vendor_normalized,
             MAX(r.vendor_name)  AS vendor_name,
             MAX(r.trade_name)   AS trade_name,
             MAX(CASE WHEN CAST(r.from_commitment AS NVARCHAR(10)) IN ('1','true','True')
                      THEN 1 ELSE 0 END) AS from_commitment,
             MAX(CASE WHEN CAST(r.from_directory AS NVARCHAR(10)) IN ('1','true','True')
                      THEN 1 ELSE 0 END) AS from_directory,
             0 AS is_manual
      FROM dbo.vendor_roster r
      JOIN proj ON proj.project_id = r.project_id
      WHERE CAST(r.is_excluded_vendor AS NVARCHAR(10)) NOT IN ('1','true','True')
        AND (${vendorSourcePredicate(s, 'r')})
      GROUP BY r.project_id, r.vendor_normalized

      UNION ALL

      SELECT mr.project_id, mr.vendor_normalized, MAX(mr.vendor_name), MAX(mr.trade_name),
             0, 0, 1
      FROM dbo.vendor_manual_roster mr
      JOIN proj ON proj.project_id = mr.project_id
      GROUP BY mr.project_id, mr.vendor_normalized
  ),
  -- A manually added vendor and a Procore one can collide on the same key; keep
  -- one row and remember it was manual.
  roster_dedup AS (
      SELECT project_id, vendor_normalized,
             MAX(vendor_name)     AS vendor_name,
             MAX(trade_name)      AS trade_name,
             MAX(from_commitment) AS from_commitment,
             MAX(from_directory)  AS from_directory,
             MAX(is_manual)       AS is_manual
      FROM roster
      GROUP BY project_id, vendor_normalized
  ),
  -- Candidate matches, from two places.
  --
  -- gold's dbo.vendor_prep_matches only ever sees vendors that were in PROCORE's
  -- roster, so a vendor an admin adds by hand is invisible to it. Without the
  -- second and third branches below, adding "K&B Electric" to a project whose
  -- prep meeting is literally titled "Preparatory Meeting - K&B Electric" would
  -- leave it reading "not held" forever — the admin action would appear to do
  -- nothing. That is not hypothetical: the first live run produced ten unmatched
  -- meetings naming real subs that Procore's project directory has never heard of.
  --
  -- Every column is CAST explicitly. The mirror pipeline auto-creates these
  -- tables, so a column's type follows whatever the last build inferred, and an
  -- un-cast UNION ALL between an NVARCHAR mirror column and a computed INT is
  -- exactly the kind of implicit-conversion failure that takes the whole
  -- dashboard down.
  eligible_raw AS (
      SELECT
          CAST(m.project_id AS BIGINT)                    AS project_id,
          CAST(m.vendor_normalized AS NVARCHAR(256))      AS vendor_normalized,
          CAST(m.meeting_id AS BIGINT)                    AS meeting_id,
          CAST(m.meeting_title AS NVARCHAR(4000))         AS meeting_title,
          CAST(m.meeting_date AS DATE)                    AS meeting_date,
          CAST(m.held AS NVARCHAR(10))                    AS held,
          CAST(m.match_method AS NVARCHAR(16))            AS match_method,
          CAST(m.attendee_attended AS NVARCHAR(10))       AS attendee_attended,
          CAST(m.matched_attendee_name AS NVARCHAR(256))  AS matched_attendee_name,
          CAST(m.attendance_status AS NVARCHAR(64))       AS attendance_status
      FROM dbo.vendor_prep_matches m
      JOIN proj ON proj.project_id = m.project_id

      UNION ALL

      -- Manual vendor, matched on the attendee list. Both sides are already
      -- normalized keys, so this needs no text processing.
      SELECT
          CAST(mv.project_id AS BIGINT),
          CAST(mv.vendor_normalized AS NVARCHAR(256)),
          CAST(mtg.meeting_id AS BIGINT),
          CAST(mtg.title AS NVARCHAR(4000)),
          CAST(mtg.meeting_date AS DATE),
          CAST(mtg.held AS NVARCHAR(10)),
          CAST('attendee' AS NVARCHAR(16)),
          CAST(MAX(CASE WHEN CAST(a.attended AS NVARCHAR(10)) IN ('1','true','True')
                        THEN 1 ELSE 0 END) AS NVARCHAR(10)),
          CAST(MAX(a.attendee_name) AS NVARCHAR(256)),
          CAST(MAX(a.attendance_status) AS NVARCHAR(64))
      FROM dbo.vendor_manual_roster mv
      JOIN proj ON proj.project_id = mv.project_id
      JOIN dbo.vendor_prep_meetings mtg ON mtg.project_id = mv.project_id
      JOIN dbo.vendor_prep_attendees a
             ON a.meeting_id = mtg.meeting_id
            AND a.company_normalized = mv.vendor_normalized
      WHERE CAST(a.is_gc AS NVARCHAR(10)) NOT IN ('1','true','True')
      GROUP BY mv.project_id, mv.vendor_normalized, mtg.meeting_id, mtg.title,
               mtg.meeting_date, mtg.held
      ${manualTitleMatchBranch()}
  ),
  eligible AS (
      SELECT * FROM eligible_raw m
      WHERE ${matchPredicate(s, 'm')}
  ),
  -- Best evidence per vendor: an attendee match outranks a title match, then the
  -- EARLIEST meeting wins — a preparatory meeting is held before the vendor
  -- starts work, so the first one is the one that satisfies the requirement.
  best AS (
      SELECT e.project_id, e.vendor_normalized, e.meeting_id, e.meeting_title,
             e.meeting_date, e.match_method, e.matched_attendee_name, e.attendance_status,
             ROW_NUMBER() OVER (
               PARTITION BY e.project_id, e.vendor_normalized
               -- Strongest evidence wins the row the UI shows: a direct
               -- attendee record, then an exact title, then a name variant.
               ORDER BY CASE e.match_method
                          WHEN 'attendee'      THEN 0
                          WHEN 'title'         THEN 1
                          WHEN 'title_variant' THEN 2
                          ELSE 3 END,
                        e.meeting_date ASC, e.meeting_id ASC) AS rn
      FROM eligible e
  ),
  evidence AS (
      SELECT project_id, vendor_normalized,
             COUNT(DISTINCT meeting_id) AS meeting_count,
             MAX(CASE WHEN match_method = 'attendee' THEN 1 ELSE 0 END) AS has_attendee_match,
             MAX(CASE WHEN match_method = 'title'    THEN 1 ELSE 0 END) AS has_title_match,
             MAX(CASE WHEN match_method = 'title_variant' THEN 1 ELSE 0 END) AS has_variant_match
      FROM eligible
      GROUP BY project_id, vendor_normalized
  ),
  resolved AS (
      SELECT
          rd.project_id,
          rd.vendor_normalized,
          rd.vendor_name,
          rd.trade_name,
          rd.from_commitment,
          rd.from_directory,
          rd.is_manual,
          b.meeting_id,
          b.meeting_title,
          b.meeting_date,
          b.match_method,
          b.matched_attendee_name,
          b.attendance_status,
          COALESCE(ev.meeting_count, 0)      AS meeting_count,
          COALESCE(ev.has_attendee_match, 0) AS has_attendee_match,
          COALESCE(ev.has_title_match, 0)    AS has_title_match,
          COALESCE(ev.has_variant_match, 0)  AS has_variant_match,
          o.status                           AS override_status,
          o.note                             AS override_note,
          o.meeting_date                     AS override_meeting_date,
          o.created_by                       AS override_by,
          -- The human's answer wins over the matcher's, always.
          CASE
            WHEN o.status IN ('held','not_held','not_applicable') THEN o.status
            WHEN b.meeting_id IS NOT NULL THEN 'held'
            ELSE 'not_held'
          END AS status
      FROM roster_dedup rd
      LEFT JOIN best b
             ON b.project_id = rd.project_id
            AND b.vendor_normalized = rd.vendor_normalized
            AND b.rn = 1
      LEFT JOIN evidence ev
             ON ev.project_id = rd.project_id
            AND ev.vendor_normalized = rd.vendor_normalized
      LEFT JOIN dbo.vendor_prep_overrides o
             ON o.project_id = rd.project_id
            AND o.vendor_normalized = rd.vendor_normalized
  )`;
}

export type ProjectSummary = {
  project_id: number;
  project_name: string;
  project_number: string | null;
  stage: string | null;
  project_manager: string | null;
  superintendent_name: string | null;
  vendor_total: number;
  vendor_held: number;
  vendor_outstanding: number;
  vendor_not_applicable: number;
  pct_complete: number | null;
  last_meeting_date: string | null;
  prep_meeting_count: number;
  unmatched_meeting_count: number;
};

/** One row per active project — the tracker's landing view. */
export async function getProjectSummaries(scope: 'active' | 'all'): Promise<ProjectSummary[]> {
  await ensureProjectColumnMeta();
  await ensureAdminTables();
  const s = await getSettings();
  const projectFilter = scope === 'all' ? '1 = 1' : activeStageFilter('p');

  const { rows } = await db.query<ProjectSummary>(`
    WITH ${vendorStatusCTEs(s, projectFilter)},
    agg AS (
        SELECT project_id,
               SUM(CASE WHEN status <> 'not_applicable' THEN 1 ELSE 0 END) AS vendor_total,
               SUM(CASE WHEN status = 'held' THEN 1 ELSE 0 END)            AS vendor_held,
               SUM(CASE WHEN status = 'not_held' THEN 1 ELSE 0 END)        AS vendor_outstanding,
               SUM(CASE WHEN status = 'not_applicable' THEN 1 ELSE 0 END)  AS vendor_not_applicable,
               MAX(meeting_date)                                           AS last_meeting_date
        FROM resolved
        GROUP BY project_id
    ),
    mtg AS (
        SELECT m.project_id,
               COUNT(*) AS prep_meeting_count,
               SUM(CASE WHEN x.meeting_id IS NULL THEN 1 ELSE 0 END) AS unmatched_meeting_count
        FROM dbo.vendor_prep_meetings m
        LEFT JOIN (${countedMeetingIds(s)}) x
               ON x.meeting_id = m.meeting_id
        GROUP BY m.project_id
    )
    SELECT
        proj.project_id,
        proj.project_name,
        proj.project_number,
        proj.stage,
        proj.project_manager,
        proj.superintendent_name,
        COALESCE(agg.vendor_total, 0)          AS vendor_total,
        COALESCE(agg.vendor_held, 0)           AS vendor_held,
        COALESCE(agg.vendor_outstanding, 0)    AS vendor_outstanding,
        COALESCE(agg.vendor_not_applicable, 0) AS vendor_not_applicable,
        CASE WHEN COALESCE(agg.vendor_total, 0) = 0 THEN NULL
             ELSE ROUND(100.0 * agg.vendor_held / agg.vendor_total, 0) END AS pct_complete,
        CONVERT(VARCHAR(10), agg.last_meeting_date, 23) AS last_meeting_date,
        COALESCE(mtg.prep_meeting_count, 0)      AS prep_meeting_count,
        COALESCE(mtg.unmatched_meeting_count, 0) AS unmatched_meeting_count
    FROM proj
    LEFT JOIN agg ON agg.project_id = proj.project_id
    LEFT JOIN mtg ON mtg.project_id = proj.project_id
    ORDER BY proj.project_name;
  `);
  return rows;
}

export type VendorRow = {
  project_id: number;
  vendor_normalized: string;
  vendor_name: string;
  trade_name: string | null;
  from_commitment: number;
  from_directory: number;
  is_manual: number;
  status: string;
  meeting_id: number | null;
  meeting_title: string | null;
  meeting_date: string | null;
  match_method: string | null;
  matched_attendee_name: string | null;
  attendance_status: string | null;
  meeting_count: number;
  has_attendee_match: number;
  has_title_match: number;
  has_variant_match: number;
  override_status: string | null;
  override_note: string | null;
  override_by: string | null;
};

/** The full drilldown for one project: checklist + meetings + review queue. */
export async function getProjectDetail(projectId: number): Promise<{
  project: Record<string, unknown> | null;
  vendors: VendorRow[];
  meetings: Record<string, unknown>[];
  unmatched: Record<string, unknown>[];
}> {
  await ensureProjectColumnMeta();
  await ensureAdminTables();
  const s = await getSettings();

  const { rows: vendors } = await db.query<VendorRow>(
    `
    WITH ${vendorStatusCTEs(s, 'p.id = @pid')}
    SELECT
        project_id, vendor_normalized, vendor_name, trade_name,
        from_commitment, from_directory, is_manual,
        status, meeting_id, meeting_title,
        CONVERT(VARCHAR(10), COALESCE(override_meeting_date, meeting_date), 23) AS meeting_date,
        match_method, matched_attendee_name, attendance_status,
        meeting_count, has_attendee_match, has_title_match, has_variant_match,
        override_status, override_note, override_by
    FROM resolved
    ORDER BY CASE status WHEN 'not_held' THEN 0 WHEN 'held' THEN 1 ELSE 2 END, vendor_name;
  `,
    { pid: projectId },
  );

  const { rows: project } = await db.query(
    `SELECT TOP 1 p.id AS project_id, p.name AS project_name, p.project_number,
            p.stage, p.project_manager,
            ${superintendentNameExpr('p')} AS superintendent_name
     FROM dbo.projects p WHERE p.id = @pid`,
    { pid: projectId },
  );

  const { rows: meetings } = await db.query(
    `SELECT m.meeting_id, m.title, CONVERT(VARCHAR(10), m.meeting_date, 23) AS meeting_date,
            m.held, m.attendee_count, m.vendor_attendee_count, m.vendor_attendees_present,
            m.series_name, m.location,
            (SELECT COUNT(DISTINCT x.vendor_normalized) FROM dbo.vendor_prep_matches x
              WHERE x.meeting_id = m.meeting_id
                AND ${matchPredicate(s, 'x')}) AS matched_vendor_count,
            (SELECT TOP 1 x.vendor_name FROM dbo.vendor_prep_matches x
              WHERE x.meeting_id = m.meeting_id
                AND x.match_method = 'title_variant'
              ORDER BY x.vendor_name) AS suggested_vendor
     FROM dbo.vendor_prep_meetings m
     WHERE m.project_id = @pid
     ORDER BY m.meeting_date DESC`,
    { pid: projectId },
  );

  const unmatched = (meetings as Record<string, unknown>[]).filter(
    (m) => Number(m.matched_vendor_count ?? 0) === 0,
  );

  return { project: project[0] ?? null, vendors, meetings, unmatched };
}

/**
 * Prep meetings the matcher could not credit to any vendor. This is the
 * tracker's honest blind spot — a meeting was held and logged, but nothing ties
 * it to a company on the roster (usually a meeting titled with a person's name,
 * or a sub who never made it onto either Procore roster). Surfacing them beats
 * quietly dropping them.
 */
export async function getUnmatchedMeetings(scope: 'active' | 'all'): Promise<Record<string, unknown>[]> {
  await ensureProjectColumnMeta();
  const s = await getSettings();
  const projectFilter = scope === 'all' ? '1 = 1' : activeStageFilter('p');
  // `suggested_vendor` is a name-variant candidate gold found but the settings
  // do not credit — the title names a company that IS on this project's roster
  // under a longer legal name, and exactly one vendor fits. Showing it turns a
  // row that says "we can't credit this" into one an admin can act on in a
  // click, without the tracker having decided anything on its own.
  const { rows } = await db.query(`
    SELECT m.project_id, p.name AS project_name, m.meeting_id, m.title,
           CONVERT(VARCHAR(10), m.meeting_date, 23) AS meeting_date,
           m.attendee_count, m.vendor_attendee_count,
           sug.vendor_name       AS suggested_vendor,
           sug.vendor_normalized AS suggested_vendor_normalized
    FROM dbo.vendor_prep_meetings m
    JOIN dbo.projects p ON p.id = m.project_id
    LEFT JOIN (${countedMeetingIds(s)}) x
           ON x.meeting_id = m.meeting_id
    OUTER APPLY (
        SELECT TOP 1 v.vendor_name, v.vendor_normalized
        FROM dbo.vendor_prep_matches v
        WHERE v.meeting_id = m.meeting_id
          AND v.match_method = 'title_variant'
        ORDER BY v.vendor_name
    ) sug
    WHERE x.meeting_id IS NULL
      AND ${projectFilter}
    ORDER BY m.meeting_date DESC;
  `);
  return rows;
}

/* ============================================================================
 * WRITES
 * ==========================================================================*/

export async function saveOverride(
  projectId: number,
  vendorNormalized: string,
  status: 'held' | 'not_held' | 'not_applicable',
  opts: { note?: string | null; meetingDate?: string | null; actor: string },
): Promise<void> {
  await ensureAdminTables();
  await db.query(
    `MERGE dbo.vendor_prep_overrides AS t
     USING (SELECT @pid AS project_id, @vn AS vendor_normalized) AS s
        ON t.project_id = s.project_id AND t.vendor_normalized = s.vendor_normalized
     WHEN MATCHED THEN UPDATE SET status = @status, note = @note,
          meeting_date = TRY_CONVERT(DATE, @mdate), created_by = @actor, created_at = SYSUTCDATETIME()
     WHEN NOT MATCHED THEN
          INSERT (project_id, vendor_normalized, status, note, meeting_date, created_by)
          VALUES (@pid, @vn, @status, @note, TRY_CONVERT(DATE, @mdate), @actor);`,
    {
      pid: projectId,
      vn: vendorNormalized,
      status,
      note: opts.note ?? null,
      mdate: opts.meetingDate ?? null,
      actor: opts.actor,
    },
  );
}

export async function clearOverride(projectId: number, vendorNormalized: string): Promise<void> {
  await ensureAdminTables();
  await db.query(
    `DELETE FROM dbo.vendor_prep_overrides
      WHERE project_id = @pid AND vendor_normalized = @vn`,
    { pid: projectId, vn: vendorNormalized },
  );
}

export async function listOverrides(): Promise<Record<string, unknown>[]> {
  await ensureAdminTables();
  const { rows } = await db.query(`
    SELECT o.id, o.project_id, p.name AS project_name, o.vendor_normalized,
           o.status, o.note, CONVERT(VARCHAR(10), o.meeting_date, 23) AS meeting_date,
           o.created_by, o.created_at
    FROM dbo.vendor_prep_overrides o
    LEFT JOIN dbo.projects p ON p.id = o.project_id
    ORDER BY o.created_at DESC;`);
  return rows;
}

export async function addManualVendor(
  projectId: number,
  vendorName: string,
  vendorNormalized: string,
  opts: { trade?: string | null; note?: string | null; actor: string },
): Promise<void> {
  await ensureAdminTables();
  await db.query(
    `MERGE dbo.vendor_manual_roster AS t
     USING (SELECT @pid AS project_id, @vn AS vendor_normalized) AS s
        ON t.project_id = s.project_id AND t.vendor_normalized = s.vendor_normalized
     WHEN MATCHED THEN UPDATE SET vendor_name = @name, trade_name = @trade, note = @note
     WHEN NOT MATCHED THEN
          INSERT (project_id, vendor_normalized, vendor_name, trade_name, note, created_by)
          VALUES (@pid, @vn, @name, @trade, @note, @actor);`,
    {
      pid: projectId,
      vn: vendorNormalized,
      name: vendorName,
      trade: opts.trade ?? null,
      note: opts.note ?? null,
      actor: opts.actor,
    },
  );
}

export async function removeManualVendor(
  projectId: number,
  vendorNormalized: string,
): Promise<void> {
  await ensureAdminTables();
  await db.query(
    `DELETE FROM dbo.vendor_manual_roster
      WHERE project_id = @pid AND vendor_normalized = @vn`,
    { pid: projectId, vn: vendorNormalized },
  );
}

/* ============================================================================
 * MISC
 * ==========================================================================*/

/* ============================================================================
 * METRICS
 *
 * ⚠️ One thing is deliberately NOT computed here: **coverage percentage over
 * time.** The vendor roster is a CURRENT snapshot mirrored from Procore — there
 * is no history of who was on a project's roster in March — so a chart claiming
 * "42% of vendors had their prep meeting in March" would be inventing its own
 * denominator. Coverage is reported as a present-day figure only; what IS
 * honestly derivable month over month is what actually happened: meetings held,
 * projects participating, vendors credited, and how well the meetings were
 * recorded. The dashboard says as much next to the charts.
 * ==========================================================================*/

export async function getMetrics(months: number): Promise<Record<string, unknown>> {
  await ensureProjectColumnMeta();
  await ensureAdminTables();
  const s = await getSettings();
  const win = Math.max(1, Math.min(60, Math.floor(months)));

  // ── Current adoption + coverage snapshot ────────────────────────────────
  const { rows: snap } = await db.query(`
    WITH ${vendorStatusCTEs(s, activeStageFilter('p'))},
    per_project AS (
        SELECT project_id,
               SUM(CASE WHEN status <> 'not_applicable' THEN 1 ELSE 0 END) AS tracked,
               SUM(CASE WHEN status = 'held' THEN 1 ELSE 0 END)            AS held
        FROM resolved GROUP BY project_id
    )
    SELECT
        (SELECT COUNT(*) FROM proj)                                    AS active_projects,
        (SELECT COUNT(DISTINCT m.project_id) FROM dbo.vendor_prep_meetings m
          JOIN proj ON proj.project_id = m.project_id)                 AS projects_with_meetings,
        COALESCE(SUM(pp.tracked), 0)                                   AS vendors_tracked,
        COALESCE(SUM(pp.held), 0)                                      AS vendors_held,
        (SELECT COUNT(*) FROM dbo.vendor_prep_meetings m
          JOIN proj ON proj.project_id = m.project_id)                 AS total_meetings,
        (SELECT COUNT(*) FROM dbo.vendor_prep_meetings m
          JOIN proj ON proj.project_id = m.project_id
          LEFT JOIN (${countedMeetingIds(s)}) x
                 ON x.meeting_id = m.meeting_id
         WHERE x.meeting_id IS NULL)                                   AS unmatched_meetings
    FROM per_project pp;`);

  // ── Month-by-month, from what actually happened ─────────────────────────
  // Each meeting is classified by its BEST evidence: an attendee match beats a
  // title-only match beats nothing. Counting a meeting once, at its strongest
  // signal, keeps the three series a partition of the total rather than
  // overlapping sets that sum to more than the meetings held.
  const { rows: monthly } = await db.query(
    `
    WITH mtg AS (
        SELECT m.meeting_id,
               m.project_id,
               CONVERT(CHAR(7), m.meeting_date, 23) AS ym,
               MAX(CASE WHEN x.match_method = 'attendee' THEN 1 ELSE 0 END) AS has_attendee,
               MAX(CASE WHEN x.match_method IN ('title','title_variant')
                        THEN 1 ELSE 0 END)                             AS has_title
        FROM dbo.vendor_prep_meetings m
        -- Only matches that COUNT feed the chart; otherwise a meeting credited
        -- to nobody would still be drawn in the "matched by title" band.
        LEFT JOIN dbo.vendor_prep_matches x
               ON x.meeting_id = m.meeting_id
              AND ${matchPredicate(s, 'x')}
        WHERE m.meeting_date IS NOT NULL
          AND m.meeting_date >= DATEADD(MONTH, -@win, CAST(GETUTCDATE() AS DATE))
        GROUP BY m.meeting_id, m.project_id, CONVERT(CHAR(7), m.meeting_date, 23)
    ),
    att AS (
        SELECT CONVERT(CHAR(7), m.meeting_date, 23) AS ym,
               COUNT(*) AS attendees,
               SUM(CASE WHEN a.attendance_status IN ('present','conference','absent','distribution')
                        THEN 1 ELSE 0 END) AS attendees_with_status,
               SUM(CASE WHEN CAST(a.is_gc AS NVARCHAR(10)) NOT IN ('1','true','True')
                        THEN 1 ELSE 0 END) AS vendor_attendees
        FROM dbo.vendor_prep_attendees a
        JOIN dbo.vendor_prep_meetings m ON m.meeting_id = a.meeting_id
        WHERE m.meeting_date IS NOT NULL
          AND m.meeting_date >= DATEADD(MONTH, -@win, CAST(GETUTCDATE() AS DATE))
        GROUP BY CONVERT(CHAR(7), m.meeting_date, 23)
    ),
    ven AS (
        SELECT CONVERT(CHAR(7), x.meeting_date, 23) AS ym,
               COUNT(DISTINCT CONCAT(CAST(x.project_id AS NVARCHAR(20)), '|', x.vendor_normalized))
                 AS vendors_credited
        FROM dbo.vendor_prep_matches x
        WHERE x.meeting_date IS NOT NULL
          AND x.meeting_date >= DATEADD(MONTH, -@win, CAST(GETUTCDATE() AS DATE))
          AND ${matchPredicate(s, 'x')}
        GROUP BY CONVERT(CHAR(7), x.meeting_date, 23)
    )
    SELECT
        mtg.ym                                                        AS month,
        COUNT(*)                                                      AS meetings,
        COUNT(DISTINCT mtg.project_id)                                AS projects,
        SUM(CASE WHEN mtg.has_attendee = 1 THEN 1 ELSE 0 END)         AS attendee_matched,
        SUM(CASE WHEN mtg.has_attendee = 0 AND mtg.has_title = 1 THEN 1 ELSE 0 END) AS title_only,
        SUM(CASE WHEN mtg.has_attendee = 0 AND mtg.has_title = 0 THEN 1 ELSE 0 END) AS unmatched,
        MAX(COALESCE(att.attendees, 0))             AS attendees,
        MAX(COALESCE(att.attendees_with_status, 0)) AS attendees_with_status,
        MAX(COALESCE(att.vendor_attendees, 0))      AS vendor_attendees,
        MAX(COALESCE(ven.vendors_credited, 0))      AS vendors_credited
    FROM mtg
    LEFT JOIN att ON att.ym = mtg.ym
    LEFT JOIN ven ON ven.ym = mtg.ym
    GROUP BY mtg.ym
    ORDER BY mtg.ym;`,
    { win },
  );

  // ── Per-project leaderboard (current coverage) ──────────────────────────
  const { rows: leaderboard } = await db.query(`
    WITH ${vendorStatusCTEs(s, activeStageFilter('p'))}
    SELECT
        proj.project_id, proj.project_name, proj.superintendent_name, proj.project_manager,
        SUM(CASE WHEN r.status <> 'not_applicable' THEN 1 ELSE 0 END) AS tracked,
        SUM(CASE WHEN r.status = 'held' THEN 1 ELSE 0 END)            AS held,
        (SELECT COUNT(*) FROM dbo.vendor_prep_meetings m
          WHERE m.project_id = proj.project_id)                       AS meetings
    FROM proj
    LEFT JOIN resolved r ON r.project_id = proj.project_id
    GROUP BY proj.project_id, proj.project_name, proj.superintendent_name, proj.project_manager
    ORDER BY proj.project_name;`);

  // ── Vendors appearing in the most prep meetings ─────────────────────────
  const { rows: topVendors } = await db.query(`
    SELECT TOP 15
           x.vendor_name,
           COUNT(DISTINCT x.meeting_id) AS meetings,
           COUNT(DISTINCT x.project_id) AS projects,
           MAX(CASE WHEN x.match_method = 'attendee' THEN 1 ELSE 0 END) AS ever_on_attendee_list
    FROM dbo.vendor_prep_matches x
    WHERE COALESCE(x.vendor_name,'') <> ''
      AND ${matchPredicate(s, 'x')}
    GROUP BY x.vendor_name
    ORDER BY COUNT(DISTINCT x.meeting_id) DESC, x.vendor_name;`);

  return { snapshot: snap[0] ?? {}, monthly, leaderboard, topVendors, months: win };
}

export async function getSyncStatus(): Promise<Record<string, unknown>> {
  const { rows } = await db.query(`
    SELECT
      (SELECT MAX(_fabric_loaded_at) FROM dbo.vendor_prep_meetings) AS meetings_loaded_at,
      (SELECT MAX(_fabric_loaded_at) FROM dbo.vendor_roster)        AS roster_loaded_at,
      (SELECT COUNT(*) FROM dbo.vendor_prep_meetings)               AS prep_meeting_count,
      (SELECT COUNT(*) FROM dbo.vendor_roster)                      AS roster_row_count;`);
  return rows[0] ?? {};
}

/**
 * Coverage comparison between the two Procore rosters, for the settings screen.
 * This is the in-app version of the ingest's COVERAGE DIAGNOSTIC and is what
 * tells an admin which `vendorSource` to pick.
 */
export async function getRosterCoverage(): Promise<Record<string, unknown>> {
  await ensureProjectColumnMeta();
  const { rows } = await db.query(`
    SELECT
      COUNT(DISTINCT CASE WHEN CAST(r.from_commitment AS NVARCHAR(10)) IN ('1','true','True')
                          THEN r.project_id END) AS projects_with_commitments,
      COUNT(DISTINCT CASE WHEN CAST(r.from_directory AS NVARCHAR(10)) IN ('1','true','True')
                          THEN r.project_id END) AS projects_with_directory,
      SUM(CASE WHEN CAST(r.from_commitment AS NVARCHAR(10)) IN ('1','true','True')
               THEN 1 ELSE 0 END) AS vendor_rows_commitment,
      SUM(CASE WHEN CAST(r.from_directory AS NVARCHAR(10)) IN ('1','true','True')
               THEN 1 ELSE 0 END) AS vendor_rows_directory,
      COUNT(DISTINCT r.project_id) AS projects_with_any,
      COUNT(*)                     AS vendor_rows_any
    FROM dbo.vendor_roster r
    JOIN dbo.projects p ON p.id = r.project_id
    WHERE ${activeStageFilter('p')};`);
  return rows[0] ?? {};
}
