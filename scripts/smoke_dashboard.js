#!/usr/bin/env node
/**
 * Dashboard smoke test — LOAD the page, don't just parse it.
 *
 * Why this exists. `node --check` on the extracted <script> was the only gate,
 * and it passed a file that took the whole dashboard down on load: a markdown
 * `**` around a URL path inside a block comment put a comment terminator INSIDE
 * the comment, ending it early and leaving a bare `details` as a live statement.
 * A bare identifier is valid syntax, so the parser was happy; the browser threw
 * "ReferenceError: details is not defined" before a single handler was bound,
 * and nothing on the page was clickable.
 *
 * A syntax check cannot catch that, and neither can review — the file looked
 * fine. Executing it catches it in about a second. It also caught a second
 * shipped bug on the first run: loadMetrics() called api('/api/metrics'), and
 * api() prepends /api, so the Metrics tab was requesting /api/api/metrics.
 *
 * Serves dashboard/ with a stub API returning fixture-shaped payloads, drives
 * every tab in headless Chromium, and fails on any uncaught error, console
 * error, or failed same-origin request.
 *
 *   node scripts/smoke_dashboard.js
 *
 * Needs playwright-core and the preinstalled Chromium; skips (exit 0, loudly)
 * when neither is present, so it can't block someone without them.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');

const ROOT = path.join(__dirname, '..', 'dashboard');
const PORT = 8731;

// Payloads shaped like the real API's meta() envelope: { data, meta }.
const NOW = new Date().toISOString();
const STUB = {
  '/api/me': { email: 'cory.zilisch@buffaloconstruction.com', is_admin: true,
               admin_mode: 'allowlist', emails_seen: ['cory.zilisch@buffaloconstruction.com'] },
  '/api/health': { status: 'ok' },
  '/api/sync-status': { last_loaded: NOW },
  '/api/tracker': [
    { project_id: 3176472, project_name: 'Hunting Creek GC Snack Shack', project_number: '24-101',
      superintendent: 'Ken Houston', vendor_total: 12, vendor_held: 9, vendor_outstanding: 3,
      prep_meeting_count: 4, pct_complete: 75, last_meeting_date: '2026-03-17',
      unmatched_meeting_count: 2, awaiting_minutes_count: 3,
      // EXPLICIT null, not a missing key. This is what the API returns before
      // dbo.vendor_project_scope is mirrored, and the difference is the whole
      // bug: Number(null) is 0 while Number(undefined) is NaN, so a stub that
      // merely omitted the field could not reproduce production and passed a
      // dashboard that labelled every project "Not ingested".
      ingested: null },
    { project_id: 3387062, project_name: 'AEP Eagle Pass Service Center', project_number: '25-004',
      superintendent: null, vendor_total: 20, vendor_held: 0, vendor_outstanding: 20,
      prep_meeting_count: 5, pct_complete: 0, last_meeting_date: null,
      unmatched_meeting_count: 4, ingested: 1, awaiting_minutes_count: 0 },
    { project_id: 3119932, project_name: 'An Old 2024 Job', project_number: '24-007',
      superintendent: null, vendor_total: 0, vendor_held: 0, vendor_outstanding: 0,
      prep_meeting_count: 0, pct_complete: null, last_meeting_date: null,
      unmatched_meeting_count: 0, ingested: 0, awaiting_minutes_count: null },
  ],
  // ⚠️ This endpoint's envelope is NOT {data:{...}} — openProject() reads the
  // vendor ARRAY from `data` and everything else from `meta`. A stub that
  // invented the shape made `vendors.forEach` throw inside openProject's own
  // try/catch, so the modal rendered an error, nothing logged, and every
  // drilldown assertion was skipped. Third time an invented stub shape has
  // hidden a real assertion — match the API, always.
  '/api/projects/3176472': {
    __data: [
      { vendor_normalized: 'zip electric', vendor_name: 'ZIP Electric LLC', status: 'held',
        match_method: 'attendee', meeting_id: 12457210, meeting_date: '2026-03-17',
        attendee_attended: true, trade_name: 'Electrical', meeting_state: 'minutes',
        meeting_state_source: 'inferred', meeting_attendee_count: 6,
        meeting_vendor_attendee_count: 3 },
      { vendor_normalized: 'makk concrete', vendor_name: 'MAKK Concrete', status: 'held',
        match_method: 'title', meeting_id: 12406175, meeting_date: '2026-03-05',
        attendee_attended: null, trade_name: 'Concrete', meeting_state: 'minutes',
        meeting_state_source: 'inferred', meeting_attendee_count: 4,
        meeting_vendor_attendee_count: 0 },
      // Negative control: also credited from the title, but the meeting DOES
      // record a vendor-side attendee — so the flag must stay off. Without a
      // negative control the assertion only proves the chip can render.
      { vendor_normalized: 'hive energy', vendor_name: 'Hive Energy Solutions LLC', status: 'held',
        match_method: 'title_variant', meeting_id: 12719705, meeting_date: '2026-04-29',
        attendee_attended: null, trade_name: null, meeting_state: 'minutes',
        meeting_state_source: 'inferred', meeting_attendee_count: 5,
        meeting_vendor_attendee_count: 2 },
      { vendor_normalized: 'escar construction', vendor_name: 'Escar Construction',
        status: 'not_held', match_method: null, meeting_id: null, meeting_date: null,
        attendee_attended: null, trade_name: null },
    ],
    __meta: {
      project: { project_id: 3176472, project_name: 'Hunting Creek GC Snack Shack', pct: 75 },
      summary: { vendor_total: 4, vendor_held: 3, vendor_outstanding: 1 },
      meetings: [
        { meeting_id: 12457210, title: 'Preparatory Meeting Agenda - ZIP',
          meeting_date: '2026-03-17', attendee_count: 6, vendor_attendee_count: 3,
          matched_vendor_count: 1, meeting_state: 'agenda',
          meeting_state_source: 'inferred' },
      ],
      unmatched: [
        { meeting_id: 12406175, title: 'Preparatory Meeting Agenda- H&W LandWorks',
          meeting_date: '2026-03-05', vendor_attendee_count: 0 },
      ],
    },
  },
  '/api/unmatched-meetings': [
    { project_id: 3176472, project_name: 'Hunting Creek GC Snack Shack', meeting_id: 12406175,
      title: 'Preparatory Meeting Agenda- H&W LandWorks', meeting_date: '2026-03-05',
      vendor_attendee_count: 0, attendee_count: 4, meeting_state: 'minutes',
      review_reason: 'no_vendor',
      suggested_vendor: 'H&W Landwork KY LLC', suggested_vendor_normalized: 'h and w landwork ky' },
    { project_id: 3387062, project_name: 'AEP Eagle Pass Service Center', meeting_id: 12719705,
      title: 'Pre-Contract Meeting Agenda - HIVE', meeting_date: '2026-04-29',
      vendor_attendee_count: 3, attendee_count: 5, meeting_state: 'agenda',
      review_reason: 'needs_minutes', meeting_state_source: 'inferred',
      suggested_vendor: null, suggested_vendor_normalized: null },
  ],
  '/api/settings': {
    settings: { vendorSource: 'either', allowTitleMatch: 1, requireVendorPresent: 0,
                requireMeetingHeld: 0, allowNameVariantMatch: 1, adminMode: 'allowlist' },
    coverage: { commitment_projects: 59, directory_projects: 91,
                commitment_vendors: 1390, directory_vendors: 2362 },
  },
  '/api/overrides': [],
  '/api/admin-users': [{ email: 'cory.zilisch@buffaloconstruction.com', added_by: 'bootstrap' }],
  '/api/people': [
    { name: 'Justin Houston', email: 'justin.houston@buffaloconstruction.com',
      job_title: 'Safety Director', department: 'Safety' },
    { name: 'No Email Person', email: '', job_title: 'Superintendent', department: null },
  ],
  '/api/metrics': {
    snapshot: { active_projects: 88, projects_with_meetings: 10, vendors_tracked: 2362,
                vendors_held: 59, total_meetings: 63, unmatched_meetings: 10 },
    monthly: [
      { month: '2026-03', meetings: 18, projects: 1, attendee_matched: 16, title_only: 2,
        unmatched: 2, attendees: 40, attendees_with_status: 30, vendor_attendees: 22,
        vendors_credited: 14 },
      { month: '2026-04', meetings: 10, projects: 1, attendee_matched: 6, title_only: 0,
        unmatched: 4, attendees: 12, attendees_with_status: 6, vendor_attendees: 8,
        vendors_credited: 6 },
    ],
    // Field names must match what getMetrics() actually returns — an invented
    // stub shape makes the smoke test assert against a page the real API would
    // never produce. These came from the query's own SELECT list.
    leaderboard: [
      { project_id: 3176472, project_name: 'Hunting Creek GC Snack Shack',
        superintendent_name: 'A Super', project_manager: 'A PM',
        tracked: 12, held: 9, meetings: 18 },
      { project_id: 3387062, project_name: 'AEP Eagle Pass Service Center',
        superintendent_name: null, project_manager: null,
        tracked: 20, held: 0, meetings: 10 },
    ],
    topVendors: [
      { vendor_name: 'ZIP Electric LLC', meetings: 6, projects: 3, ever_on_attendee_list: 1 },
      { vendor_name: 'MAKK Concrete', meetings: 2, projects: 1, ever_on_attendee_list: 0 },
    ],
    months: 12,
  },
};

function stubFor(url) {
  const clean = url.split('?')[0];
  if (STUB[clean] !== undefined) return STUB[clean];
  if (/^\/api\/projects\/\d+$/.test(clean)) return STUB['/api/projects/3176472'];
  return null;
}

const server = http.createServer((req, res) => {
  const url = req.url || '/';
  if (url.startsWith('/api/')) {
    const data = stubFor(url);
    if (data === null) { res.writeHead(404).end('{"error":"no stub"}'); return; }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    const body = (data && data.__data !== undefined)
      ? { data: data.__data, meta: { generated_at: NOW, cached: false, ...(data.__meta || {}) } }
      : { data, meta: { generated_at: NOW, cached: false } };
    res.end(JSON.stringify(body));
    return;
  }
  const file = url === '/' ? '/index.html' : url.split('?')[0];
  const full = path.join(ROOT, file);
  if (!full.startsWith(ROOT) || !fs.existsSync(full)) { res.writeHead(404).end('not found'); return; }
  res.writeHead(200, { 'Content-Type': file.endsWith('.html') ? 'text/html' : 'text/plain' });
  res.end(fs.readFileSync(full));
});

(async () => {
  let chromium;
  try { ({ chromium } = require('playwright-core')); }
  catch { console.log('SKIP: playwright-core not installed'); process.exit(0); }

  const exe = ['/opt/pw-browsers/chromium/chrome-linux/chrome',
               '/opt/pw-browsers/chromium-1194/chrome-linux/chrome']
    .find((p) => fs.existsSync(p));
  if (!exe) { console.log('SKIP: no Chromium at /opt/pw-browsers'); process.exit(0); }

  await new Promise((r) => server.listen(PORT, r));
  const browser = await chromium.launch({ executablePath: exe, args: ['--no-sandbox'] });
  const page = await browser.newPage();

  const problems = [];
  // Only OUR code and OUR requests. Google Fonts is blocked in some sandboxes and
  // is a progressive enhancement — failing the smoke test on it would train
  // people to ignore the test, which is worse than not having one.
  const ours = (u) => !u || u.startsWith(`http://127.0.0.1:${PORT}`);
  page.on('pageerror', (e) => problems.push(`uncaught: ${e.message}`));
  page.on('console', (m) => {
    if (m.type() !== 'error') return;
    const at = (m.location() || {}).url || '';
    if (!ours(at)) return;
    problems.push(`console.error: ${m.text()}`);
  });
  page.on('requestfailed', (r) => { if (ours(r.url())) problems.push(`request failed: ${r.url()}`); });
  page.on('response', (r) => {
    if (r.status() >= 400 && ours(r.url())) problems.push(`HTTP ${r.status()}: ${r.url()}`);
  });

  await page.goto(`http://127.0.0.1:${PORT}/`, { waitUntil: 'networkidle' });

  // Every tab, because a render function only runs when its tab is opened —
  // which is exactly why the Metrics tab could ship requesting /api/api/metrics.
  // The Settings tab starts display:none until /api/me says admin, so click by
  // data-tab (all four exist in the DOM from the start) rather than by id.
  const tabs = await page.$$eval('nav.tabs button[data-tab]',
    (els) => els.map((e) => e.getAttribute('data-tab')));
  if (tabs.length < 4) problems.push(`only found ${tabs.length} tab(s) — selector is stale`);
  for (const name of tabs) {
    // Dispatch a real click so the delegated nav handler is exercised too. Done
    // via evaluate because the Settings tab is display:none until /api/me says
    // admin, and Playwright's click() waits for visibility.
    await page.evaluate((n) => {
      const b = document.querySelector(`nav.tabs button[data-tab="${n}"]`);
      if (b) b.click();
    }, name);
    await page.waitForTimeout(400);
  }
  // Spot-check that the tabs actually RENDERED, not merely failed to throw. A
  // page that silently draws nothing passes an error-only check.
  const expectations = [
    ['#review-out', 'H&W Landwork KY LLC', 'Review Queue shows the name-variant suggestion'],
    ['#review-out', 'Confirm as held', 'Review Queue offers the one-click confirm to admins'],
    ['#mc-vend,#metrics-out', 'ZIP Electric', 'Metrics rendered its vendor table'],
    ['#review-out', 'still an', 'Review Queue explains the agenda-state rows'],
    ['#review-out', 'Converting', 'Review Queue says an agenda row needs converting'],
    ['#review-out', 'Likely not converted', 'Inferred state is labelled as a guess, not a fact'],
    ['#kpis', 'Awaiting minutes', 'KPI strip surfaces the un-converted meeting count'],
    ['#review-out', 'converted to', 'Review Queue flags the minutes-state rows'],
  ];
  // The "Not ingested" chip must appear exactly once: on the project that has
  // no data AND ingested:0. Hunting Creek carries no `ingested` field at all
  // (what the API returns before dbo.vendor_project_scope is mirrored) and must
  // NOT be labelled — Number(null) === 0 made every row claim it once already.
  const notIngested = await page.$$eval('#projects-out tr', (trs) =>
    trs.filter((tr) => /not ingested/i.test(tr.textContent || ''))
       .map((tr) => (tr.querySelector('.pname') || {}).textContent || '?'));
  if (notIngested.length !== 1 || !/Old 2024 Job/.test(notIngested[0])) {
    problems.push(`"Not ingested" should mark only the unfetched project, marked: `
      + (notIngested.length ? notIngested.join(', ') : '(none)'));
  }
  for (const [sel, needle, what] of expectations) {
    const text = await page.$$eval(sel, (els) => els.map((e) => e.textContent).join(' '))
      .catch(() => '');
    if (!text.includes(needle)) problems.push(`${what} — "${needle}" not found in ${sel}`);
  }

  // And the drilldown, which is where most of the rendering lives.
  // Back to Projects first: the tab loop above leaves Settings active, and
  // a bare `tbody tr` then matched a row in the admin table instead of a
  // project — so the drilldown never opened and every assertion below was
  // skipped in silence. The selector is data-pid, which is what the row
  // actually carries.
  await page.evaluate(() => {
    const b = document.querySelector('nav.tabs button[data-tab="projects"]');
    if (b) b.click();
  });
  await page.waitForTimeout(300);
  const projRow = await page.$('#projects-out tr.click[data-pid]');
  if (!projRow) {
    problems.push('no project row to open — the drilldown assertions cannot run');
  } else {
    await projRow.click().catch(() => {});
    await page.waitForTimeout(700);
  }
  // In the project drilldown: the title-matched vendor with an empty attendee
  // list must carry "No attendance list", and the attendee-matched one must
  // not. A flag that fires on every row conveys nothing.
  const modal = await page.$eval('#modal', (el) => el.textContent || '').catch(() => '');
  if (!/MAKK Concrete|ZIP Electric/.test(modal)) {
    problems.push('project drilldown did not render its vendor rows');
  } else {
    if (!modal.includes('No attendance list')) {
      problems.push('project modal: title-matched vendor with 0 attendees is not flagged');
    }
    // Exactly one: MAKK (title-matched, 0 vendor attendees) fires; Hive
    // (title-matched but 2 attendees recorded) and ZIP (attendee-matched) must
    // not. A flag that fires on every row conveys nothing.
    const hits = (modal.match(/No attendance list/g) || []).length;
    if (hits !== 1) problems.push(`"No attendance list" fired ${hits} times — expected exactly 1`);
  }

  await browser.close();
  server.close();

  if (problems.length) {
    console.error(`FAIL — ${problems.length} problem(s):`);
    for (const p of [...new Set(problems)]) console.error('  ' + p);
    process.exit(1);
  }
  console.log(`PASS — page loaded, ${tabs.length} tab(s) driven (${tabs.join(', ')}), no errors.`);
})();
