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
      superintendent: 'Ken Houston', vendor_count: 12, held_count: 9, outstanding_count: 3,
      pct: 75, last_meeting_date: '2026-03-17', unmatched_count: 2 },
    { project_id: 3387062, project_name: 'AEP Eagle Pass Service Center', project_number: '25-004',
      superintendent: null, vendor_count: 20, held_count: 0, outstanding_count: 20,
      pct: 0, last_meeting_date: null, unmatched_count: 4 },
  ],
  '/api/projects/3176472': {
    project: { project_id: 3176472, project_name: 'Hunting Creek GC Snack Shack', pct: 75 },
    vendors: [
      { vendor_normalized: 'zip electric', vendor_name: 'ZIP Electric LLC', status: 'held',
        match_method: 'attendee', meeting_id: 12457210, meeting_date: '2026-03-17',
        attendee_attended: true, trade_name: 'Electrical' },
      { vendor_normalized: 'makk concrete', vendor_name: 'MAKK Concrete', status: 'held',
        match_method: 'title', meeting_id: 12406175, meeting_date: '2026-03-05',
        attendee_attended: null, trade_name: 'Concrete' },
      { vendor_normalized: 'escar construction', vendor_name: 'Escar Construction',
        status: 'not_held', match_method: null, meeting_id: null, meeting_date: null,
        attendee_attended: null, trade_name: null },
    ],
    meetings: [
      { meeting_id: 12457210, title: 'Preparatory Meeting Agenda - ZIP', meeting_date: '2026-03-17',
        vendor_attendee_count: 3, matched_vendors: 1 },
    ],
    unmatched: [
      { meeting_id: 12406175, title: 'Preparatory Meeting Agenda- H&W LandWorks',
        meeting_date: '2026-03-05', vendor_attendee_count: 0 },
    ],
  },
  '/api/unmatched-meetings': [
    { project_id: 3176472, project_name: 'Hunting Creek GC Snack Shack', meeting_id: 12406175,
      title: 'Preparatory Meeting Agenda- H&W LandWorks', meeting_date: '2026-03-05',
      vendor_attendee_count: 0 },
  ],
  '/api/settings': {
    settings: { vendorSource: 'either', allowTitleMatch: 1, requireVendorPresent: 0,
                requireMeetingHeld: 0, adminMode: 'allowlist' },
    coverage: { commitment_projects: 59, directory_projects: 91,
                commitment_vendors: 1390, directory_vendors: 2362 },
  },
  '/api/overrides': [],
  '/api/admins': [{ email: 'cory.zilisch@buffaloconstruction.com', added_by: 'bootstrap' }],
  '/api/metrics': {
    snapshot: { projects: 88, projects_with_meetings: 10, meetings: 63, vendors_credited: 59,
                vendors_total: 2362, coverage_pct: 2.5, adoption_pct: 11.4 },
    monthly: [
      { month: '2026-03', meetings: 18, projects: 1, vendors_credited: 14,
        by_attendee: 16, by_title: 2, unmatched: 2, attendance_ticked: 12 },
      { month: '2026-04', meetings: 10, projects: 1, vendors_credited: 6,
        by_attendee: 6, by_title: 0, unmatched: 4, attendance_ticked: 6 },
    ],
    projects: [
      { project_id: 3176472, project_name: 'Hunting Creek GC Snack Shack', meetings: 18, pct: 75 },
    ],
    vendors: [{ vendor_name: 'ZIP Electric LLC', meetings: 6, projects: 3 }],
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
    res.end(JSON.stringify({ data, meta: { generated_at: NOW, cached: false } }));
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
  // And the drilldown, which is where most of the rendering lives.
  for (const sel of ['[data-project-id]', 'tbody tr']) {
    const el = await page.$(sel);
    if (el) { await el.click().catch(() => {}); await page.waitForTimeout(400); break; }
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
