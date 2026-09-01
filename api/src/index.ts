// SWA managed Functions entry point.
//
// One file per concern; each registers its own app.http routes. Note that
// misc.ts registers several small routes together — the "one app.http() per
// file" rule that bit the intranet applies to *duplicate registrations of the
// same name*, not to distinct routes, and keeping four trivial handlers in one
// file is fewer moving parts than four near-empty ones.
import './functions/misc.js';
import './functions/tracker.js';
import './functions/projectDetail.js';
import './functions/overrides.js';
import './functions/manualVendors.js';
import './functions/settings.js';
import './functions/admins.js';
