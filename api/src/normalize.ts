/**
 * Company-name normalization.
 *
 * ⚠️ This is a THIRD copy of the same rules that live in
 * fabric/ingest_vendor_compliance.py and fabric/build_vendor_gold.py. The Python
 * copies produce the keys stored in dbo.vendor_roster / dbo.vendor_prep_matches;
 * this one only ever runs on names a human types into the tracker (a manual
 * vendor, or an override addressed by name). If the three diverge, a
 * hand-entered vendor silently fails to line up with its Procore row and reads
 * as a second, permanently-outstanding company.
 *
 * Keep all three in sync. The rules are intentionally small so that is cheap:
 * lowercase, "&" spelled out, punctuation stripped, whitespace collapsed, and
 * trailing legal-entity tokens removed. Industry words ("construction",
 * "masonry") are deliberately KEPT — they are what distinguishes two vendors.
 */
const LEGAL_SUFFIX_TOKENS = new Set([
  'llc', 'lc', 'inc', 'incorporated', 'co', 'corp', 'corporation', 'company',
  'ltd', 'limited', 'lp', 'llp', 'plc', 'pllc', 'pc', 'pa',
]);

export function normalizeCompany(name: string | null | undefined): string {
  if (!name) return '';
  let s = String(name).toLowerCase();
  s = s.replace(/&/g, ' and ');
  // Periods are DELETED, not turned into spaces, so "L.L.C." collapses to "llc"
  // and gets stripped as a suffix. Turning them into spaces instead produced
  // "l l c", three tokens that survive the suffix strip — which meant
  // "Smith & Sons, L.L.C." and "Smith and Sons LLC" were two different vendors.
  // Deleting is safe for the "St. Louis" case too: the following space survives.
  s = s.replace(/\./g, '');
  s = s.replace(/[^a-z0-9 ]+/g, ' ');
  s = s.replace(/\s+/g, ' ').trim();
  let tokens = s.split(' ').filter(Boolean);

  // Strip trailing legal-entity tokens. Runs twice around the single-letter
  // rescue below so "Contracting L L C Inc" fully unwinds.
  const stripTrailing = (): void => {
    while (tokens.length && LEGAL_SUFFIX_TOKENS.has(tokens[tokens.length - 1])) tokens.pop();
  };
  stripTrailing();

  // Rescue an already-space-separated acronym ("l l c") that the period fix
  // above wouldn't have seen, e.g. a name typed as "Smith L L C".
  let i = tokens.length;
  while (i > 0 && tokens[i - 1].length === 1) i--;
  if (i < tokens.length && LEGAL_SUFFIX_TOKENS.has(tokens.slice(i).join(''))) {
    tokens = tokens.slice(0, i);
    stripTrailing();
  }

  return tokens.join(' ');
}
