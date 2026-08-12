# Vendor-Compliance — Prep-Phase Meeting Tracker

Tracks the **Preparatory Meeting** Buffalo Construction holds with each vendor before that vendor
starts work on a job.

For every active project (Course of Construction — the same definition the Safety Dashboard and
the intranet use), the tracker lists every vendor on the job and answers one question per row:

> **Has their preparatory meeting happened — yes or no?**

## How it knows

The meetings are created in Procore from the meeting template at
`.../tools/meetings/create/383995` and retitled with the vendor's name
("Preparatory Meeting - ZIP"). The tracker credits a vendor from either of two signals:

- **the attendee list** — someone from that company was on the meeting (strong; also knows whether
  they were marked *Present* vs *For Distribution Only*)
- **the meeting title** — the vendor's name appears in it (weaker, flagged as a "title match",
  and switchable off)

Meetings that match **neither** aren't dropped — they go to a **Review Queue**, because a prep
meeting nobody can credit is a gap in the data, not a reason to show a better percentage.

## Quick links

- **Setting it up / deploying:** [`docs/setup.md`](docs/setup.md)
- **Design decisions, matching internals, what's still unverified:** [`CLAUDE.md`](CLAUDE.md)

## Stack

Procore API → Fabric Lakehouse (bronze/silver/gold) → Fabric Data Pipeline → the **Safety-Dash**
Fabric SQL Database → Azure Static Web Apps + managed Azure Functions → a single-file dashboard.

It shares Safety-Dash's SQL database rather than provisioning its own: the Fabric capacity is
already throttling, and `dbo.projects` lives there — so "the same active projects as the other
dashboards" is true by construction instead of by a duplicated definition.

## Status

Built and statically verified; **not yet run against live Procore data.** The first ingest prints
four diagnostics that settle the remaining open questions (which vendor roster BCI actually
maintains, whether the meeting template id is exposed by the API, and whether the attendee
company/attendance fields were read correctly). See the *What is NOT verified* section in
`CLAUDE.md` before trusting a number.
