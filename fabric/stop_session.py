# ======================================================================
# LAST CELL: stop the Spark session the moment the work is done.
#
# Paste this as the FINAL cell of every Procore notebook (and the Vendor
# Compliance notebook; the same file is in that repo). Nothing else
# changes. Do NOT re-paste the other cells from the repo to add this:
# the live ingest cell is AHEAD of the repo copy (incremental sweep, Nov
# 2026) and re-pasting would revert it.
#
# Why: Fabric bills a Spark session for every second it is alive, on every
# node, and an interactive session (the paste-and-run workflow) stays alive
# for the workspace idle timeout (20 min by default) after the last cell
# finishes. That is 20 minutes of a Medium cluster doing nothing, on every
# manual run, smoothed onto the capacity for the next 24 hours. Stopping
# here ends the bill when the work ends. A scheduled run ends on its own;
# this cell is then a harmless no-op.
#
# Two things to know:
#   * After this runs the session is gone, so you cannot inspect variables
#     or re-run an earlier cell without starting a new session. That is the
#     point. Run it last, only when you are done.
#   * If the workspace has HIGH CONCURRENCY sessions switched on, this stops
#     the SHARED session for every notebook attached to it. Either keep high
#     concurrency off for these notebooks or drop this cell.
# ======================================================================
import time as _time

_t0 = _time.time()
print(f"Finished at {_time.strftime('%Y-%m-%d %H:%M:%S UTC', _time.gmtime())}. Stopping the Spark session...")

_stopped = False
try:
    notebookutils.session.stop()          # current Fabric runtime
    _stopped = True
except NameError:
    try:
        mssparkutils.session.stop()       # older runtimes
        _stopped = True
    except NameError:
        pass
except Exception as _e:                   # a stop that errors must never fail the run
    print(f"session.stop() raised: {_e!r}")

if not _stopped:
    print("No session utility found (not running in Fabric?). Nothing stopped.")
