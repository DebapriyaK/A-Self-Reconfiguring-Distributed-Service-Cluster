"""
Test Case 2: Two Nodes Crash Simultaneously
============================================
Scenario:
    3 nodes start: nodeA, nodeB, nodeC.
    nodeB AND nodeC both crash at the same time (after 7 seconds).
    Both had in-flight tasks when they crashed.
    nodeA is the only survivor.

What this proves:
    - The monitor handles MULTIPLE simultaneous node failures correctly.
    - fail_node() is called independently for each dead node -- they don't
      interfere with each other.
    - ALL stranded tasks from BOTH dead nodes are requeued.
    - The single surviving node (nodeA) picks up all the requeued work.
    - The cluster continues operating at reduced capacity (1 node).

Expected output markers:
    [FAILURE] Node nodeB timed out.
    [FAILURE] Node nodeC timed out.
    [RECOVERY] Requeued ... from failed node nodeB.
    [RECOVERY] Requeued ... from failed node nodeC.
    [RECONFIGURED] Cluster size=1.
    Completed tasks >= 4, Active nodes == 1
"""

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
STATE_FILE = ROOT / "tc2_state.json"
LOG_FILE = ROOT / "tc2_monitor.log"
PORT = "5102"

sys.path.insert(0, str(Path(__file__).parent))
from helpers import kill_all, start_monitored


def run():
    for f in (STATE_FILE, LOG_FILE):
        if f.exists():
            f.unlink()

    base_env = os.environ.copy()
    base_env.update({
        "PYTHONUNBUFFERED": "1",
        "MONITOR_PORT": PORT,
        "STATE_SNAPSHOT_FILE": str(STATE_FILE),
        "TASK_GENERATION_INTERVAL_SECONDS": "1.0",
        "HEARTBEAT_TIMEOUT_SECONDS": "4.0",
        "LIVENESS_CHECK_INTERVAL_SECONDS": "1.0",
        "MAX_IN_FLIGHT_PER_NODE": "3",
    })

    monitor_lines = []
    monitor = start_monitored([sys.executable, "-u", "monitor.py"], base_env, ROOT, monitor_lines)

    time.sleep(0.5)

    node_processes = []
    for name in ("nodeA", "nodeB", "nodeC"):
        env = base_env.copy()
        env["HOSTNAME"] = name
        env["MONITOR_HOST"] = "127.0.0.1"
        env["WORK_UNIT_DURATION_SECONDS"] = "2.5"
        if name in ("nodeB", "nodeC"):
            env["CRASH_AFTER_SECONDS"] = "7"
        node_processes.append(
            start_monitored([sys.executable, "-u", "node.py"], env, ROOT, [])
        )

    # Timeline:
    #   t=0   : all 3 nodes start
    #   t=7   : nodeB and nodeC crash simultaneously
    #   t=11  : monitor detects both (7s crash + 4s timeout)
    #   t=11  : tasks from nodeB and nodeC requeued to nodeA
    print("Running test... (35 seconds)")
    time.sleep(35)

    kill_all(node_processes + [monitor])
    LOG_FILE.write_text("".join(monitor_lines), encoding="utf-8")

    logs = "".join(monitor_lines)

    if not STATE_FILE.exists():
        print("RESULT: FAIL  —  monitor never wrote a state snapshot.")
        return

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    completed = state["completed_count"]
    active_count = len(state["active_nodes"])

    print()
    print("=" * 62)
    print("TEST CASE 2 — Two Nodes Crash Simultaneously")
    print("=" * 62)

    passed = True

    for name in ("nodeB", "nodeC"):
        if f"[FAILURE] Node {name} timed out." in logs:
            print(f"  PASS  {name} failure detected.")
        else:
            print(f"  FAIL  {name} failure was NOT detected.")
            passed = False

    for name in ("nodeB", "nodeC"):
        if f"from failed node {name}" in logs:
            print(f"  PASS  Tasks requeued from {name}.")
        else:
            print(f"  FAIL  No task requeue from {name} found.")
            passed = False

    if "[RECONFIGURED] Cluster size=1." in logs:
        print("  PASS  Cluster reconfigured to size=1.")
    else:
        print("  FAIL  Cluster size=1 log line not found.")
        passed = False

    if completed >= 4:
        print(f"  PASS  {completed} tasks completed despite double failure.")
    else:
        print(f"  FAIL  Only {completed} tasks completed (need >= 4).")
        passed = False

    if active_count == 1:
        print("  PASS  Exactly 1 active node remains (nodeA).")
    else:
        print(f"  FAIL  Expected 1 active node, found {active_count}.")
        passed = False

    print()
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"Monitor log saved to: {LOG_FILE}")
    print("=" * 62)


if __name__ == "__main__":
    run()
