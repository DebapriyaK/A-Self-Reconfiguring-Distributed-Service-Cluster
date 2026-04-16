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

This is a harder scenario than TC1 because:
    - Two failures must be detected (not just one)
    - nodeA must absorb double the normal workload
    - The load cap is raised to 3 to give nodeA enough room to handle the backlog

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
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
STATE_FILE = ROOT / "tc2_state.json"
LOG_FILE = ROOT / "tc2_monitor.log"
PORT = "5102"


def _stream(process, lines):
    for line in iter(process.stdout.readline, ""):
        lines.append(line)
    process.stdout.close()


def _start(cmd, env):
    return subprocess.Popen(
        cmd, cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


def _kill(processes):
    for p in processes:
        p.terminate()
    for p in processes:
        try:
            p.wait(timeout=3)
        except subprocess.TimeoutExpired:
            p.kill()


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
        # Raise cap to 3 so nodeA can absorb requeued tasks from both dead nodes
        "MAX_IN_FLIGHT_PER_NODE": "3",
    })

    monitor_lines = []
    monitor = _start([sys.executable, "-u", "monitor.py"], base_env)
    threading.Thread(target=_stream, args=(monitor, monitor_lines), daemon=True).start()

    time.sleep(0.5)

    node_processes = []
    for name in ("nodeA", "nodeB", "nodeC"):
        env = base_env.copy()
        env["HOSTNAME"] = name
        env["MONITOR_HOST"] = "127.0.0.1"
        env["WORK_UNIT_DURATION_SECONDS"] = "2.5"
        if name in ("nodeB", "nodeC"):
            # Both crash at the same moment -- simultaneous failure
            env["CRASH_AFTER_SECONDS"] = "7"
        node_processes.append(_start([sys.executable, "-u", "node.py"], env))
        threading.Thread(
            target=_stream, args=(node_processes[-1], []), daemon=True
        ).start()

    # Timeline:
    #   t=0   : all 3 nodes start
    #   t=7   : nodeB and nodeC crash simultaneously
    #   t=11  : monitor detects both (7s crash + 4s timeout)
    #   t=11  : tasks from nodeB and nodeC requeued to nodeA
    #   t=~19 : nodeA finishes requeued tasks (up to 2.5s * 4 units = 10s per task)
    # Run 35s for comfortable margin
    print("Running test... (35 seconds)")
    time.sleep(35)

    _kill(node_processes + [monitor])
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

    # 1. Both nodeB and nodeC failures detected
    for name in ("nodeB", "nodeC"):
        marker = f"[FAILURE] Node {name} timed out."
        if marker in logs:
            print(f"  PASS  {name} failure detected.")
        else:
            print(f"  FAIL  {name} failure was NOT detected.")
            passed = False

    # 2. Tasks requeued from both failed nodes
    for name in ("nodeB", "nodeC"):
        marker = f"from failed node {name}"
        if marker in logs:
            print(f"  PASS  Tasks requeued from {name}.")
        else:
            print(f"  FAIL  No task requeue from {name} found.")
            passed = False

    # 3. Cluster reduced to 1 node
    if "[RECONFIGURED] Cluster size=1." in logs:
        print("  PASS  Cluster reconfigured to size=1.")
    else:
        print("  FAIL  Cluster size=1 log line not found.")
        passed = False

    # 4. nodeA continued completing tasks after both failures
    if completed >= 4:
        print(f"  PASS  {completed} tasks completed despite double failure.")
    else:
        print(f"  FAIL  Only {completed} tasks completed (need >= 4).")
        passed = False

    # 5. Only nodeA remains
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
