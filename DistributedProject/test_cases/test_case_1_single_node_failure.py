"""
Test Case 1: Single Node Failure Mid-Task
==========================================
Scenario:
    3 nodes start: nodeA, nodeB, nodeC.
    The monitor assigns tasks using least-loaded balancing.
    Because all nodes start at load=0 and names are sorted alphabetically,
    the first 3 tasks always go to nodeA, nodeB, nodeC respectively.
    TASK-0003 (work_units=4) lands on nodeC and takes 8 seconds to finish.
    nodeC is configured to crash after 3 seconds -- well before TASK-0003 completes.

What this proves:
    - Heartbeat timeout correctly identifies a crashed node (~4s after last heartbeat).
    - All in-flight tasks on the dead node are requeued automatically.
    - Healthy nodes pick up the stranded task and complete it.
    - Cluster continues processing new work with the 2 remaining nodes.

Expected output markers:
    [FAILURE] Node nodeC timed out.
    [RECOVERY] Requeued TASK-0003 from failed node nodeC.
    [RECONFIGURED] Cluster size=2.
    Completed tasks >= 3, Active nodes == 2
"""

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

# monitor.py and node.py live one level up from this file
ROOT = Path(__file__).parent.parent
STATE_FILE = ROOT / "tc1_state.json"
LOG_FILE = ROOT / "tc1_monitor.log"

# Use a unique port so this test never collides with other tests or Docker
PORT = "5101"


# ── helpers ──────────────────────────────────────────────────────────────────

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


# ── test ─────────────────────────────────────────────────────────────────────

def run():
    # Clean up leftover files from any previous run
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
        "MAX_IN_FLIGHT_PER_NODE": "2",
    })

    monitor_lines = []
    monitor = _start([sys.executable, "-u", "monitor.py"], base_env)
    threading.Thread(target=_stream, args=(monitor, monitor_lines), daemon=True).start()

    # Give monitor a moment to bind its socket before nodes try to connect
    time.sleep(0.5)

    node_processes = []
    for name in ("nodeA", "nodeB", "nodeC"):
        env = base_env.copy()
        env["HOSTNAME"] = name
        env["MONITOR_HOST"] = "127.0.0.1"
        # 2.0s per work_unit: TASK-0003 has work_units=4, so takes 8s total.
        # nodeC crashes at 3s -- TASK-0003 is only 3/8 done at crash time.
        env["WORK_UNIT_DURATION_SECONDS"] = "2.0"
        if name == "nodeC":
            env["CRASH_AFTER_SECONDS"] = "3"
        node_processes.append(_start([sys.executable, "-u", "node.py"], env))
        threading.Thread(
            target=_stream, args=(node_processes[-1], []), daemon=True
        ).start()

    # Timeline:
    #   t=0   : nodes start
    #   t=3   : nodeC crashes
    #   t=7   : monitor detects nodeC (3s crash + 4s timeout)
    #   t=7   : TASK-0003 requeued, assigned to nodeA or nodeB
    #   t=~15 : reassigned TASK-0003 finishes (up to 8s after reassignment)
    # Run 28s to comfortably capture all events
    print("Running test... (28 seconds)")
    time.sleep(28)

    _kill(node_processes + [monitor])
    LOG_FILE.write_text("".join(monitor_lines), encoding="utf-8")

    # ── assertions ────────────────────────────────────────────────────────────
    logs = "".join(monitor_lines)

    if not STATE_FILE.exists():
        print("RESULT: FAIL  —  monitor never wrote a state snapshot.")
        return

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    completed = state["completed_count"]
    active_count = len(state["active_nodes"])

    print()
    print("=" * 62)
    print("TEST CASE 1 — Single Node Failure Mid-Task")
    print("=" * 62)

    passed = True

    # 1. nodeC failure must have been detected
    if "[FAILURE] Node nodeC timed out." in logs:
        print("  PASS  nodeC failure detected by heartbeat timeout.")
    else:
        print("  FAIL  nodeC failure was NOT detected.")
        passed = False

    # 2. The stranded task must have been requeued
    if "[RECOVERY] Requeued" in logs and "from failed node nodeC" in logs:
        print("  PASS  Stranded task requeued from nodeC.")
    else:
        print("  FAIL  No task requeue from nodeC found in logs.")
        passed = False

    # 3. Cluster reconfigured to 2 nodes
    if "[RECONFIGURED] Cluster size=2." in logs:
        print("  PASS  Cluster reconfigured to size=2.")
    else:
        print("  FAIL  Cluster reconfiguration log line not found.")
        passed = False

    # 4. Healthy nodes kept completing tasks after failure
    if completed >= 3:
        print(f"  PASS  {completed} tasks completed (>= 3 required).")
    else:
        print(f"  FAIL  Only {completed} tasks completed (need >= 3).")
        passed = False

    # 5. Exactly 2 nodes remain active
    if active_count == 2:
        print(f"  PASS  2 active nodes remain after nodeC crash.")
    else:
        print(f"  FAIL  Expected 2 active nodes, found {active_count}.")
        passed = False

    print()
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"Monitor log saved to: {LOG_FILE}")
    print("=" * 62)


if __name__ == "__main__":
    run()
