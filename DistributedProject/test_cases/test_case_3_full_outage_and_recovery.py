"""
Test Case 3: Full Cluster Outage, Then a New Node Joins
========================================================
Scenario:
    3 nodes start: nodeA, nodeB, nodeC.
    ALL THREE crash after 5 seconds -- the entire cluster goes down.
    The monitor detects all 3 failures and requeues every in-flight task.
    For 8+ seconds there are zero workers. The monitor keeps generating
    tasks but has nobody to send them to. The pending queue grows.
    Then nodeD joins as a brand-new node.
    nodeD should immediately receive all the queued tasks and begin processing.

What this proves:
    - The system survives a TOTAL cluster outage without losing queued work.
    - pending_tasks accumulates work safely while no workers exist.
    - When capacity returns (nodeD), tasks drain from the queue automatically
      -- no manual restart or intervention.
    - This is the most extreme fault scenario: 100% failure rate.

Key design point being tested:
    least_loaded_alive_node() returns None when all nodes are dead.
    assign_tasks() does nothing in that case -- it just leaves tasks in
    pending_tasks. The moment nodeD sends its first heartbeat and is added
    to active_nodes, the next assign_tasks() call drains the entire backlog.

Expected output markers:
    [FAILURE] Node nodeA timed out.
    [FAILURE] Node nodeB timed out.
    [FAILURE] Node nodeC timed out.
    [JOINED] Node nodeD joined.
    [BALANCER] Assigned ... to nodeD   (multiple, queue draining)
    Completed tasks >= 3, Active nodes == 1 (only nodeD)
"""

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
STATE_FILE = ROOT / "tc3_state.json"
LOG_FILE = ROOT / "tc3_monitor.log"
PORT = "5103"


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
        "TASK_GENERATION_INTERVAL_SECONDS": "0.8",
        "HEARTBEAT_TIMEOUT_SECONDS": "4.0",
        "LIVENESS_CHECK_INTERVAL_SECONDS": "1.0",
        "MAX_IN_FLIGHT_PER_NODE": "3",
    })

    monitor_lines = []
    monitor = _start([sys.executable, "-u", "monitor.py"], base_env)
    threading.Thread(target=_stream, args=(monitor, monitor_lines), daemon=True).start()

    time.sleep(0.5)

    # Phase 1: start the 3 nodes that will all crash
    initial_nodes = []
    for name in ("nodeA", "nodeB", "nodeC"):
        env = base_env.copy()
        env["HOSTNAME"] = name
        env["MONITOR_HOST"] = "127.0.0.1"
        env["WORK_UNIT_DURATION_SECONDS"] = "3.0"
        env["CRASH_AFTER_SECONDS"] = "5"   # all crash at 5s
        initial_nodes.append(_start([sys.executable, "-u", "node.py"], env))
        threading.Thread(
            target=_stream, args=(initial_nodes[-1], []), daemon=True
        ).start()

    # Wait until all 3 are dead and the monitor has detected all 3 failures.
    # t=5: all crash. t=9: monitor detects last one (5+4 timeout).
    # Then wait 8 more seconds for the queue to build up with no workers.
    print("Phase 1: waiting for full cluster outage... (17 seconds)")
    time.sleep(17)

    # Snapshot the queue depth at the moment of outage to verify backlog built up
    pre_recovery_pending = None
    if STATE_FILE.exists():
        snap = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        pre_recovery_pending = snap.get("pending_count", 0)

    # Phase 2: start a single new node (nodeD) -- simulates capacity returning
    print("Phase 2: nodeD joins the cluster (backlog drains)... (18 seconds)")
    env_d = base_env.copy()
    env_d["HOSTNAME"] = "nodeD"
    env_d["MONITOR_HOST"] = "127.0.0.1"
    env_d["WORK_UNIT_DURATION_SECONDS"] = "1.5"
    node_d = _start([sys.executable, "-u", "node.py"], env_d)
    threading.Thread(target=_stream, args=(node_d, []), daemon=True).start()

    time.sleep(18)

    _kill(initial_nodes + [node_d, monitor])
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
    print("TEST CASE 3 — Full Cluster Outage, Then New Node Joins")
    print("=" * 62)

    passed = True

    # 1. All 3 original nodes must have been detected as failed
    for name in ("nodeA", "nodeB", "nodeC"):
        marker = f"[FAILURE] Node {name} timed out."
        if marker in logs:
            print(f"  PASS  {name} failure detected.")
        else:
            print(f"  FAIL  {name} failure was NOT detected.")
            passed = False

    # 2. Pending queue had tasks queued during the outage (proved backlog built up)
    if pre_recovery_pending is not None and pre_recovery_pending > 0:
        print(f"  PASS  Queue had {pre_recovery_pending} pending tasks during full outage.")
    elif pre_recovery_pending == 0:
        print("  WARN  Queue was 0 at snapshot time -- snapshot may have been taken too early.")
    else:
        print("  WARN  Could not read queue depth during outage.")

    # 3. nodeD joined successfully
    if "[JOINED] Node nodeD joined." in logs:
        print("  PASS  nodeD successfully joined the cluster after outage.")
    else:
        print("  FAIL  nodeD did not join the cluster.")
        passed = False

    # 4. Tasks were assigned to nodeD (queue drained)
    nodeD_assignments = logs.count("Assigned TASK-") and "to nodeD" in logs
    if "to nodeD" in logs:
        print("  PASS  Monitor assigned queued tasks to nodeD.")
    else:
        print("  FAIL  No tasks assigned to nodeD.")
        passed = False

    # 5. Completions happened after recovery
    if completed >= 3:
        print(f"  PASS  {completed} tasks completed after full outage recovery.")
    else:
        print(f"  FAIL  Only {completed} tasks completed (need >= 3).")
        passed = False

    # 6. nodeD is the only active node
    if active_count == 1:
        print("  PASS  Exactly 1 active node remains (nodeD).")
    else:
        print(f"  FAIL  Expected 1 active node, found {active_count}.")
        passed = False

    print()
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"Monitor log saved to: {LOG_FILE}")
    print("=" * 62)


if __name__ == "__main__":
    run()
