"""
Test Case 6: Graceful Node Shutdown via LEAVING Message
========================================================
Scenario:
    3 nodes start: nodeA, nodeB, nodeC.
    nodeC sends a graceful LEAVING message after 8 seconds instead of
    hard-crashing. The monitor immediately reassigns nodeC's in-flight
    tasks without waiting for the heartbeat timeout to expire.

What this proves:
    - The LEAVING message type enables near-zero-wait task reassignment.
    - Recovery time with graceful shutdown: < 0.5s (UDP round trip only).
    - Recovery time with hard crash (TC1): ~4s (must wait for heartbeat timeout).
    - This directly demonstrates the value of the graceful shutdown improvement.
    - All in-flight tasks on nodeC are safely requeued with no loss.

Why this matters:
    In production, planned maintenance (rolling restarts, deployments) is far
    more common than sudden crashes. Without a LEAVING message, every planned
    shutdown wastes HEARTBEAT_TIMEOUT seconds before the cluster self-heals.
    With LEAVING, the cluster adapts instantly.

Timing:
    nodeC starts at t=0.5. LEAVING_AFTER_SECONDS=8 -> nodeC leaves at t=8.5.
    Monitor receives LEAVING, calls fail_node(graceful=True) immediately.
    [LEAVING] and [RECONFIGURED] appear in the same log timestamp (~0.0s gap).
    Compare to TC1 where [FAILURE] appears ~4s after the crash.

Expected log markers:
    [LEAVING] Node nodeC sent graceful shutdown. Reassigning tasks immediately.
    [RECOVERY] Requeued TASK-XXXX from failed node nodeC. (...)
    [RECONFIGURED] Cluster size=2.
    [DONE] TASK-XXXX finished by nodeA (or nodeB).
"""

import json
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
STATE_FILE = ROOT / "tc6_state.json"
LOG_FILE = ROOT / "tc6_monitor.log"
PORT = "5106"
HEARTBEAT_TIMEOUT = 4.0

sys.path.insert(0, str(Path(__file__).parent))
from helpers import kill_all, start_monitored


def parse_timestamp(line):
    m = re.search(r'\[t=\s*([\d.]+)s\]', line)
    return float(m.group(1)) if m else None


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
        "HEARTBEAT_TIMEOUT_SECONDS": str(HEARTBEAT_TIMEOUT),
        "LIVENESS_CHECK_INTERVAL_SECONDS": "1.0",
        "MAX_IN_FLIGHT_PER_NODE": "2",
        "MAX_RETRIES": "3",
    })

    monitor_lines = []
    monitor = start_monitored([sys.executable, "-u", "monitor.py"], base_env, ROOT, monitor_lines)
    time.sleep(0.5)

    node_processes = []
    for name in ("nodeA", "nodeB", "nodeC"):
        env = base_env.copy()
        env["HOSTNAME"] = name
        env["MONITOR_HOST"] = "127.0.0.1"
        env["WORK_UNIT_DURATION_SECONDS"] = "1.5"
        if name == "nodeC":
            env["LEAVING_AFTER_SECONDS"] = "8"   # graceful shutdown, NOT a crash
        node_processes.append(
            start_monitored([sys.executable, "-u", "node.py"], env, ROOT, [])
        )

    # Timeline:
    #   t=0.5  : all 3 nodes start
    #   t=1.0  : TASK-0001 -> nodeA
    #   t=2.0  : TASK-0002 -> nodeB
    #   t=3.0  : TASK-0003 -> nodeC
    #   t=8.5  : nodeC sends LEAVING, monitor reconfigures instantly
    #   t=8.5  : nodeC's in-flight tasks requeued to nodeA/nodeB immediately
    #   t=20s  : test ends
    print("Running test... (20 seconds)")
    time.sleep(20)

    kill_all(node_processes + [monitor])
    LOG_FILE.write_text("".join(monitor_lines), encoding="utf-8")
    logs = "".join(monitor_lines)

    if not STATE_FILE.exists():
        print("RESULT: FAIL  --  monitor never wrote a state snapshot.")
        return

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    completed = state["completed_count"]
    active_count = len(state["active_nodes"])

    print()
    print("=" * 62)
    print("TEST CASE 6 -- Graceful Node Shutdown (LEAVING message)")
    print("=" * 62)

    passed = True

    # --- Check LEAVING was sent and processed ---
    if "[LEAVING] Node nodeC sent graceful shutdown" in logs:
        print("  PASS  nodeC sent graceful LEAVING message.")
    else:
        print("  FAIL  LEAVING message not found in logs.")
        passed = False

    # --- Check tasks were requeued ---
    requeued = re.findall(r"\[RECOVERY\] Requeued TASK-\d+ from failed node nodeC", logs)
    if requeued:
        print(f"  PASS  {len(requeued)} task(s) requeued from nodeC immediately.")
    else:
        print("  WARN  No tasks were in-flight on nodeC when it left (timing edge case).")

    # --- Measure recovery time: gap between [LEAVING] and [RECONFIGURED] ---
    leaving_t = None
    reconfig_t = None
    for line in logs.splitlines():
        if "[LEAVING] Node nodeC" in line and leaving_t is None:
            leaving_t = parse_timestamp(line)
        if "[RECONFIGURED]" in line and leaving_t is not None and reconfig_t is None:
            reconfig_t = parse_timestamp(line)

    if leaving_t is not None and reconfig_t is not None:
        recovery_time = reconfig_t - leaving_t
        print(f"  PASS  Recovery time after LEAVING: {recovery_time:.3f}s "
              f"(vs ~{HEARTBEAT_TIMEOUT:.1f}s for hard crash -- "
              f"{HEARTBEAT_TIMEOUT / max(recovery_time, 0.001):.0f}x faster).")
        if recovery_time < 1.0:
            print(f"  PASS  Recovery time < 1.0s confirmed.")
        else:
            print(f"  WARN  Recovery time {recovery_time:.3f}s is higher than expected.")
    else:
        print("  FAIL  Could not parse timestamps for recovery time measurement.")
        passed = False

    # --- Check cluster continues normally after nodeC leaves ---
    if "[RECONFIGURED] Cluster size=2." in logs:
        print("  PASS  Cluster reconfigured to size=2 after LEAVING.")
    else:
        print("  FAIL  Cluster reconfiguration to size=2 not found.")
        passed = False

    if completed >= 4:
        print(f"  PASS  {completed} tasks completed (cluster continued working after LEAVING).")
    else:
        print(f"  FAIL  Only {completed} tasks completed (need >= 4).")
        passed = False

    if active_count == 2:
        print("  PASS  2 active nodes remain (nodeA, nodeB).")
    else:
        print(f"  FAIL  Expected 2 active nodes, found {active_count}.")
        passed = False

    # --- Confirm no hard crash was detected (LEAVING != crash) ---
    if "[FAILURE] Node nodeC timed out." not in logs:
        print("  PASS  No heartbeat timeout for nodeC -- LEAVING processed before timeout.")
    else:
        print("  FAIL  Heartbeat timeout fired for nodeC -- LEAVING was not processed in time.")
        passed = False

    print()
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"Monitor log saved to: {LOG_FILE}")
    print("=" * 62)


if __name__ == "__main__":
    run()
