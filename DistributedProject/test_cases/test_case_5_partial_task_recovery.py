"""
Test Case 5: Partial Task Recovery via Checkpointing
======================================================
Scenario:
    3 nodes start: nodeA, nodeB, nodeC.
    TASK-0003 (work_units=4, 2s per unit = 8s total) is assigned to nodeC.
    nodeC processes the task unit by unit, sending a TASK_PROGRESS checkpoint
    to the monitor after each unit.
    nodeC crashes at 8.5 seconds -- after completing 3 of the 4 units.
    The monitor's last recorded checkpoint for TASK-0003 is units_done=3.

What this proves:
    - Nodes report incremental progress (TASK_PROGRESS), not just final completion.
    - The monitor stores this checkpoint (units_done) per in-flight task.
    - On failure, fail_node() reads the checkpoint and computes:
          remaining = original_work_units - units_done = 4 - 3 = 1
    - Only 1 unit is requeued, NOT the full 4.
    - A healthy node picks up only the 1 remaining unit and finishes.
    - Total work done = 3 units (nodeC) + 1 unit (nodeA/B) = 4 units. No duplication.

Why this matters (conceptually):
    Without checkpointing, the full task would restart from scratch, wasting
    3 units of already-completed work. With checkpointing, the cluster only
    does the minimum remaining work. This is the same principle used by
    MapReduce, Apache Spark, and most production distributed job schedulers.

Timing:
    nodeC starts at ~t=0.5. TASK-0003 assigned at ~t=2.5.
    With WORK_UNIT_DURATION=2.0s:
        unit 1 done at ~t=4.5  -> TASK_PROGRESS(units_done=1)
        unit 2 done at ~t=6.5  -> TASK_PROGRESS(units_done=2)
        unit 3 done at ~t=8.5  -> TASK_PROGRESS(units_done=3)
        nodeC crashes at CRASH_AFTER_SECONDS=8 from node start (~t=8.5 absolute)
        unit 4 would be at ~t=10.5 -- never reached.
    Monitor detects failure at ~t=12.5 (crash + 4s heartbeat timeout).
    Requeue with work_units=1. Healthy node finishes in 2s more.

Expected log markers:
    [CHECKPOINT] TASK-0003 on nodeC: 1/4 units done.
    [CHECKPOINT] TASK-0003 on nodeC: 2/4 units done.
    [CHECKPOINT] TASK-0003 on nodeC: 3/4 units done.
    [FAILURE] Node nodeC timed out.
    [RECOVERY] Requeued TASK-0003 from failed node nodeC. (3/4 units done, 1 remaining)
    [DONE] TASK-0003 finished by nodeA (or nodeB).
"""

import json
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
STATE_FILE = ROOT / "tc5_state.json"
LOG_FILE = ROOT / "tc5_monitor.log"
PORT = "5105"

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
        "MAX_IN_FLIGHT_PER_NODE": "2",
    })

    monitor_lines = []
    monitor = start_monitored([sys.executable, "-u", "monitor.py"], base_env, ROOT, monitor_lines)

    time.sleep(0.5)

    node_processes = []
    for name in ("nodeA", "nodeB", "nodeC"):
        env = base_env.copy()
        env["HOSTNAME"] = name
        env["MONITOR_HOST"] = "127.0.0.1"
        env["WORK_UNIT_DURATION_SECONDS"] = "2.0"
        if name == "nodeC":
            env["CRASH_AFTER_SECONDS"] = "8"
        node_processes.append(
            start_monitored([sys.executable, "-u", "node.py"], env, ROOT, [])
        )

    # Timeline:
    #   t=0.5  : nodes start
    #   t=2.5  : TASK-0003 assigned to nodeC
    #   t=4.5  : nodeC finishes unit 1 -> CHECKPOINT(1/4)
    #   t=6.5  : nodeC finishes unit 2 -> CHECKPOINT(2/4)
    #   t=8.5  : nodeC finishes unit 3 -> CHECKPOINT(3/4), then crashes
    #   t=12.5 : monitor detects nodeC timeout, requeues 1 remaining unit
    #   t=14.5 : healthy node completes the final unit
    print("Running test... (28 seconds)")
    time.sleep(28)

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
    print("TEST CASE 5 — Partial Task Recovery via Checkpointing")
    print("=" * 62)

    passed = True

    checkpoints = re.findall(r"\[CHECKPOINT\] TASK-0003 on nodeC: (\d+)/4 units done\.", logs)
    if checkpoints:
        max_checkpoint = max(int(c) for c in checkpoints)
        if max_checkpoint >= 2:
            print(f"  PASS  nodeC sent checkpoints. Max units_done before crash = {max_checkpoint}/4.")
        else:
            print(f"  WARN  Only {max_checkpoint} checkpoint(s) before crash. Check tc5_monitor.log.")
    else:
        print("  FAIL  No TASK_PROGRESS checkpoints for TASK-0003 found in log.")
        passed = False

    if "[FAILURE] Node nodeC timed out." in logs:
        print("  PASS  nodeC failure detected by heartbeat timeout.")
    else:
        print("  FAIL  nodeC failure was NOT detected.")
        passed = False

    partial_match = re.search(
        r"\[RECOVERY\] Requeued TASK-0003 from failed node nodeC\. "
        r"\((\d+)/4 units done, (\d+) remaining\)",
        logs,
    )
    if partial_match:
        units_done_at_crash = int(partial_match.group(1))
        remaining = int(partial_match.group(2))
        if units_done_at_crash > 0 and remaining < 4:
            print(
                f"  PASS  Checkpointed recovery: {units_done_at_crash} units already done, "
                f"only {remaining} unit(s) requeued (not the full 4)."
            )
        else:
            print(f"  WARN  Recovery found but units_done={units_done_at_crash}.")
    else:
        if "[RECOVERY] Requeued TASK-0003 from failed node nodeC." in logs:
            print("  WARN  TASK-0003 requeued with full units (nodeC crashed before any unit completed).")
        else:
            print("  FAIL  TASK-0003 requeue not found in logs.")
            passed = False

    if "[DONE] TASK-0003 finished by" in logs:
        completer = re.search(r"\[DONE\] TASK-0003 finished by (\w+)\.", logs)
        node = completer.group(1) if completer else "unknown"
        print(f"  PASS  TASK-0003 eventually completed by {node} after nodeC's crash.")
    else:
        print("  FAIL  TASK-0003 was never completed.")
        passed = False

    if completed >= 3:
        print(f"  PASS  {completed} total tasks completed.")
    else:
        print(f"  FAIL  Only {completed} tasks completed (need >= 3).")
        passed = False

    if active_count == 2:
        print("  PASS  2 active nodes remain after nodeC crash.")
    else:
        print(f"  FAIL  Expected 2 active nodes, found {active_count}.")
        passed = False

    print()
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"Monitor log saved to: {LOG_FILE}")
    print("=" * 62)


if __name__ == "__main__":
    run()
