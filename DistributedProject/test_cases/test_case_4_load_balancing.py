"""
Test Case 4: Load Balancing Verification (Least-Connections Algorithm)
=======================================================================
Scenario:
    3 nodes start: nodeA, nodeB, nodeC. None of them crash.
    Tasks arrive quickly (every 0.4 seconds).
    MAX_IN_FLIGHT_PER_NODE is capped at 1 -- so as soon as a node
    receives a task, its load=1=cap, and the NEXT task is forced to go
    to one of the other two nodes.

What this proves:
    - The "Least Connections" algorithm distributes work fairly.
    - No single node monopolises all the tasks.
    - The per-node cap is respected (no node exceeds MAX_IN_FLIGHT_PER_NODE).
    - Over a long run, each node completes a roughly equal share of work.

How the algorithm works (explained):
    least_loaded_alive_node() builds a list of all nodes with load < cap,
    sorts them by (load, name), and returns the name of the first entry.
    With cap=1 and 3 nodes, the rotation is:
        - All at 0: assign to nodeA (alphabetically first at equal load)
        - nodeA now at 1=cap, others at 0: assign to nodeB
        - nodeB at 1=cap, nodeC at 0: assign to nodeC
        - All at cap: wait until a completion drops one below cap
    This guarantees round-robin distribution when work units are equal.

Verification approach:
    Parse the monitor log to count how many tasks each node completed.
    Fair distribution means no node should have more than 2x the completions
    of the least-loaded node. We also verify that all 3 nodes participated.

Expected output:
    Each node completes roughly equal tasks.
    Max completions per node <= 2 * min completions per node.
    All 3 nodes appear in [DONE] lines.
"""

import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).parent.parent
STATE_FILE = ROOT / "tc4_state.json"
LOG_FILE = ROOT / "tc4_monitor.log"
PORT = "5104"


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
        # Fast task generation so the balancer is under constant pressure
        "TASK_GENERATION_INTERVAL_SECONDS": "0.4",
        "HEARTBEAT_TIMEOUT_SECONDS": "5.0",
        "LIVENESS_CHECK_INTERVAL_SECONDS": "1.0",
        # Cap of 1: once a node has 1 task, it's full -- forces spread
        "MAX_IN_FLIGHT_PER_NODE": "1",
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
        # 1.0s per work_unit keeps tasks short and completions frequent
        env["WORK_UNIT_DURATION_SECONDS"] = "1.0"
        # No crash -- this test is purely about balancing, not failure handling
        node_processes.append(_start([sys.executable, "-u", "node.py"], env))
        threading.Thread(
            target=_stream, args=(node_processes[-1], []), daemon=True
        ).start()

    # With 0.4s task interval and 1s task duration, cap=1, 3 nodes:
    # Throughput ~ 3 tasks/s.  Over 22s → ~66 tasks generated, ~55 completed.
    # Expected ~18 per node.
    print("Running test... (22 seconds)")
    time.sleep(22)

    _kill(node_processes + [monitor])
    LOG_FILE.write_text("".join(monitor_lines), encoding="utf-8")

    logs = "".join(monitor_lines)

    if not STATE_FILE.exists():
        print("RESULT: FAIL  —  monitor never wrote a state snapshot.")
        return

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    total_completed = state["completed_count"]

    # Count completions per node by parsing "[DONE] TASK-xxxx finished by nodeX."
    per_node = {}
    for match in re.finditer(r"\[DONE\] TASK-\d+ finished by (\w+)\.", logs):
        node = match.group(1)
        per_node[node] = per_node.get(node, 0) + 1

    # Count max in-flight observed per node from [BALANCER] lines
    # (node_load values after assignment -- tells us if cap was respected)
    cap_violations = re.findall(r"node_load=(\d+)", logs)
    max_observed_load = max((int(v) for v in cap_violations), default=0)

    print()
    print("=" * 62)
    print("TEST CASE 4 — Load Balancing (Least-Connections Algorithm)")
    print("=" * 62)
    print(f"  Total completed tasks: {total_completed}")
    print(f"  Completions per node: {per_node}")
    print()

    passed = True

    # 1. All 3 nodes must have received and completed tasks
    for name in ("nodeA", "nodeB", "nodeC"):
        count = per_node.get(name, 0)
        if count >= 4:
            print(f"  PASS  {name} completed {count} tasks.")
        else:
            print(f"  FAIL  {name} only completed {count} tasks (need >= 4).")
            passed = False

    # 2. Distribution must be reasonably fair (no node gets 2x another's share)
    if per_node:
        min_tasks = min(per_node.values())
        max_tasks = max(per_node.values())
        ratio = max_tasks / min_tasks if min_tasks > 0 else float("inf")
        if ratio <= 2.0:
            print(
                f"  PASS  Load distribution is fair "
                f"(max/min ratio = {ratio:.2f}, threshold = 2.0)."
            )
        else:
            print(
                f"  FAIL  Load distribution is uneven "
                f"(max={max_tasks}, min={min_tasks}, ratio={ratio:.2f} > 2.0)."
            )
            passed = False

    # 3. The per-node cap (MAX_IN_FLIGHT=1) must never have been exceeded.
    #    [BALANCER] lines log the load AFTER incrementing, so valid values are 1.
    #    A value of 2+ means the balancer sent a task to a node already at cap.
    if max_observed_load <= 1:
        print(f"  PASS  Per-node cap respected (max observed load = {max_observed_load}).")
    else:
        print(
            f"  FAIL  Cap violated: a node reached load={max_observed_load} "
            f"(MAX_IN_FLIGHT_PER_NODE=1)."
        )
        passed = False

    print()
    print(f"RESULT: {'PASS' if passed else 'FAIL'}")
    print(f"Monitor log saved to: {LOG_FILE}")
    print("=" * 62)


if __name__ == "__main__":
    run()
