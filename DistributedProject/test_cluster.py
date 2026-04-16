import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path


ROOT = Path(__file__).parent
STATE_FILE = ROOT / "test_cluster_state.json"
LOG_FILE = ROOT / "test_cluster_monitor.log"
PORT = "5011"


def stream_output(process, sink):
    for line in iter(process.stdout.readline, ""):
        sink.append(line)
    process.stdout.close()


def start_process(command, env):
    return subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def run_test():
    if STATE_FILE.exists():
        STATE_FILE.unlink()
    if LOG_FILE.exists():
        LOG_FILE.unlink()

    base_env = os.environ.copy()
    base_env["PYTHONUNBUFFERED"] = "1"
    base_env["MONITOR_PORT"] = PORT
    base_env["STATE_SNAPSHOT_FILE"] = str(STATE_FILE)
    base_env["TASK_GENERATION_INTERVAL_SECONDS"] = "0.7"
    base_env["HEARTBEAT_TIMEOUT_SECONDS"] = "4.0"
    base_env["MAX_IN_FLIGHT_PER_NODE"] = "2"

    monitor_lines = []
    monitor = start_process([sys.executable, "-u", "monitor.py"], base_env)
    monitor_reader = threading.Thread(target=stream_output, args=(monitor, monitor_lines), daemon=True)
    monitor_reader.start()

    node_lines = {}
    node_readers = []
    nodes = []
    for name in ("nodeA", "nodeB", "nodeC"):
        env = base_env.copy()
        env["HOSTNAME"] = name
        env["MONITOR_HOST"] = "127.0.0.1"
        env["WORK_UNIT_DURATION_SECONDS"] = "1.0"
        if name == "nodeC":
            env["CRASH_AFTER_SECONDS"] = "5"
        node = start_process([sys.executable, "-u", "node.py"], env)
        nodes.append(node)
        node_lines[name] = []
        reader = threading.Thread(target=stream_output, args=(node, node_lines[name]), daemon=True)
        reader.start()
        node_readers.append(reader)

    # Allow the cluster to run, fail a node, and recover.
    time.sleep(16)

    for node in nodes:
        node.terminate()
    monitor.terminate()

    for node in nodes:
        try:
            node.wait(timeout=3)
        except subprocess.TimeoutExpired:
            node.kill()
    try:
        monitor.wait(timeout=3)
    except subprocess.TimeoutExpired:
        monitor.kill()

    LOG_FILE.write_text("".join(monitor_lines), encoding="utf-8")

    if not STATE_FILE.exists():
        raise RuntimeError("State snapshot file not produced by monitor.")

    state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
    completed_count = int(state["completed_count"])
    active_nodes = state["active_nodes"]

    logs = "".join(monitor_lines)
    if "[JOINED] Node nodeA joined." not in logs or "[JOINED] Node nodeB joined." not in logs:
        node_debug = {
            "nodeA": "".join(node_lines["nodeA"]),
            "nodeB": "".join(node_lines["nodeB"]),
            "nodeC": "".join(node_lines["nodeC"]),
        }
        raise RuntimeError(f"Nodes failed to join monitor.\nNode logs: {node_debug}")
    if "[FAILURE] Node nodeC timed out." not in logs and "[FAILURE] Node Unknown_Node timed out." not in logs:
        raise RuntimeError("Expected node failure was not detected.")
    if "[RECOVERY] Requeued" not in logs:
        raise RuntimeError("Expected task requeue after node failure was not detected.")
    if completed_count < 8:
        raise RuntimeError(f"Expected at least 8 completed tasks, observed {completed_count}.")
    if len(active_nodes) < 2:
        raise RuntimeError(f"Expected at least 2 active nodes after failover, observed {len(active_nodes)}.")

    print("PASS: autonomous failure detection, reconfiguration, and adaptive task completion verified.")
    print(f"Completed tasks: {completed_count}")
    print(f"Active nodes after failover: {len(active_nodes)}")
    print(f"Monitor log saved to: {LOG_FILE}")


if __name__ == "__main__":
    run_test()
