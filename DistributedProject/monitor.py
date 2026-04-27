import json
import os
import socket
import time
from collections import deque

PORT = int(os.environ.get("MONITOR_PORT", "5005"))
HEARTBEAT_TIMEOUT_SECONDS = float(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "6.0"))
LIVENESS_CHECK_INTERVAL_SECONDS = float(os.environ.get("LIVENESS_CHECK_INTERVAL_SECONDS", "1.0"))
TASK_GENERATION_INTERVAL_SECONDS = float(os.environ.get("TASK_GENERATION_INTERVAL_SECONDS", "1.2"))
MAX_IN_FLIGHT_PER_NODE = int(os.environ.get("MAX_IN_FLIGHT_PER_NODE", "2"))
STATE_SNAPSHOT_FILE = os.environ.get("STATE_SNAPSHOT_FILE", "cluster_state.json")

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", PORT))
sock.settimeout(0.5)

# node_id -> {"last_seen": float, "addr": (host, port), "load": int}
active_nodes = {}

# task_id -> {"task_id": str, "work_units": int}
pending_tasks = deque()

# task_id -> {
#   "task":        original task dict,
#   "node_id":     str,
#   "assigned_at": float,
#   "units_done":  int   <-- checkpoint: updated by TASK_PROGRESS messages
# }
in_flight_tasks = {}

# task_id -> {"task_id": str, "node_id": str, "finished_at": float}
completed_tasks = []

task_counter = 0
last_task_generated_at = 0.0
last_liveness_check_at = 0.0
last_summary_at = 0.0


def now() -> float:
    return time.time()


def send_json(addr, payload):
    sock.sendto(json.dumps(payload).encode("utf-8"), addr)


def snapshot_state():
    state = {
        "active_nodes": active_nodes,
        "pending_count": len(pending_tasks),
        "in_flight_count": len(in_flight_tasks),
        "completed_count": len(completed_tasks),
        "timestamp": now(),
    }
    with open(STATE_SNAPSHOT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def enqueue_task():
    global task_counter
    task_counter += 1
    task = {
        "task_id": f"TASK-{task_counter:04d}",
        "work_units": 1 + (task_counter % 4),
    }
    pending_tasks.append(task)
    print(
        f"[INGRESS] Received new workload {task['task_id']} "
        f"(work_units={task['work_units']}). Queue={len(pending_tasks)}"
    )


def least_loaded_alive_node():
    """
    Least-Connections algorithm: return the node with the lowest current load
    that has not yet hit the per-node in-flight cap.

    Sorting by (load, node_id) gives deterministic tie-breaking -- among
    equally loaded nodes the alphabetically first name wins.  This keeps
    the assignment order predictable for tests.
    """
    candidates = [
        (node_id, node_state["load"])
        for node_id, node_state in active_nodes.items()
        if node_state["load"] < MAX_IN_FLIGHT_PER_NODE
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda entry: (entry[1], entry[0]))
    return candidates[0][0]


def assign_tasks():
    while pending_tasks:
        node_id = least_loaded_alive_node()
        if node_id is None:
            return  # all nodes at capacity or none alive; leave tasks in queue

        task = pending_tasks.popleft()
        node_state = active_nodes[node_id]
        node_state["load"] += 1
        in_flight_tasks[task["task_id"]] = {
            "task": task,
            "node_id": node_id,
            "assigned_at": now(),
            "units_done": 0,      # checkpoint counter -- updated by TASK_PROGRESS
        }
        send_json(
            node_state["addr"],
            {"type": "TASK_ASSIGN", "task_id": task["task_id"], "work_units": task["work_units"]},
        )
        print(
            f"[BALANCER] Assigned {task['task_id']} to {node_id} "
            f"(work_units={task['work_units']}, node_load={node_state['load']}, "
            f"queue={len(pending_tasks)})"
        )


def fail_node(node_id):
    """
    Called when a node's heartbeat has not been seen for HEARTBEAT_TIMEOUT_SECONDS.

    For every task that was in-flight on the dead node:
      - Read the checkpoint (units_done) that was last reported via TASK_PROGRESS.
      - Compute remaining = original_work_units - units_done.
      - If remaining > 0: requeue with only the remaining units so the next
        node does NOT redo work that was already completed (checkpointing).
      - If remaining == 0: the node finished all units but the TASK_DONE
        message was lost in the crash.  Count it as completed anyway.
    """
    print(f"[FAILURE] Node {node_id} timed out. Starting autonomous reconfiguration.")

    stranded_task_ids = [
        task_id
        for task_id, task_state in in_flight_tasks.items()
        if task_state["node_id"] == node_id
    ]

    for task_id in stranded_task_ids:
        task_state = in_flight_tasks.pop(task_id)
        original_task = task_state["task"]
        units_done = task_state["units_done"]
        remaining = original_task["work_units"] - units_done

        if remaining <= 0:
            # Edge case: all units were finished but TASK_DONE was never received
            # (node crashed between completing the last unit and sending TASK_DONE).
            completed_tasks.append({
                "task_id": task_id,
                "node_id": node_id,
                "finished_at": now(),
            })
            print(
                f"[RECOVERY] {task_id}: all {original_task['work_units']} units were done "
                f"on {node_id} before crash. Marking complete."
            )
        else:
            # Build a partial task -- only the remaining work units
            partial_task = {
                "task_id": task_id,
                "work_units": remaining,
            }
            pending_tasks.appendleft(partial_task)  # priority re-insertion at front
            print(
                f"[RECOVERY] Requeued {task_id} from failed node {node_id}. "
                f"({units_done}/{original_task['work_units']} units done, "
                f"{remaining} remaining)"
            )

    del active_nodes[node_id]
    print(f"[RECONFIGURED] Cluster size={len(active_nodes)}. Queue={len(pending_tasks)}.")


def check_liveness():
    current = now()
    dead_nodes = [
        node_id
        for node_id, node_state in active_nodes.items()
        if current - node_state["last_seen"] > HEARTBEAT_TIMEOUT_SECONDS
    ]
    for node_id in dead_nodes:
        fail_node(node_id)


def handle_message(raw_data, addr):
    message = json.loads(raw_data.decode("utf-8"))
    msg_type = message.get("type")
    current = now()

    if msg_type == "HEARTBEAT":
        node_id = message["node_id"]
        observed_load = int(message.get("load", 0))
        was_new = node_id not in active_nodes
        active_nodes[node_id] = {
            "last_seen": current,
            "addr": addr,
            "load": observed_load,
        }
        if was_new:
            print(f"[JOINED] Node {node_id} joined. Active nodes={len(active_nodes)}")
        return

    if msg_type == "TASK_PROGRESS":
        task_id = message["task_id"]
        node_id = message["node_id"]
        units_done = int(message.get("units_done", 0))
        if (
            task_id in in_flight_tasks
            and in_flight_tasks[task_id]["node_id"] == node_id
        ):
            in_flight_tasks[task_id]["units_done"] = units_done
            total = in_flight_tasks[task_id]["task"]["work_units"]
            print(
                f"[CHECKPOINT] {task_id} on {node_id}: "
                f"{units_done}/{total} units done."
            )
        return

    if msg_type == "TASK_DONE":
        node_id = message["node_id"]
        task_id = message["task_id"]
        if task_id not in in_flight_tasks:
            print(f"[WARN] Completion for unknown task {task_id} from {node_id}. Ignoring.")
            return
        task_state = in_flight_tasks.pop(task_id)
        if node_id in active_nodes:
            active_nodes[node_id]["load"] = max(0, active_nodes[node_id]["load"] - 1)
            active_nodes[node_id]["last_seen"] = current
        completed_tasks.append({"task_id": task_id, "node_id": node_id, "finished_at": current})
        print(
            f"[DONE] {task_id} finished by {node_id}. "
            f"Completed={len(completed_tasks)} InFlight={len(in_flight_tasks)}"
        )
        return

    print(f"[WARN] Unknown message type {msg_type} from {addr}.")


print("--- SELF-RECONFIGURING CLUSTER MONITOR ONLINE ---")
print(
    f"[CONFIG] heartbeat_timeout={HEARTBEAT_TIMEOUT_SECONDS}s "
    f"task_interval={TASK_GENERATION_INTERVAL_SECONDS}s "
    f"max_in_flight_per_node={MAX_IN_FLIGHT_PER_NODE}"
)

while True:
    current = now()

    try:
        data, addr = sock.recvfrom(4096)
        handle_message(data, addr)
    except socket.timeout:
        pass
    except ConnectionResetError:
        # Windows UDP raises this after sending to a dead peer.
        # Swallow it so the monitor stays alive and liveness logic runs.
        pass

    if current - last_task_generated_at >= TASK_GENERATION_INTERVAL_SECONDS:
        enqueue_task()
        last_task_generated_at = current

    if current - last_liveness_check_at >= LIVENESS_CHECK_INTERVAL_SECONDS:
        check_liveness()
        last_liveness_check_at = current

    assign_tasks()

    if current - last_summary_at >= 4.0:
        snapshot_state()
        print(
            f"[STATUS] nodes={len(active_nodes)} queue={len(pending_tasks)} "
            f"in_flight={len(in_flight_tasks)} completed={len(completed_tasks)}"
        )
        last_summary_at = current
