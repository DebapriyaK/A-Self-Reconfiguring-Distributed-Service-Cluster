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
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
TASK_ASSIGN_TIMEOUT_SECONDS = float(os.environ.get("TASK_ASSIGN_TIMEOUT_SECONDS", "10.0"))

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", PORT))
sock.settimeout(0.5)

# node_id -> {"last_seen": float, "addr": (host, port), "load": int, "capacity": int}
active_nodes = {}

# task_id -> {"task_id": str, "work_units": int, "retries": int}
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

# task_id -> {"task_id": str, "reason": str, "failed_at": float}
failed_tasks = []

task_counter = 0
last_task_generated_at = 0.0
last_liveness_check_at = 0.0
last_summary_at = 0.0

monitor_start_at = time.time()


def now() -> float:
    return time.time()


def log(msg):
    elapsed = time.time() - monitor_start_at
    print(f"[t={elapsed:7.2f}s] {msg}")


def send_json(addr, payload):
    sock.sendto(json.dumps(payload).encode("utf-8"), addr)


def snapshot_state():
    state = {
        "active_nodes": active_nodes,
        "pending_count": len(pending_tasks),
        "in_flight_count": len(in_flight_tasks),
        "completed_count": len(completed_tasks),
        "failed_count": len(failed_tasks),
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
        "retries": 0,
    }
    pending_tasks.append(task)
    log(
        f"[INGRESS] Received new workload {task['task_id']} "
        f"(work_units={task['work_units']}). Queue={len(pending_tasks)}"
    )


def least_loaded_alive_node():
    """
    Least-Connections algorithm: return the node with the lowest current load
    that has not yet hit its per-node capacity.

    Each node advertises its own capacity via heartbeat (NODE_CAPACITY env var).
    Falls back to MAX_IN_FLIGHT_PER_NODE for nodes that haven't reported capacity.

    Sorting by (load, node_id) gives deterministic tie-breaking -- among
    equally loaded nodes the alphabetically first name wins.
    """
    candidates = [
        (node_id, node_state["load"])
        for node_id, node_state in active_nodes.items()
        if node_state["load"] < min(node_state.get("capacity", MAX_IN_FLIGHT_PER_NODE), MAX_IN_FLIGHT_PER_NODE)
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
            "units_done": 0,
        }
        send_json(
            node_state["addr"],
            {"type": "TASK_ASSIGN", "task_id": task["task_id"], "work_units": task["work_units"]},
        )
        log(
            f"[BALANCER] Assigned {task['task_id']} to {node_id} "
            f"(work_units={task['work_units']}, node_load={node_state['load']}, "
            f"queue={len(pending_tasks)})"
        )


def fail_node(node_id, graceful=False):
    """
    Called when a node's heartbeat times out (graceful=False) or the node
    sends a LEAVING message (graceful=True).

    For every task in-flight on the dead/leaving node:
      - Read the checkpoint (units_done).
      - Compute remaining = original_work_units - units_done.
      - If remaining > 0 and retries < MAX_RETRIES: requeue with remaining units.
      - If remaining > 0 and retries >= MAX_RETRIES: mark permanently failed.
      - If remaining == 0: all units done but TASK_DONE was lost. Count as complete.
    """
    if graceful:
        log(f"[LEAVING] Node {node_id} sent graceful shutdown. Reassigning tasks immediately.")
    else:
        log(f"[FAILURE] Node {node_id} timed out. Starting autonomous reconfiguration.")

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
        retries = original_task.get("retries", 0)

        if remaining <= 0:
            completed_tasks.append({
                "task_id": task_id,
                "node_id": node_id,
                "finished_at": now(),
            })
            log(
                f"[RECOVERY] {task_id}: all {original_task['work_units']} units were done "
                f"on {node_id} before crash. Marking complete."
            )
        elif retries >= MAX_RETRIES:
            failed_tasks.append({
                "task_id": task_id,
                "reason": f"exceeded {MAX_RETRIES} retries",
                "failed_at": now(),
            })
            log(
                f"[FAILED] {task_id}: exceeded {MAX_RETRIES} retries. Task permanently failed."
            )
        else:
            partial_task = {
                "task_id": task_id,
                "work_units": remaining,
                "retries": retries + 1,
            }
            pending_tasks.appendleft(partial_task)
            log(
                f"[RECOVERY] Requeued {task_id} from failed node {node_id}. "
                f"({units_done}/{original_task['work_units']} units done, "
                f"{remaining} remaining, retry {retries + 1}/{MAX_RETRIES})"
            )

    del active_nodes[node_id]
    log(f"[RECONFIGURED] Cluster size={len(active_nodes)}. Queue={len(pending_tasks)}.")


def check_liveness():
    current = now()
    dead_nodes = [
        node_id
        for node_id, node_state in active_nodes.items()
        if current - node_state["last_seen"] > HEARTBEAT_TIMEOUT_SECONDS
    ]
    for node_id in dead_nodes:
        fail_node(node_id)

    # Detect TASK_ASSIGN packets silently dropped by UDP:
    # A task in-flight with zero progress for longer than TASK_ASSIGN_TIMEOUT_SECONDS
    # likely never arrived at the node — resend by requeuing.
    stuck_task_ids = [
        task_id
        for task_id, ts in in_flight_tasks.items()
        if ts["units_done"] == 0
        and ts["node_id"] in active_nodes
        and current - ts["assigned_at"] > TASK_ASSIGN_TIMEOUT_SECONDS
    ]
    for task_id in stuck_task_ids:
        task_state = in_flight_tasks.pop(task_id)
        node_id = task_state["node_id"]
        if node_id in active_nodes:
            active_nodes[node_id]["load"] = max(0, active_nodes[node_id]["load"] - 1)
        original_task = task_state["task"]
        retries = original_task.get("retries", 0)
        if retries >= MAX_RETRIES:
            failed_tasks.append({
                "task_id": task_id,
                "reason": "assign timeout, max retries exceeded",
                "failed_at": now(),
            })
            log(f"[FAILED] {task_id}: TASK_ASSIGN timed out after {MAX_RETRIES} retries.")
        else:
            retry_task = {
                "task_id": task_id,
                "work_units": original_task["work_units"],
                "retries": retries + 1,
            }
            pending_tasks.appendleft(retry_task)
            log(
                f"[ASSIGN-TIMEOUT] {task_id} on {node_id}: no progress after "
                f"{TASK_ASSIGN_TIMEOUT_SECONDS}s. Requeuing (retry {retries + 1}/{MAX_RETRIES})."
            )


def handle_message(raw_data, addr):
    message = json.loads(raw_data.decode("utf-8"))
    msg_type = message.get("type")
    current = now()

    if msg_type == "HEARTBEAT":
        node_id = message["node_id"]
        observed_load = int(message.get("load", 0))
        capacity = int(message.get("capacity", MAX_IN_FLIGHT_PER_NODE))
        if node_id not in active_nodes:
            active_nodes[node_id] = {
                "last_seen": current,
                "addr": addr,
                "load": observed_load,
                "capacity": capacity,
            }
            log(f"[JOINED] Node {node_id} joined (capacity={capacity}). Active nodes={len(active_nodes)}")
        else:
            node_state = active_nodes[node_id]
            node_state["last_seen"] = current
            node_state["addr"] = addr
            # Never let a heartbeat decrease the tracked load. A stale heartbeat
            # (sent before the node received a TASK_ASSIGN packet) would report a
            # lower load than reality, which could cause the monitor to assign a
            # second task to a node already at capacity. Taking max() prevents the
            # rollback while still allowing upward correction if the monitor somehow
            # underestimated (e.g. a TASK_DONE message was lost).
            node_state["load"] = max(node_state["load"], observed_load)
            node_state["capacity"] = capacity
        return

    if msg_type == "LEAVING":
        node_id = message["node_id"]
        if node_id in active_nodes:
            fail_node(node_id, graceful=True)
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
            log(
                f"[CHECKPOINT] {task_id} on {node_id}: "
                f"{units_done}/{total} units done."
            )
        return

    if msg_type == "TASK_DONE":
        node_id = message["node_id"]
        task_id = message["task_id"]
        if task_id not in in_flight_tasks:
            log(f"[WARN] Completion for unknown task {task_id} from {node_id}. Ignoring.")
            return
        task_state = in_flight_tasks.pop(task_id)
        if node_id in active_nodes:
            active_nodes[node_id]["load"] = max(0, active_nodes[node_id]["load"] - 1)
            active_nodes[node_id]["last_seen"] = current
        completed_tasks.append({"task_id": task_id, "node_id": node_id, "finished_at": current})
        log(
            f"[DONE] {task_id} finished by {node_id}. "
            f"Completed={len(completed_tasks)} InFlight={len(in_flight_tasks)}"
        )
        return

    log(f"[WARN] Unknown message type {msg_type} from {addr}.")


log("--- SELF-RECONFIGURING CLUSTER MONITOR ONLINE ---")
log(
    f"[CONFIG] heartbeat_timeout={HEARTBEAT_TIMEOUT_SECONDS}s "
    f"task_interval={TASK_GENERATION_INTERVAL_SECONDS}s "
    f"max_in_flight_per_node={MAX_IN_FLIGHT_PER_NODE} "
    f"max_retries={MAX_RETRIES}"
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
        log(
            f"[STATUS] nodes={len(active_nodes)} queue={len(pending_tasks)} "
            f"in_flight={len(in_flight_tasks)} completed={len(completed_tasks)} "
            f"failed={len(failed_tasks)}"
        )
        last_summary_at = current
