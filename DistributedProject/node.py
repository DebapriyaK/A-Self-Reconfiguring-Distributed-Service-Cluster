import json
import os
import socket
import time

node_name = os.environ.get("HOSTNAME", "Unknown_Node")
monitor_host = os.environ.get("MONITOR_HOST", "monitor")
monitor_port = int(os.environ.get("MONITOR_PORT", "5005"))
heartbeat_interval_seconds = float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "1.0"))
work_unit_duration_seconds = float(os.environ.get("WORK_UNIT_DURATION_SECONDS", "1.3"))
crash_after_seconds = float(os.environ.get("CRASH_AFTER_SECONDS", "0"))

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("0.0.0.0", 0))
sock.settimeout(0.2)

# task_id -> {total_units, units_done, unit_finish_at}
# Each task is processed one work-unit at a time.
# After every unit we send a TASK_PROGRESS checkpoint to the monitor.
# This means if this node crashes mid-task, the monitor knows exactly how
# many units were completed and only requeues the REMAINING work.
active_tasks = {}

last_heartbeat_at = 0.0
started_at = time.time()


def send_json(payload):
    sock.sendto(json.dumps(payload).encode("utf-8"), (monitor_host, monitor_port))


def send_heartbeat():
    send_json({"type": "HEARTBEAT", "node_id": node_name, "load": len(active_tasks)})


def handle_assignment(message):
    task_id = message["task_id"]
    work_units = int(message.get("work_units", 1))
    active_tasks[task_id] = {
        "total_units": work_units,
        "units_done": 0,
        "unit_finish_at": time.time() + work_unit_duration_seconds,
    }
    print(
        f"[{node_name}] Accepted {task_id} "
        f"(work_units={work_units}, est={work_units * work_unit_duration_seconds:.1f}s, "
        f"load={len(active_tasks)})"
    )


def tick_tasks():
    """
    Process tasks one work-unit at a time.

    Each iteration of the main loop calls this.  When the wall-clock time
    passes a unit's deadline we:
      1. Increment units_done.
      2. Send TASK_PROGRESS to the monitor -- this is the checkpoint.
         If this node dies right now, the monitor knows how far we got
         and will only requeue the remaining units.
      3. If all units are done, send TASK_DONE and remove the task.
      4. Otherwise, schedule the next unit's deadline.
    """
    current = time.time()
    for task_id in list(active_tasks.keys()):
        state = active_tasks[task_id]
        if current < state["unit_finish_at"]:
            continue  # this unit is still in progress

        state["units_done"] += 1
        units_done = state["units_done"]
        total = state["total_units"]

        send_json({
            "type": "TASK_PROGRESS",
            "node_id": node_name,
            "task_id": task_id,
            "units_done": units_done,
        })

        if units_done >= total:
            # All units finished -- task is complete
            del active_tasks[task_id]
            send_json({"type": "TASK_DONE", "node_id": node_name, "task_id": task_id})
            print(f"[{node_name}] Completed {task_id} ({total}/{total} units). load={len(active_tasks)}")
        else:
            # More units remain -- schedule the next one
            state["unit_finish_at"] = current + work_unit_duration_seconds
            print(
                f"[{node_name}] Progress {task_id}: {units_done}/{total} units done. "
                f"load={len(active_tasks)}"
            )


print(f"[{node_name}] Node online. Sending heartbeats to {monitor_host}:{monitor_port}")

while True:
    current = time.time()

    # Simulated crash -- used by chaos node and test cases
    if crash_after_seconds > 0 and (current - started_at) >= crash_after_seconds:
        print(f"[{node_name}] Simulated crash triggered after {crash_after_seconds}s.")
        os._exit(1)

    if current - last_heartbeat_at >= heartbeat_interval_seconds:
        send_heartbeat()
        last_heartbeat_at = current

    try:
        data, _ = sock.recvfrom(4096)
        message = json.loads(data.decode("utf-8"))
        if message.get("type") == "TASK_ASSIGN":
            handle_assignment(message)
    except socket.timeout:
        pass

    tick_tasks()
