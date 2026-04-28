"""
Free-Running Cluster Demo
=========================
Starts a monitor and N worker nodes with randomized parameters.
No crashes are pre-programmed -- you trigger them yourself.

Usage:
    python demo.py            # 4 nodes (default)
    python demo.py 6          # 6 nodes

Controls (type and press Enter while it's running):
    k  -- kill a random live node  (hard crash)
    a  -- add a new node to the cluster
    q  -- quit cleanly
    Ctrl+C -- force quit
"""

import os
import queue
import random
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).parent
PORT = "5200"
HEARTBEAT_TIMEOUT = 4.0

processes = {}       # name -> Popen
node_counter = 0
cmd_queue = queue.Queue()
running = True


# ---------------------------------------------------------------------------
# Output streaming
# ---------------------------------------------------------------------------

def stream_output(proc, label):
    """Read a process's stdout line-by-line and print with a label prefix."""
    for line in proc.stdout:
        line = line.rstrip()
        if line:
            print(f"  [{label}] {line}", flush=True)


# ---------------------------------------------------------------------------
# Node lifecycle
# ---------------------------------------------------------------------------

def _spawn_node(name, base_env):
    """Start a node.py process with randomized per-node parameters."""
    env = base_env.copy()
    env["HOSTNAME"] = name
    env["MONITOR_HOST"] = "127.0.0.1"
    env["WORK_UNIT_DURATION_SECONDS"] = f"{random.uniform(0.8, 2.5):.1f}"
    env["NODE_CAPACITY"] = str(random.randint(1, 3))

    proc = subprocess.Popen(
        [sys.executable, "-u", "node.py"],
        env=env,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    threading.Thread(target=stream_output, args=(proc, name), daemon=True).start()
    return proc


def _node_name(counter):
    if counter <= 26:
        return f"node{chr(64 + counter)}"   # nodeA … nodeZ
    return f"node{counter}"                  # node27, node28, …


def add_node(base_env):
    global node_counter
    node_counter += 1
    name = _node_name(node_counter)
    proc = _spawn_node(name, base_env)
    processes[name] = proc
    print(f"\n  ++ Added {name} to cluster\n", flush=True)
    threading.Timer(0.4, _print_prompt).start()


def kill_random_node():
    live = [n for n, p in processes.items() if n != "monitor" and p.poll() is None]
    if not live:
        print("  No live worker nodes to kill.", flush=True)
        return
    victim = random.choice(live)
    processes[victim].kill()
    print(f"\n  !! Hard-crashed {victim} -- monitor will detect via heartbeat timeout !!\n",
          flush=True)
    threading.Timer(0.4, _print_prompt).start()


# ---------------------------------------------------------------------------
# Keyboard input thread
# ---------------------------------------------------------------------------

def _input_thread():
    while running:
        try:
            line = input()
            cmd_queue.put(line.strip().lower())
        except EOFError:
            break


def _print_prompt():
    print("  [k=kill random  a=add node  q=quit]", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    global running, node_counter

    num_nodes = int(sys.argv[1]) if len(sys.argv) > 1 else 4

    base_env = os.environ.copy()
    base_env.update({
        "PYTHONUNBUFFERED": "1",
        "MONITOR_PORT": PORT,
        "TASK_GENERATION_INTERVAL_SECONDS": "1.5",
        "HEARTBEAT_TIMEOUT_SECONDS": str(HEARTBEAT_TIMEOUT),
        "LIVENESS_CHECK_INTERVAL_SECONDS": "1.0",
        "MAX_IN_FLIGHT_PER_NODE": "3",
        "MAX_RETRIES": "3",
        "STATE_SNAPSHOT_FILE": str(ROOT / "demo_state.json"),
    })

    print()
    print("=" * 60)
    print("  Self-Reconfiguring Cluster  --  Free-Running Demo")
    print("=" * 60)
    print(f"  Nodes    : {num_nodes} (random capacity 1-3 each)")
    print(f"  Port     : {PORT}")
    print(f"  HB timeout : {HEARTBEAT_TIMEOUT}s  |  max_in_flight : 3  |  max_retries : 3")
    print()
    print("  Controls (type + Enter):")
    print("    k  -- kill a random node  (hard crash)")
    print("    a  -- add a new node")
    print("    q  -- quit")
    print("=" * 60)
    print()

    # --- Start monitor ---
    monitor_proc = subprocess.Popen(
        [sys.executable, "-u", "monitor.py"],
        env=base_env,
        cwd=str(ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    processes["monitor"] = monitor_proc
    threading.Thread(target=stream_output, args=(monitor_proc, "MONITOR"), daemon=True).start()
    time.sleep(0.5)

    # --- Start initial nodes ---
    for _ in range(num_nodes):
        node_counter += 1
        name = _node_name(node_counter)
        proc = _spawn_node(name, base_env)
        processes[name] = proc

    print(f"\n  {num_nodes} nodes started. Cluster is live.\n", flush=True)
    _print_prompt()

    # --- Keyboard input thread ---
    threading.Thread(target=_input_thread, daemon=True).start()

    # --- Command loop ---
    try:
        while running:
            try:
                cmd = cmd_queue.get(timeout=0.3)
            except queue.Empty:
                continue

            if cmd == "k":
                kill_random_node()
            elif cmd == "a":
                add_node(base_env)
            elif cmd == "q":
                running = False
            else:
                print("  Unknown command. [k=kill  a=add  q=quit]", flush=True)
    except KeyboardInterrupt:
        pass

    print("\n  Shutting down...", flush=True)
    for proc in processes.values():
        try:
            proc.kill()
        except Exception:
            pass
    print("  All processes stopped.", flush=True)


if __name__ == "__main__":
    main()
