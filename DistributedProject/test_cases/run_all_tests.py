"""
Run All Test Cases
==================
Runs TC1 → TC5 sequentially and prints a final summary.
Each test uses a different port so they never collide.

Usage (from the DistributedProject folder):
    python test_cases/run_all_tests.py

Or run any single test on its own:
    python test_cases/test_case_1_single_node_failure.py
    python test_cases/test_case_2_two_nodes_simultaneous_crash.py
    python test_cases/test_case_3_full_outage_and_recovery.py
    python test_cases/test_case_4_load_balancing.py
    python test_cases/test_case_5_partial_task_recovery.py
"""

import importlib.util
import sys
import time
from pathlib import Path

TESTS = [
    ("TC1 — Single Node Failure Mid-Task",             "test_case_1_single_node_failure"),
    ("TC2 — Two Nodes Crash Simultaneously",            "test_case_2_two_nodes_simultaneous_crash"),
    ("TC3 — Full Outage Then New Node Joins",           "test_case_3_full_outage_and_recovery"),
    ("TC4 — Load Balancing Verification",               "test_case_4_load_balancing"),
    ("TC5 — Partial Task Recovery via Checkpointing",   "test_case_5_partial_task_recovery"),
]

HERE = Path(__file__).parent

# Ensure helpers.py is importable when test modules are loaded via importlib
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))


def load_and_run(module_filename):
    path = HERE / f"{module_filename}.py"
    spec = importlib.util.spec_from_file_location(module_filename, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.run()


def main():
    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║     Self-Reconfiguring Cluster — Full Test Suite         ║")
    print("╚══════════════════════════════════════════════════════════╝")
    print()
    print("Tests run sequentially. Total runtime ≈ 2.5 minutes.")
    print()

    results = []
    for label, module in TESTS:
        print(f"▶  Starting {label}")
        print("-" * 62)
        try:
            load_and_run(module)
            results.append((label, "ran"))
        except Exception as exc:
            print(f"  ERROR: {exc}")
            results.append((label, f"ERROR: {exc}"))
        print()
        # Brief pause between tests so OS sockets fully release
        time.sleep(2)

    print()
    print("╔══════════════════════════════════════════════════════════╗")
    print("║                     SUITE SUMMARY                       ║")
    print("╚══════════════════════════════════════════════════════════╝")
    for label, status in results:
        print(f"  {label}  →  {status}")
    print()


if __name__ == "__main__":
    main()
