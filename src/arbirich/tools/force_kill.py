#!/usr/bin/env python3
"""
Forcibly kills all ArbiRich processes.
This is a last resort when normal shutdown doesn't work.
"""

import os
import signal
import subprocess
import time


def find_processes(process_names):
    """Find all processes matching the given names"""
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=True)
        processes = []

        for line in result.stdout.split("\n"):
            # Check if line contains any of the process names
            if any(name.lower() in line.lower() for name in process_names):
                # Skip the ps command itself and this script
                if "force_kill.py" in line or "ps aux" in line:
                    continue

                parts = line.split()
                if len(parts) > 1:
                    try:
                        pid = int(parts[1])
                        cmd = " ".join(parts[10:]) if len(parts) > 10 else ""
                        processes.append((pid, cmd))
                    except (ValueError, IndexError):
                        pass

        return processes
    except Exception as e:
        print(f"Error finding processes: {e}")
        return []


def force_kill():
    """Kill all ArbiRich related processes"""
    # Find all python processes related to ArbiRich
    process_names = ["arbirich", "bytewax", "redis-arbirich"]
    processes = find_processes(process_names)

    if not processes:
        print("No ArbiRich processes found.")
        return

    print(f"Found {len(processes)} ArbiRich-related processes:")
    for pid, cmd in processes:
        print(f"  PID {pid}: {cmd[:100]}...")

    # Try graceful termination first (SIGTERM)
    print("Sending SIGTERM to all processes...")
    for pid, _ in processes:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass  # Process already gone
        except PermissionError:
            print(f"  No permission to kill PID {pid}, try running with sudo")

    # Wait a bit for graceful shutdown
    print("Waiting 3 seconds for graceful termination...")
    time.sleep(3)

    # Check which processes are still running
    still_running = []
    for pid, cmd in processes:
        try:
            # Check if process exists by sending signal 0
            os.kill(pid, 0)
            still_running.append((pid, cmd))
        except ProcessLookupError:
            print(f"  PID {pid} terminated successfully")
        except PermissionError:
            print(f"  No permission to check PID {pid}")

    # Force kill remaining processes (SIGKILL)
    if still_running:
        print(f"{len(still_running)} processes still running, sending SIGKILL...")
        for pid, _ in still_running:
            try:
                os.kill(pid, signal.SIGKILL)
                print(f"  SIGKILL sent to PID {pid}")
            except (ProcessLookupError, PermissionError):
                pass
    else:
        print("All processes terminated successfully.")


if __name__ == "__main__":
    print("Force killing all ArbiRich processes...")
    force_kill()
    print("Force kill complete.")
