#!/usr/bin/env python3
"""
Emergency abort script that kills all Python processes, including the entire process group.
This should only be used when all other shutdown methods fail.
"""

import os
import signal
import subprocess
import sys
import time


def find_process_group_leaders():
    """Find all process group leaders that might be related to our app"""
    try:
        # Run ps command to get all processes
        result = subprocess.run(["ps", "-eo", "pid,pgid,ppid,command"], capture_output=True, text=True, check=True)

        # Store potential group leaders (pid == pgid)
        group_leaders = {}

        # Process each line
        for line in result.stdout.split("\n")[1:]:  # Skip header
            if not line.strip():
                continue

            parts = line.split(None, 3)
            if len(parts) < 4:
                continue

            pid, pgid, ppid, cmd = parts

            try:
                pid = int(pid)
                pgid = int(pgid)
                ppid = int(ppid)

                # If this is a group leader (pid == pgid) and it's a Python process
                # potentially related to ArbiRich
                if pid == pgid and ("python" in cmd) and ("arbirich" in cmd.lower() or "bytewax" in cmd.lower()):
                    # Skip our own process or direct parent
                    if pid == os.getpid() or pid == os.getppid():
                        continue

                    group_leaders[pgid] = cmd
            except ValueError:
                continue

        return group_leaders

    except Exception as e:
        print(f"Error finding process groups: {e}")
        return {}


def kill_process_groups():
    """Kill all process groups that might be related to our app"""
    group_leaders = find_process_group_leaders()

    if not group_leaders:
        print("No ArbiRich process groups found!")
        return 0

    print(f"Found {len(group_leaders)} potential ArbiRich process groups:")
    for pgid, cmd in group_leaders.items():
        print(f"  Process Group {pgid}: {cmd[:80]}...")

    # First try graceful termination
    print("\nSending SIGTERM to all process groups...")
    for pgid in group_leaders.keys():
        try:
            os.killpg(pgid, signal.SIGTERM)
            print(f"  Sent SIGTERM to process group {pgid}")
        except (ProcessLookupError, PermissionError) as e:
            print(f"  Error sending SIGTERM to group {pgid}: {e}")

    # Wait a few seconds
    print("\nWaiting 5 seconds for processes to exit...")
    time.sleep(5)

    # Check which groups are still alive
    remaining_groups = {}
    for pgid in group_leaders.keys():
        try:
            # Check if group exists by trying to send signal 0
            os.killpg(pgid, 0)
            remaining_groups[pgid] = group_leaders[pgid]
        except (ProcessLookupError, PermissionError):
            print(f"  Process group {pgid} terminated successfully")

    # Use SIGKILL for remaining groups
    if remaining_groups:
        print(f"\n{len(remaining_groups)} process groups still running, sending SIGKILL...")
        for pgid in remaining_groups.keys():
            try:
                os.killpg(pgid, signal.SIGKILL)
                print(f"  Sent SIGKILL to process group {pgid}")
            except (ProcessLookupError, PermissionError) as e:
                print(f"  Error sending SIGKILL to group {pgid}: {e}")

        # Wait a moment and verify
        time.sleep(2)
        for pgid in remaining_groups.keys():
            try:
                os.killpg(pgid, 0)
                print(f"  WARNING: Process group {pgid} still exists!")
            except (ProcessLookupError, PermissionError):
                print(f"  Confirmed process group {pgid} is terminated")

    return len(group_leaders)


def kill_all_python():
    """Kill all Python processes (last resort)"""
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=True)

        killed = 0
        for line in result.stdout.split("\n"):
            if "python" in line and "arbirich" in line.lower():
                # Don't kill this script
                if "emergency_abort.py" in line:
                    continue

                parts = line.split()
                if len(parts) > 1:
                    try:
                        pid = int(parts[1])
                        # Skip our own process
                        if pid == os.getpid() or pid == os.getppid():
                            continue

                        print(f"Sending SIGKILL to Python process {pid}")
                        os.kill(pid, signal.SIGKILL)
                        killed += 1
                    except (ValueError, ProcessLookupError, PermissionError):
                        pass

        return killed
    except Exception as e:
        print(f"Error in kill_all_python: {e}")
        return 0


if __name__ == "__main__":
    print("==== EMERGENCY ABORT SCRIPT ====")
    print("This will forcibly kill all ArbiRich-related processes!")
    print("Use with caution - last resort when normal shutdown fails.")

    answer = input("Continue? (y/N): ")
    if answer.lower() not in ("y", "yes"):
        print("Aborted.")
        sys.exit(0)

    print("\n1. Attempting to kill process groups...")
    kill_process_groups()

    print("\n2. Killing remaining Python processes...")
    killed = kill_all_python()

    print(f"\nAbort complete! Killed at least {killed} processes.")
    print("If you're still having issues, try rebooting your system.")
