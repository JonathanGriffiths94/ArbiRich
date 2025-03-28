#!/usr/bin/env python3
"""
Stop the ArbiRich application cleanly.
"""

import os
import signal
import time

import psutil


def find_arbirich_processes(exclude_pid=None):
    """Find ArbiRich processes excluding the current one."""
    arbirich_processes = []

    # Get current process ID to exclude
    current_pid = os.getpid() if exclude_pid is None else exclude_pid

    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            # Skip the current process
            if proc.pid == current_pid:
                continue

            # Look for main.py or ArbiRich mentions in command line
            cmdline = " ".join(proc.cmdline()) if proc.cmdline() else ""
            if "arbirich" in cmdline.lower() and "main.py" in cmdline.lower():
                arbirich_processes.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass

    return arbirich_processes


def stop_arbirich_processes():
    """Stop all ArbiRich processes gracefully."""
    # Get ArbiRich processes
    arbirich_processes = find_arbirich_processes()

    if not arbirich_processes:
        print("No ArbiRich processes found.")
        return

    print(f"Found {len(arbirich_processes)} ArbiRich processes:")
    for proc in arbirich_processes:
        print(f"  PID {proc.pid}: {' '.join(proc.cmdline())[:80]}...")

    # Send SIGTERM signal
    print("Sending SIGTERM to processes...")
    for proc in arbirich_processes:
        try:
            os.kill(proc.pid, signal.SIGTERM)
        except OSError as e:
            print(f"Failed to terminate process {proc.pid}: {e}")

    # Wait a bit for processes to terminate
    print("Waiting for processes to terminate...")
    time.sleep(2)

    # Check if processes still exist
    still_running = [p for p in arbirich_processes if psutil.pid_exists(p.pid)]
    if still_running:
        print(f"{len(still_running)} processes still running.")
    else:
        print("All ArbiRich processes terminated successfully.")


if __name__ == "__main__":
    print("Stopping ArbiRich application...")
    stop_arbirich_processes()
