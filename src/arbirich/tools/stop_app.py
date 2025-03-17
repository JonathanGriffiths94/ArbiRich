#!/usr/bin/env python3
"""
Stop the ArbiRich application cleanly.
"""

import os
import signal
import subprocess
import sys
import time


def find_main_process():
    """Find the main ArbiRich process"""
    try:
        result = subprocess.run(["ps", "aux"], capture_output=True, text=True, check=True)

        for line in result.stdout.split("\n"):
            # Look for the main Python process running main.py
            if "python" in line and "main.py" in line and "arbirich" in line.lower():
                # Skip this script itself
                if "stop_app.py" in line:
                    continue

                parts = line.split()
                if len(parts) > 1:
                    try:
                        pid = int(parts[1])
                        return pid, line
                    except ValueError:
                        pass

        # If main.py not found, look for any arbirich process
        for line in result.stdout.split("\n"):
            if "python" in line and "arbirich" in line.lower():
                if "stop_app.py" in line:
                    continue

                parts = line.split()
                if len(parts) > 1:
                    try:
                        pid = int(parts[1])
                        return pid, line
                    except ValueError:
                        pass

        return None, None
    except Exception as e:
        print(f"Error finding process: {e}")
        return None, None


def stop_application():
    """Stop the main ArbiRich application process"""
    pid, process_info = find_main_process()

    if not pid:
        print("No ArbiRich process found.")
        return False

    print(f"Found ArbiRich process (PID {pid}):")
    print(f"  {process_info}")

    try:
        # Send SIGTERM for graceful shutdown
        print(f"Sending SIGTERM to process {pid}...")
        os.kill(pid, signal.SIGTERM)

        # Wait up to 15 seconds for process to exit
        max_wait = 15
        print(f"Waiting up to {max_wait} seconds for process to exit...")

        for i in range(max_wait):
            try:
                # Check if process still exists
                os.kill(pid, 0)
                # If we're here, process still exists
                time.sleep(1)
                sys.stdout.write(".")
                sys.stdout.flush()
            except ProcessLookupError:
                # Process has exited
                print("\nProcess exited cleanly!")
                return True

        print("\nProcess didn't exit in time. Sending SIGKILL...")
        os.kill(pid, signal.SIGKILL)
        print("SIGKILL sent.")
        return True

    except ProcessLookupError:
        print("Process not found or already stopped.")
        return True
    except PermissionError:
        print("No permission to kill this process. Try running with sudo.")
        return False
    except Exception as e:
        print(f"Error stopping process: {e}")
        return False


if __name__ == "__main__":
    print("Stopping ArbiRich application...")
    if stop_application():
        print("ArbiRich application stopped.")
    else:
        print("Failed to stop ArbiRich application.")
        print("You can try with force kill:")
        print("  python -m src.arbirich.tools.force_kill")
        sys.exit(1)
