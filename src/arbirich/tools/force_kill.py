#!/usr/bin/env python3
"""
Forcibly kills all ArbiRich processes.
This is a last resort when normal shutdown doesn't work.
"""

import os
import subprocess


def get_own_pid():
    """Get the current process ID."""
    return os.getpid()


def force_kill_arbirich_processes():
    """Find and kill all ArbiRich-related processes."""
    own_pid = get_own_pid()

    # Use pgrep to find ArbiRich processes without including ourselves
    try:
        # Find Python processes with "arbirich" in the command line
        # excluding our own PID
        cmd = f"pgrep -f arbirich | grep -v {own_pid}"
        process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
        output, _ = process.communicate()

        # Parse PIDs
        pids = [int(pid) for pid in output.decode().strip().split("\n") if pid]

        if not pids:
            print("No ArbiRich processes found.")
            return

        # Write PIDs to a temp file so we can kill them from a shell script
        # This prevents the Python process from killing itself
        with open("/tmp/arbirich_pids.txt", "w") as f:
            for pid in pids:
                f.write(f"{pid}\n")

        # Create a shell script that will kill the processes
        kill_script = """#!/bin/bash
echo "Force killing ArbiRich processes..."
while read pid; do
    if [ -n "$pid" ] && kill -0 $pid 2>/dev/null; then
        echo "  Killing PID $pid"
        kill -9 $pid
    fi
done < /tmp/arbirich_pids.txt
echo "Done."
rm /tmp/arbirich_pids.txt
"""

        # Write the shell script
        with open("/tmp/arbirich_kill.sh", "w") as f:
            f.write(kill_script)

        # Make it executable
        os.chmod("/tmp/arbirich_kill.sh", 0o755)

        # Execute it using subprocess and wait for completion
        print(f"Found {len(pids)} ArbiRich-related processes to kill.")
        subprocess.call(["/bin/bash", "/tmp/arbirich_kill.sh"])

        # Clean up
        if os.path.exists("/tmp/arbirich_kill.sh"):
            os.remove("/tmp/arbirich_kill.sh")

    except Exception as e:
        print(f"Error while killing processes: {e}")


if __name__ == "__main__":
    print("Force killing all ArbiRich processes...")
    force_kill_arbirich_processes()
    print("Force kill complete.")
