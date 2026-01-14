import sys
import subprocess
from pathlib import Path


def main():
    if len(sys.argv) != 2:
        print("Usage: python launcher.py <num_instances>")
        sys.exit(1)

    try:
        num_instances = int(sys.argv[1])
        if num_instances <= 0:
            raise ValueError
    except ValueError:
        print("num_instances must be a positive integer")
        sys.exit(1)

    base_dir = Path(__file__).parent.resolve()
    script = base_dir / "run-worker-wc.sh"

    if not script.exists():
        print(f"Script not found: {script}")
        sys.exit(1)

    processes = []

    for i in range(num_instances):
        log_file = base_dir / f"worker_{i}.log"
        log_handle = open(log_file, "w")

        p = subprocess.Popen(
            [str(script)],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            cwd=base_dir,
        )

        processes.append((p, log_handle))
        print(f"Started worker {i}, PID={p.pid}")

    # Optional: wait for all scripts
    for p, log_handle in processes:
        p.wait()
        log_handle.close()

    print("All scripts finished.")


if __name__ == "__main__":
    main()
