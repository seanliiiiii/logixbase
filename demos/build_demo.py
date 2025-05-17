import subprocess
from pathlib import Path

LOCAL_DIR = Path(__file__).parent


if __name__ == "__main__":
    # Execute in cmd
    command = ["python", str(LOCAL_DIR.joinpath("build_demo_exec.py")), "build_ext", "--inplace"]
    process = subprocess.Popen(command)
    process.wait()
