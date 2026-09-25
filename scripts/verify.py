"""Validate Compose and run the synthetic integration check on a Docker host."""
from pathlib import Path
import shutil
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def run(*args):
    subprocess.run(args, cwd=ROOT, check=True)


def main():
    if not shutil.which("docker"):
        sys.exit("Docker is not installed or not on PATH. Start Docker Desktop / Docker Engine first.")
    if not (ROOT / ".env").is_file():
        sys.exit("Missing .env. Run python scripts/setup_env.py first.")
    run("docker", "compose", "config", "--quiet")
    run("docker", "compose", "ps", "--all")
    # Run this only after up --build has finished and while the DAG is idle.
    run("docker", "compose", "--profile", "tools", "run", "--rm", "spark-job")
    # A second run checks overwrite/idempotent behavior for the demo snapshot.
    run("docker", "compose", "--profile", "tools", "run", "--rm", "spark-job")
    run("docker", "compose", "exec", "-T", "airflow-scheduler",
        "airflow", "dags", "list-import-errors")
    print("Two demo runs succeeded. Also check that Airflow reports no DAG import errors.")


if __name__ == "__main__":
    main()
