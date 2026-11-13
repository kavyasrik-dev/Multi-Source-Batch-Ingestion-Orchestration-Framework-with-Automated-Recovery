"""Verify local infrastructure health and named-volume persistence across restart."""

import argparse
import json
import shutil
import subprocess
import time
import uuid

DATA_PATHS = {
    "postgres": "/var/lib/postgresql/data",
    "minio": "/data",
    "prometheus": "/prometheus",
    "grafana": "/var/lib/grafana",
}


def run(*args: str) -> str:
    """Run a checked command without exposing interpolated Compose secrets."""
    return subprocess.run(args, check=True, text=True, stdout=subprocess.PIPE).stdout.strip()


def main() -> None:
    """Start the stack, rerun initialization and verify persistent volume markers."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--compose-file", default="infrastructure/docker/compose.yaml")
    args = parser.parse_args()
    if shutil.which("docker") is None:
        parser.error("Docker is not installed or is not on PATH; install Docker with Compose v2")
    compose = (
        "docker",
        "compose",
        "--env-file",
        args.env_file,
        "-f",
        args.compose_file,
    )
    run(*compose, "config", "--quiet")
    run(*compose, "up", "-d", "--build", "--wait", "--wait-timeout", "240")
    # Both runs must succeed; the second exercises idempotency with existing buckets.
    run(*compose, "run", "--rm", "minio-init")
    run(*compose, "run", "--rm", "minio-init")
    marker = f".persistence-probe-{uuid.uuid4().hex}"
    mounts = {}
    written = []
    try:
        for service, path in DATA_PATHS.items():
            container = run(*compose, "ps", "-q", service)
            details = json.loads(run("docker", "inspect", container))[0]
            volume = next(m for m in details["Mounts"] if m["Destination"] == path)
            if volume["Type"] != "volume":
                raise RuntimeError(f"{service} data is not in a named volume")
            mounts[service] = volume["Name"]
            run(
                *compose,
                "exec",
                "-T",
                service,
                "sh",
                "-ec",
                'printf "%s" "$1" > "$2"',
                "probe",
                marker,
                f"{path}/{marker}",
            )
            written.append((service, path))
        run(*compose, "restart", *DATA_PATHS)
        deadline = time.monotonic() + 240
        while True:
            healthy = True
            for service in DATA_PATHS:
                container = run(*compose, "ps", "-q", service)
                details = json.loads(run("docker", "inspect", container))[0]
                healthy &= details["State"].get("Health", {}).get("Status") == "healthy"
            if healthy:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("Services did not become healthy after restart")
            time.sleep(3)
        for service, path in DATA_PATHS.items():
            container = run(*compose, "ps", "-q", service)
            details = json.loads(run("docker", "inspect", container))[0]
            volume = next(m for m in details["Mounts"] if m["Destination"] == path)
            if volume["Name"] != mounts[service]:
                raise RuntimeError(f"{service} volume changed after restart")
            if run(*compose, "exec", "-T", service, "cat", f"{path}/{marker}") != marker:
                raise RuntimeError(f"{service} data did not survive restart")
        run(*compose, "run", "--rm", "minio-init")
        print("PASS: four healthy services, four buckets, persistent data in four named volumes")
    finally:
        for service, path in written:
            run(*compose, "exec", "-T", service, "rm", "-f", f"{path}/{marker}")


if __name__ == "__main__":
    main()
