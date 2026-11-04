"""How pi is launched for a session — port of executors/index.ts.

Two kinds:

* `host` runs pi as a child process of the portal, working directly on the
  mounted workspace directories. Fast and simple; pi has the portal's own
  permissions, so this assumes you trust the tasks you submit. (The JS portal
  prefers pi's in-process SDK here; that SDK is JavaScript, so the Python port
  always uses the RPC child process — one code path for both executors.)
* `container` runs pi inside a throwaway Docker container with only the
  workspace directory mounted, so a task cannot reach the rest of the host.
"""

from __future__ import annotations

import os
import subprocess
from typing import Optional

from .pi_rpc import PiRpcClient

EXECUTOR_KIND = os.environ.get("EXECUTOR", "host")


def _pi_args(opts: dict, session_dir: str) -> list[str]:
    args = ["--mode", "rpc", "--session-dir", session_dir]
    if opts.get("provider"):
        args += ["--provider", opts["provider"]]
    if opts.get("model"):
        args += ["--model", opts["model"]]
    return args


def _pi_env() -> dict:
    env = dict(os.environ)
    # pi writes nothing interactive; make sure it never tries.
    env["CI"] = "1"
    env["TERM"] = "dumb"
    return env


class HostExecutor:
    kind = "host"

    def __init__(self, session_root: str):
        self._session_root = session_root

    def launch(self, opts: dict) -> PiRpcClient:
        session_dir = os.path.join(self._session_root, opts["sessionId"])
        os.makedirs(session_dir, exist_ok=True)
        child = subprocess.Popen(
            ["pi", *_pi_args(opts, session_dir)],
            cwd=opts["workspacePath"],
            env=_pi_env(),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        return PiRpcClient(child)

    def cleanup(self, session_id: str) -> None:
        pass


class ContainerExecutor:
    """`docker run -i` keeps stdin open, which is what the JSONL protocol
    needs, and the container is labelled so a crashed portal can still find
    and reap it."""

    kind = "container"

    def __init__(self, image: str, session_root: str, limits: dict):
        self._image = image
        self._session_root = session_root
        self._limits = limits

    def launch(self, opts: dict) -> PiRpcClient:
        container_name = f"pithagoras-{opts['sessionId']}"
        session_dir = os.path.join(self._session_root, opts["sessionId"])
        os.makedirs(session_dir, exist_ok=True)

        passthrough: list[str] = []
        for key in ("OPENROUTER_API_KEY", "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "PI_PROVIDER", "PI_MODEL"):
            if os.environ.get(key):
                passthrough += ["-e", f"{key}={os.environ[key]}"]

        args = [
            "docker", "run", "-i", "--rm",
            "--name", container_name,
            "--label", f"pithagoras.session={opts['sessionId']}",
            "--label", "pithagoras.managed=true",
            "-w", "/workspace",
            "-v", f"{opts['workspacePath']}:/workspace",
            "-v", f"{session_dir}:/sessions",
            # Same hardening posture as the sandboxes: no extra capabilities, no
            # privilege escalation, and hard resource ceilings.
            "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges",
            "--memory", f"{self._limits['memoryMb']}m",
            "--memory-swap", f"{self._limits['memoryMb']}m",
            "--cpus", str(self._limits["cpus"]),
            "--pids-limit", str(self._limits["pidsLimit"]),
            *passthrough,
            self._image,
            "pi", *_pi_args(opts, "/sessions"),
        ]
        child = subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE, bufsize=0)
        return PiRpcClient(child)

    def cleanup(self, session_id: str) -> None:
        subprocess.run(["docker", "rm", "-f", f"pithagoras-{session_id}"],
                       capture_output=True, timeout=30)


def build_executor(kind: Optional[str] = None, session_root: str = "./data/sessions"):
    kind = kind or EXECUTOR_KIND
    if kind == "container":
        return ContainerExecutor(
            os.environ.get("PI_IMAGE", "pithagoras-runner:latest"),
            session_root,
            {
                "memoryMb": int(os.environ.get("TASK_MEMORY_MB", "2048")),
                "cpus": int(os.environ.get("TASK_CPUS", "2")),
                "pidsLimit": int(os.environ.get("TASK_PIDS_LIMIT", "512")),
            },
        )
    return HostExecutor(session_root)
