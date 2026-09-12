"""Self-contained snapshot job bootstrap and best-effort runtime diagnostics.

The generated command contains only Python standard-library code. It does not
require cloudru to be installed in the training image.
"""
from __future__ import annotations

import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import zlib


_PATH_VARIABLE = re.compile(r"\$(?:\{([A-Za-z_][A-Za-z0-9_]*)\}|([A-Za-z_][A-Za-z0-9_]*))")


class _JobInterrupted(RuntimeError):
    # selectors/subprocess may swallow InterruptedError as an interrupted syscall.
    # A distinct exception must escape communicate() to cancel an active transfer.
    pass


def validate_setup(setup: dict, env: dict) -> dict:
    """Validate managed setup and expand path variables without executing shell."""
    if not isinstance(setup, dict):
        raise ValueError("setup must be an object")
    allowed = {"shell_init", "conda_env", "pre_command", "check_hf_auth", "workdir", "print_pwd"}
    if set(setup) - allowed:
        raise ValueError("Unsupported managed setup keys: " + ", ".join(sorted(set(setup) - allowed)))
    result = dict(setup)
    for key in ("shell_init", "conda_env", "workdir"):
        if key in result and (not isinstance(result[key], str) or "\0" in result[key]):
            raise ValueError(f"setup.{key} must be a string without NUL characters")
    if "workdir" in result and not result["workdir"]:
        raise ValueError("setup.workdir must not be empty")
    for key in ("check_hf_auth", "print_pwd"):
        if key in result and type(result[key]) is not bool:
            raise ValueError(f"setup.{key} must be a boolean")
    commands = result.get("pre_command", [])
    if isinstance(commands, str):
        commands = [commands]
    if not isinstance(commands, list) or any(not isinstance(v, str) or "\0" in v for v in commands):
        raise ValueError("setup.pre_command must be a string or list of strings without NUL characters")
    result["pre_command"] = [v for v in commands if v.strip()]
    result.setdefault("workdir", "${CLOUDRU_SOURCE_DIR}")

    for key in ("workdir", "conda_env"):
        if key in result:
            result[key] = expand_variables(result[key], env, f"setup.{key}")
    return result


def expand_variables(value: str, env: dict, label: str) -> str:
    """Expand configured variables once, without interpreting shell expressions."""
    if "$" in _PATH_VARIABLE.sub("", value):
        raise ValueError(f"{label} supports only $NAME and ${{NAME}} substitution")

    def expand(match):
        name = match.group(1) or match.group(2)
        if name not in env:
            raise ValueError(f"Unknown environment variable in {label}: {name}")
        return str(env[name])

    return _PATH_VARIABLE.sub(expand, value)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_text(path: Path, text: str) -> None:
    fd, temporary = tempfile.mkstemp(prefix=".writing-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(text)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _write_json(path: Path, value) -> None:
    _write_text(path, json.dumps(value, ensure_ascii=True, indent=2, sort_keys=True) + "\n")


def _probe(command: list[str]) -> str:
    """Keep tool output readable, with bounded waits and explicit failures."""
    try:
        result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, errors="replace", timeout=10)
        output = result.stdout.rstrip()
        if result.stderr:
            output += "\nStderr:\n" + result.stderr.rstrip()
        if result.returncode:
            output += f"\nExit code: {result.returncode}"
        return output.strip() or "No output"
    except subprocess.TimeoutExpired:
        return "Unavailable: timed out after 10 seconds"
    except (InterruptedError, _JobInterrupted):
        raise
    except OSError as exc:
        return f"Unavailable: {type(exc).__name__}"


def _best_effort(control: Path, name: str, collect) -> None:
    try:
        value = collect()
    except (InterruptedError, _JobInterrupted):
        raise
    except Exception as exc:
        value = f"Unavailable: {type(exc).__name__}\n" if name.endswith(".txt") else {"error": type(exc).__name__}
    try:
        writer = _write_text if name.endswith(".txt") else _write_json
        writer(control / name, value)
    except OSError:
        print(f"cloudru: could not save diagnostic {name}", file=sys.stderr)


def _system(directory: Path, bash_executable: str, aws_cli: str) -> str:
    lines = [f"Captured at: {_now()}", f"Hostname: {socket.gethostname()}",
             f"OS: {platform.platform()}", f"UID/GID: {os.getuid()}/{os.getgid()}",
             f"CPU count: {os.cpu_count()}"]
    try:
        memory = Path("/proc/meminfo").read_text().splitlines()
        lines.extend(line for line in memory if line.startswith(("MemTotal:", "MemAvailable:", "SwapTotal:", "SwapFree:")))
    except OSError:
        lines.append("Memory: unavailable")
    disk = shutil.disk_usage(directory)
    lines.append(f"Disk (GiB): total={disk.total / 2**30:.1f}, used={disk.used / 2**30:.1f}, free={disk.free / 2**30:.1f}")
    for label, command in (("Bash", [bash_executable, "--version"]),
                           ("AWS", [aws_cli, "--version"]), ("GPU", ["nvidia-smi"])):
        lines.extend(["", label, _probe(command)])
    return "\n".join(lines) + "\n"


def _packages() -> str:
    executable = shutil.which("python") or shutil.which("python3") or sys.executable
    program = ("import sys,importlib.metadata as m; "
               "print('Executable: ' + sys.executable); print('Version: ' + sys.version); "
               "print('\\n'.join(sorted((d.metadata.get('Name', '') + '==' + d.version "
               "for d in m.distributions()), key=str.lower)))")
    conda = os.environ.get("CONDA_EXE") or shutil.which("conda")
    return ("Python\n" + _probe([executable, "-c", program]) + "\n\nConda\n" +
            (_probe([conda, "list"]) if conda else "Unavailable: conda not found") + "\n")


def _prepared(control: Path) -> None:
    _best_effort(control, "environment.prepared.json", lambda: dict(os.environ))
    _best_effort(control, "packages.txt", _packages)
    _set_phase(control, "experiment")


def _set_phase(control: Path, phase: str, **updates) -> None:
    path = control / "status.json"
    status = json.loads(path.read_text()) if path.exists() else {"started_at": _now()}
    status.update(phase=phase, updated_at=_now(), **updates)
    _write_json(path, status)


def _experiment_script(setup: dict, main: str, python: str, helper: Path, control: Path) -> str:
    # Shell initialization/activation already ran before launching Python.
    steps = list(setup.get("pre_command", []))
    if setup.get("check_hf_auth"):
        steps.append("hf auth whoami")
    steps.append("cd -- " + shlex.quote(setup["workdir"]))
    if setup.get("print_pwd"):
        steps.append('echo "Current directory: $(pwd)"')
    steps.append(shlex.join([python, str(helper), "prepared", str(control)]))
    # Each setup entry is a shell program. A failing simple command in it must
    # stop setup, while exports/functions/cd must persist into the main command.
    return "set -e\n" + "\n".join(steps) + "\n" + main + "\n"


def _collect_outputs(control: Path, collection: dict, destination: str, storage) -> bool:
    from cloudru_storage import upload_directory

    collection["status"] = "running"
    for item in collection["outputs"]:
        item["status"] = "uploading"
        _set_phase(control, "collection", collection=collection)
        print(f"cloudru: collecting {item['source']} -> {item['destination']}", file=sys.stderr)
        try:
            uploaded = upload_directory(item["source"], destination, storage)
            item["status"] = "uploaded" if uploaded else "skipped"
            if not uploaded:
                item["reason"] = "directory is missing"
                print(f"cloudru: warning: output directory is missing: {item['source']}", file=sys.stderr)
        except _JobInterrupted:
            item["status"] = "interrupted"
            raise
        except (OSError, ValueError, RuntimeError) as exc:
            item.update(status="failed", error=type(exc).__name__,
                        reason=type(exc).__name__ if isinstance(exc, OSError) else str(exc))
            print(f"cloudru: output collection failed for {item['source']}: {item['reason']}", file=sys.stderr)
        _set_phase(control, "collection", collection=collection)
    failed = any(item["status"] == "failed" for item in collection["outputs"])
    collection["status"] = "failed" if failed else "succeeded"
    return failed


def run_job(config: dict, aws_cli: str, bash_executable: str, config_yaml: str) -> int:
    """Run on the job machine; stdout/stderr remain attached to the platform."""
    from cloudru_snapshot import extract_snapshot
    from cloudru_storage import resolve_download_config, download_snapshot, output_destination

    # Keep the activated PATH and initialization exports. Reapply only managed
    # context so activation hooks cannot redirect transfers or job directories.
    configured = config["job"]["env_variables"]
    for key in ("HOME", "CLOUDRU_JOBS_ROOT", "CLOUDRU_JOB_DIR_NAME", "CLOUDRU_JOB_DIR", "CLOUDRU_SOURCE_DIR",
                "CLOUDRU_SNAPSHOT_URI", "AWS_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"):
        os.environ[key] = configured[key]
    os.environ["CLOUDRU_AWS_CLI"] = aws_cli
    env = os.environ
    directory = Path(env["CLOUDRU_JOB_DIR"])
    root = Path(env["CLOUDRU_JOBS_ROOT"])
    if (not directory.is_absolute() or not root.is_absolute() or root == Path("/")
            or directory.parent != root or directory.name in ("", ".", "..")):
        print("cloudru: invalid job directory", file=sys.stderr)
        return 2
    control = directory / ".cloudru"
    child = None
    experiment_code = None
    collection = None
    if config.get("outputs"):
        collection = {"status": "pending", "outputs": [
            {"source": path, "destination": output_destination(path, config["collect_outputs_to"]), "status": "pending"}
            for path in config["outputs"]]}
    received_signal = None
    previous_handlers = {}

    def forward(signum, frame):
        nonlocal received_signal
        received_signal = signum
        if child is not None:
            try:
                os.killpg(child.pid, signum)
            except ProcessLookupError:
                pass
        else:
            raise _JobInterrupted("Job terminated")

    try:
        root.mkdir(parents=True, exist_ok=True)
        directory.mkdir(mode=0o700)  # Atomic collision check, including symlinks.
    except OSError:
        print("cloudru: cannot create fresh job directory (it may already exist)", file=sys.stderr)
        return 2
    try:
        control.mkdir(mode=0o700)
        _set_phase(control, "startup", complete=False, exit_code=None,
                   **({"collection": collection} if collection else {}))
        _write_text(control / "config.yaml", config_yaml)
        _best_effort(control, "environment.startup.json", lambda: dict(env))
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous_handlers[signum] = signal.signal(signum, forward)
        # Keep the environment's interpreter fixed if pre-commands change PATH.
        python = os.path.abspath(sys.executable)
        helper = control / "runtime.py"
        source = getattr(sys.modules[__name__], "__source__", None)
        if source is None:
            source = Path(__file__).read_text()
        fd = os.open(helper, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(source)
        _set_phase(control, "aws_validation")
        storage = resolve_download_config(env["CLOUDRU_SNAPSHOT_URI"], config["endpoint_url"], dict(env))
        _best_effort(control, "system.txt", lambda: _system(directory, bash_executable, storage.aws_cli))
        archive = control / "input.tar.gz"
        _set_phase(control, "download")
        download_snapshot(env["CLOUDRU_SNAPSHOT_URI"], str(archive), storage)
        _set_phase(control, "extraction")
        snapshot = extract_snapshot(str(archive), str(directory))
        expected_digest = config.get("snapshot", {}).get("source_digest")
        if expected_digest is not None and snapshot["source_digest"] != expected_digest:
            raise ValueError("Downloaded source digest differs from the submitted snapshot")
        archive.unlink()
        _set_phase(control, "setup")
        script = _experiment_script(config["setup"], config["job"]["script"], python, helper, control)
        child = subprocess.Popen([bash_executable, "-c", script], start_new_session=True)
        code = child.wait()
        child = None  # Signals must now interrupt AWS collection, not the completed shell.
        if received_signal:
            code = 128 + received_signal
        elif code < 0:
            code = 128 - code
        try:
            status = json.loads((control / "status.json").read_text())
            phase = status["phase"]
        except (OSError, ValueError, KeyError):
            print("cloudru: could not read final status; preserving command exit code", file=sys.stderr)
            return code or (1 if collection else 0)
        if phase != "experiment" and code == 0:
            # An early `exit 0`/`exec` in setup must not report successful training.
            code = 2
        if phase == "experiment":
            experiment_code = code
        if collection:
            if experiment_code is None or received_signal:
                collection["status"] = "interrupted" if received_signal else "skipped"
                for item in collection["outputs"]:
                    item.update(status="skipped", reason="job cancelled" if received_signal else "experiment did not start")
            else:
                phase = "collection"
                _set_phase(control, phase, experiment_exit_code=experiment_code, collection=collection)
                failed = _collect_outputs(control, collection, config["collect_outputs_to"], storage)
                code = experiment_code or int(failed)
        try:
            _set_phase(control, phase, complete=True, finished_at=_now(), exit_code=code,
                       **({"experiment_exit_code": experiment_code, "collection": collection} if collection else {}))
        except (OSError, ValueError, KeyError):
            print("cloudru: could not save final status; preserving command exit code", file=sys.stderr)
        return code
    except Exception as exc:
        code = 128 + received_signal if received_signal else (experiment_code or (1 if experiment_code is not None else 2))
        if collection:
            collection["status"] = "interrupted" if received_signal else ("failed" if experiment_code is not None else "skipped")
            for item in collection["outputs"]:
                if item["status"] in ("pending", "uploading"):
                    item.update(status="skipped", reason="job cancelled" if received_signal else "collection did not complete")
        # Exception messages may contain credentials, commands, or environment values.
        print(f"cloudru: job bootstrap failed ({type(exc).__name__}); see .cloudru/status.json", file=sys.stderr)
        if control.is_dir():
            try:
                status = json.loads((control / "status.json").read_text())
                _set_phase(control, status["phase"], complete=True, finished_at=_now(),
                           exit_code=code, error=type(exc).__name__, message=str(exc),
                           **({"experiment_exit_code": experiment_code, "collection": collection} if collection else {}))
            except (OSError, ValueError):
                pass
        return code
    finally:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


def build_job_script(config: dict) -> str:
    """Embed stdlib modules and resolved config in a single-line API script."""
    import yaml  # Local CLI dependency only; the remote runtime stays stdlib-only.

    class ConfigDumper(yaml.SafeDumper):
        pass

    def readable_string(dumper, value):
        return dumper.represent_scalar("tag:yaml.org,2002:str", value,
                                       style="|" if "\n" in value else None)

    ConfigDumper.add_representer(str, readable_string)
    config_yaml = yaml.dump(config, Dumper=ConfigDumper, sort_keys=False, allow_unicode=True)
    directory = Path(__file__).parent
    lines = ["import sys, types"]
    for name in ("cloudru_snapshot", "cloudru_storage", "cloudru_job_runtime"):
        source = (directory / (name + ".py")).read_text()
        lines.extend([f"m = types.ModuleType({name!r})", f"m.__file__ = {name + '.py'!r}",
                      f"m.__source__ = {source!r}", f"sys.modules[{name!r}] = m",
                      f"exec(compile(m.__source__, m.__file__, 'exec'), m.__dict__)"])
    lines.append("import json")
    lines.append(f"sys.exit(m.run_job(json.loads({json.dumps(config, ensure_ascii=True)!r}), sys.argv[1], sys.argv[2], {config_yaml!r}))")
    payload = base64.b64encode(zlib.compress("\n".join(lines).encode(), 9)).decode("ascii")
    code = ("import sys; sys.version_info >= (3, 9) or "
            "sys.exit('cloudru: activated environment requires Python 3.9+'); "
            f"import base64,zlib;exec(zlib.decompress(base64.b64decode({payload!r})))")
    steps = ["set -e", 'readonly _cloudru_bootstrap_bash="$BASH"',
             "trap '_cloudru_bootstrap_exit=$?; if [ \"$_cloudru_bootstrap_exit\" -eq 0 ]; then "
             "printf \"%s\\n\" \"cloudru: initialization exited before starting the job\" >&2; exit 2; fi' EXIT"]
    steps.extend("export " + shlex.quote(key + "=" + value)
                 for key, value in config["job"]["env_variables"].items())
    # Only Bash builtins are needed before activation. Resolve executables now,
    # including relative PATH entries, so conda cannot replace the selected AWS.
    steps.extend([
        '_cloudru_bootstrap_aws=$(type -P -- "$CLOUDRU_AWS_CLI") || '
        '{ printf "%s\\n" "cloudru: AWS executable unavailable before environment activation" >&2; exit 2; }',
        'case "$_cloudru_bootstrap_aws" in /*) ;; *) _cloudru_bootstrap_aws="$(builtin pwd -P)/$_cloudru_bootstrap_aws" ;; esac',
        'readonly _cloudru_bootstrap_aws',
        # Export scalar initialization state as well as explicit exports so it
        # remains available across the Python supervisor and experiment shell.
        'set -a',
    ])
    setup = config["setup"]
    if setup.get("shell_init"):
        steps.append(setup["shell_init"])
    elif setup.get("conda_env"):
        steps.append('eval "$(conda shell.bash hook)"')
    if setup.get("conda_env"):
        steps.append("conda activate " + shlex.quote(setup["conda_env"]))
    steps.extend([
        'set +a',
        'while IFS= read -r _cloudru_bootstrap_function; do export -f "$_cloudru_bootstrap_function"; done < <(compgen -A function)',
        '_cloudru_bootstrap_python=$(type -P python || type -P python3) || '
        '{ printf "%s\\n" "cloudru: activated environment requires python or python3 (3.9+)" >&2; exit 2; }',
        'exec "$_cloudru_bootstrap_python" -c ' + shlex.quote(code) +
        ' "$_cloudru_bootstrap_aws" "$_cloudru_bootstrap_bash"',
    ])
    # eval receives a literal script: preserve multiline user commands without
    # placing literal newlines into the API's one-line command field.
    script = "\n".join(steps)
    quoted = "$'" + script.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n").replace("\r", "\\r") + "'"
    return "bash -c " + shlex.quote("eval " + quoted)


if __name__ == "__main__":
    if len(sys.argv) == 3 and sys.argv[1] == "prepared":
        _prepared(Path(sys.argv[2]))
    else:
        raise SystemExit("Usage: runtime.py prepared /path/to/job/.cloudru")
