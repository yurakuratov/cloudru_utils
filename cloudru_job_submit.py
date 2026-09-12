"""Read-only managed-job validation and snapshot submission orchestration."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from urllib.parse import urlsplit
import uuid

import cloudru_config
from cloudru_snapshot import DEFAULT_MAX_BYTES, create_snapshot, inspect_snapshot, prepare_snapshot, snapshot_filename
from cloudru_storage import _endpoint, _prefix, resolve_upload_config, upload_snapshot, validate_snapshot_uri, output_destination

GENERATED_ENV = {"CLOUDRU_JOB_DIR_NAME", "CLOUDRU_JOB_DIR", "CLOUDRU_SOURCE_DIR", "CLOUDRU_SNAPSHOT_URI"}
CAPTURE_KEYS = {"output_dir", "use_gitignore", "exclude", "exclude_from", "max_bytes"}
LOCAL_MAP = {"aws_cli": "aws_cli", "profile": "aws_profile", "config_file": "aws_config_file",
             "credentials_file": "aws_credentials_file"}
REMOTE_AWS_ENV = {"AWS_PROFILE", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE",
                  "AWS_REGION", "AWS_DEFAULT_REGION"}


def _text(value):
    return isinstance(value, str) and bool(value.strip()) and not any(ord(c) < 32 or ord(c) == 127 for c in value)


def _unknown(value, allowed, label, errors):
    for key in value:
        if key not in allowed:
            errors.append(f"Unknown {label} key: {key}")


def _object(value, label, errors):
    if not isinstance(value, dict):
        errors.append(f"{label} must be an object")
        return {}
    return value


def _local_path(value, base):
    path = Path(value).expanduser()
    return str(path if path.is_absolute() else base / path)


def _remote_path(value, label, *, allow_dollar=False):
    if (not _text(value) or not value.startswith("/") or value.startswith("//")
            or any(p in (".", "..") for p in value.split("/")) or ("$" in value and not allow_dollar) or "\\" in value):
        raise ValueError(f"{label} must be an absolute remote path without variables or traversal")
    return value


def resolve_remote_env(raw):
    """Keep the remote context independent of local HOME and AWS settings."""
    errors = []
    env = dict(_object(raw, "job.env_variables", errors))
    for key, value in env.items():
        if not isinstance(key, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            errors.append("job.env_variables keys must be shell variable names")
        if not isinstance(value, str) or "\0" in value:
            errors.append(f"job.env_variables.{key} must be a string without NUL")
        if key in GENERATED_ENV:
            errors.append(f"job.env_variables.{key} is generated and cannot be overridden")
        if isinstance(key, str) and ((key.upper().startswith("AWS_") and key not in REMOTE_AWS_ENV)
                                     or key.upper() in {"BOTO_CONFIG", "BOTO_PATH"}):
            errors.append(f"job.env_variables.{key} is an unsupported AWS setting; use the explicit static profile/files")
    home = env.get("HOME")
    try:
        _remote_path(home, "job.env_variables.HOME")
    except ValueError as exc:
        errors.append(str(exc))
    env.setdefault("AWS_PROFILE", "default")
    if not _text(env.get("AWS_PROFILE")) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@+-]*", env.get("AWS_PROFILE", "")):
        errors.append("job.env_variables.AWS_PROFILE must be a static profile name")
    if isinstance(home, str):
        env.setdefault("CLOUDRU_JOBS_ROOT", home.rstrip("/") + "/data/jobs")
        env.setdefault("AWS_CONFIG_FILE", home.rstrip("/") + "/.aws/config")
        env.setdefault("AWS_SHARED_CREDENTIALS_FILE", home.rstrip("/") + "/.aws/credentials")
    env.setdefault("CLOUDRU_AWS_CLI", "aws")
    for key in ("CLOUDRU_JOBS_ROOT", "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE"):
        try:
            _remote_path(env.get(key), f"job.env_variables.{key}")
            if env[key].rstrip("/") == "":
                raise ValueError(f"job.env_variables.{key} must not be the filesystem root")
        except ValueError as exc:
            errors.append(str(exc))
    cli = env["CLOUDRU_AWS_CLI"]
    if not _text(cli):
        errors.append("job.env_variables.CLOUDRU_AWS_CLI must be a command name or absolute remote path")
    elif "/" in cli:
        try:
            _remote_path(cli, "job.env_variables.CLOUDRU_AWS_CLI")
        except ValueError as exc:
            errors.append(str(exc))
    elif not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.+-]*", cli):
        errors.append("job.env_variables.CLOUDRU_AWS_CLI must be a command name or absolute remote path")
    return env, errors


def snapshot_job_dir(uri, jobs_root, *, dry_run=False):
    """Use literal S3 key bytes, never URL-decoding percent escapes."""
    basename = urlsplit(uri).path.rsplit("/", 1)[-1]
    if not basename:
        raise ValueError("snapshot.uri must have a nonempty object name")
    name = basename[:-7] if basename.endswith(".tar.gz") else basename
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("._") or "source"
    suffix = "__4_hex_suffix__" if dry_run else uuid.uuid4().hex[:4]
    return jobs_root.rstrip("/") + "/" + name[:200] + "-" + suffix


def validate_managed_schema(document, job, setup, allowed_job_fields):
    """Aggregate schema/type errors before performing capture or authentication."""
    errors = []
    _unknown(document, {"job", "setup", "snapshot", "s3", "outputs"}, "top-level", errors)
    _unknown(job, allowed_job_fields, "job", errors)
    snapshot = _object(document.get("snapshot"), "snapshot", errors)
    _unknown(snapshot, {"source", "archive", "uri"} | CAPTURE_KEYS, "snapshot", errors)
    modes = [k for k in ("source", "archive", "uri") if k in snapshot]
    if len(modes) != 1:
        errors.append("snapshot requires exactly one of source, archive, uri")
    if "source" not in snapshot and CAPTURE_KEYS.intersection(snapshot):
        errors.append("snapshot capture-only options require snapshot.source")
    for key in ("source", "archive", "uri", "output_dir", "exclude_from"):
        if key in snapshot and not _text(snapshot[key]):
            errors.append(f"snapshot.{key} must be a nonempty string without control characters")
    if "use_gitignore" in snapshot and type(snapshot["use_gitignore"]) is not bool:
        errors.append("snapshot.use_gitignore must be a boolean")
    if "max_bytes" in snapshot and (type(snapshot["max_bytes"]) is not int or snapshot["max_bytes"] <= 0):
        errors.append("snapshot.max_bytes must be a positive integer")
    if "exclude" in snapshot and (not isinstance(snapshot["exclude"], list) or not all(_text(x) for x in snapshot["exclude"])):
        errors.append("snapshot.exclude must be a list of nonempty strings")
    s3 = _object(document.get("s3", {}), "s3", errors)
    _unknown(s3, {"endpoint_url", "snapshot_prefix", "local", "collect_outputs_to"}, "s3", errors)
    for key in ("endpoint_url", "snapshot_prefix", "collect_outputs_to"):
        if key in s3 and not _text(s3[key]):
            errors.append(f"s3.{key} must be a nonempty string")
    local = _object(s3.get("local", {}), "s3.local", errors)
    _unknown(local, LOCAL_MAP, "s3.local", errors)
    for key, value in local.items():
        if not _text(value):
            errors.append(f"s3.local.{key} must be a nonempty string")
    required = {"script", "base_image", "instance_type", "region"}
    string_fields = required | {"job_type", "job_desc", "conda_env", "priority_class", "checkpoint_dir", "spark_executor_memory", "allocation_name", "queue_name"}
    for key in required | set(job):
        value = job.get(key)
        if key in string_fields and (not isinstance(value, str) or (key in required and not value.strip()) or (isinstance(value, str) and "\0" in value)):
            if value is not None or key in required:
                errors.append(f"job.{key} must be a {'nonempty ' if key in required else ''}string")
    for key in ("internet", "pytorch_use_env"):
        if key in job and type(job[key]) is not bool:
            errors.append(f"job.{key} must be a boolean")
    if job.get("internet") is False:
        errors.append("Managed jobs require job.internet: true to download the snapshot")
    for key in ("flags", "health_params"):
        if key in job and job[key] is not None and not isinstance(job[key], dict):
            errors.append(f"job.{key} must be an object")
    if job.get("flags"):
        errors.append("Managed jobs do not support nonempty job.flags; include experiment arguments in job.script")
    if "stop_timer" in job and (type(job["stop_timer"]) is not int or job["stop_timer"] < 0):
        errors.append("job.stop_timer must be a nonnegative integer")
    if job.get("job_type", "binary") != "binary":
        errors.append("Managed jobs require job.job_type: binary")
    for key in ("n_workers", "processes_per_worker"):
        if key in job and (type(job[key]) is not int or job[key] != 1):
            errors.append(f"Managed jobs require job.{key}: 1")
    for key in ("elastic_min_workers", "elastic_max_workers", "elastic_max_restarts", "spark_executor_memory"):
        if key in job:
            errors.append(f"Managed jobs do not support distributed setting job.{key}")
    if job.get("pytorch_use_env") is True:
        errors.append("Managed jobs do not support job.pytorch_use_env")
    if "max_retry" in job and job["max_retry"] is not None:
        errors.append("Managed jobs do not support explicit automatic retries (job.max_retry)")
    try:
        json.dumps({"job": job, "setup": setup, "snapshot": snapshot, "s3": s3}, allow_nan=False)
    except (ValueError, TypeError, RecursionError):
        errors.append("Managed job configuration must contain JSON-compatible values without NaN or infinity")
    return errors


def resolve_storage_values(document, base, profile_values, overrides):
    values = {key: profile_values.get(key) for key in cloudru_config.SNAPSHOT_STORAGE_KEYS}
    s3 = document.get("s3", {})
    if not isinstance(s3, dict):
        s3 = {}
    for yaml_key, key in (("endpoint_url", "s3_endpoint_url"), ("snapshot_prefix", "s3_snapshot_prefix")):
        if yaml_key in s3:
            values[key] = s3[yaml_key]
    local = s3.get("local", {})
    if not isinstance(local, dict):
        local = {}
    for yaml_key, value in local.items():
        if yaml_key not in LOCAL_MAP:
            continue
        key = LOCAL_MAP[yaml_key]
        if isinstance(value, str) and (yaml_key in ("config_file", "credentials_file") or (yaml_key == "aws_cli" and "/" in value)):
            value = _local_path(value, base)
        values[key] = value
    values.update({key: value for key, value in overrides.items() if value is not None})
    return values


def resolve_collection(outputs, destination, env):
    from cloudru_job_runtime import expand_variables

    if not isinstance(outputs, list) or any(not _text(path) for path in outputs):
        raise ValueError("outputs must be a list of nonempty directory paths")
    if not outputs:
        return {}
    if not _text(destination):
        raise ValueError("outputs requires s3.collect_outputs_to or --collect-outputs-to")
    destination = expand_variables(destination, env, "s3.collect_outputs_to")
    try:
        _prefix(destination)
    except ValueError:
        raise ValueError("s3.collect_outputs_to must resolve to a valid S3 destination") from None
    paths, names = [], set()
    for index, path in enumerate(outputs):
        label = f"outputs[{index}]"
        path = _remote_path(expand_variables(path, env, label), label, allow_dollar=True).rstrip("/")
        name = path.rsplit("/", 1)[-1]
        if not name or name in names:
            raise ValueError("outputs requires non-root directories with distinct basenames")
        names.add(name)
        output_destination(path, destination)
        paths.append(path)
    return {"outputs": paths, "collect_outputs_to": destination}


@dataclass
class ManagedSubmission:
    job: dict
    setup: dict
    snapshot: dict
    endpoint_url: str
    storage: object = None
    capture_plan: object = None
    archive: dict | None = None
    outputs: list | None = None
    collect_outputs_to: str | None = None

    def runtime_config(self, uri, digest=None, *, dry_run=False):
        from cloudru_job_runtime import validate_setup
        env = dict(self.job["env_variables"])
        directory = snapshot_job_dir(uri, env["CLOUDRU_JOBS_ROOT"], dry_run=dry_run)
        env.update(CLOUDRU_JOB_DIR_NAME=directory.rsplit("/", 1)[-1], CLOUDRU_JOB_DIR=directory,
                   CLOUDRU_SOURCE_DIR=directory + "/source", CLOUDRU_SNAPSHOT_URI=uri)
        snapshot = {"uri": uri}
        if digest is not None:
            snapshot["source_digest"] = digest
        return {"job": {**self.job, "env_variables": env}, "setup": validate_setup(self.setup, env),
                "snapshot": snapshot, "endpoint_url": self.endpoint_url,
                **resolve_collection(self.outputs or [], self.collect_outputs_to, env)}

    def preview(self):
        if "uri" in self.snapshot:
            uri = self.snapshot["uri"]
        else:
            name = Path(self.archive["archive_path"]).name if self.archive else "__snapshot_name_pending__.tar.gz"
            uri = self.storage.s3_snapshot_prefix + "/" + name
        result = self.runtime_config(uri, self.archive.get("source_digest") if self.archive else None, dry_run=True)
        result["dry_run"] = True
        if result.get("outputs"):
            result["collection"] = [{"source": path, "destination": output_destination(path, result["collect_outputs_to"])}
                                    for path in result["outputs"]]
        if self.capture_plan:
            result["capture"] = self.capture_plan.public_dict()
            result["snapshot"]["source_digest"] = "<computed during capture>"
        if self.archive:
            result["archive_path"] = self.archive["archive_path"]
        if self.storage:
            result["upload"] = self.storage.public_dict()
        return result

    def materialize(self, progress):
        if self.capture_plan:
            progress(f"Capturing source: {self.capture_plan.source}")
            self.archive = create_snapshot(self.capture_plan)
        if self.archive:
            progress(f"Local snapshot: {self.archive['archive_path']}")
        uri = self.snapshot.get("uri")
        if self.archive:
            # Build the final wrapper before upload as a final validation step.
            uri = self.storage.s3_snapshot_prefix + "/" + Path(self.archive["archive_path"]).name
        config = self.runtime_config(validate_snapshot_uri(uri), self.archive.get("source_digest") if self.archive else None)
        from cloudru_job_runtime import build_job_script
        payload = {**config["job"], "script": build_job_script(config)}
        if self.archive:
            upload_snapshot(self.archive, self.storage)
        return config, payload


def prepare_snapshot_submission(document, job, setup, *, base, profile_values,
                                storage_overrides, allowed_job_fields, no_bootstrap=False,
                                use_gitignore=None, collect_outputs_to=None):
    from cloudru_job_runtime import validate_setup
    job, setup = dict(job), dict(setup)
    errors = validate_managed_schema(document, job, setup, allowed_job_fields)
    # Activate inside our Bash bootstrap, after installing remote context and
    # resolving AWS, so its Python can run download/extraction and diagnostics.
    conda = job.pop("conda_env", None)
    if conda is not None:
        if "conda_env" in setup and setup["conda_env"] != conda:
            errors.append("job.conda_env conflicts with setup.conda_env; specify one managed environment")
        else:
            setup["conda_env"] = conda
    if no_bootstrap:
        errors.append("Managed snapshot submissions do not support --no-bootstrap")
    env, env_errors = resolve_remote_env(job.get("env_variables", {}))
    errors.extend(env_errors)
    snapshot = document.get("snapshot")
    if isinstance(snapshot, dict) and use_gitignore is not None and "source" not in snapshot:
        errors.append("--use-gitignore/--no-use-gitignore requires snapshot.source")
    if isinstance(snapshot, dict) and "uri" in snapshot and any(
            value is not None for key, value in storage_overrides.items() if key != "s3_endpoint_url"):
        errors.append("Local upload CLI options are unused with snapshot.uri; use --env for remote AWS settings")
    # Resolve structured setup against placeholder generated paths before any I/O.
    placeholder_env = {**env, "CLOUDRU_JOB_DIR_NAME": "__cloudru_pending__",
                       "CLOUDRU_JOB_DIR": "/__cloudru_pending__",
                       "CLOUDRU_SOURCE_DIR": "/__cloudru_pending__/source",
                       "CLOUDRU_SNAPSHOT_URI": "s3://placeholder/__snapshot_name_pending__.tar.gz"}
    try:
        validate_setup(setup, placeholder_env)
    except (ValueError, RuntimeError, TypeError) as exc:
        errors.append(str(exc))
    s3 = document.get("s3", {})
    destination = collect_outputs_to
    if destination is None:
        destination = s3.get("collect_outputs_to") if isinstance(s3, dict) else None
    outputs = document.get("outputs", [])
    try:
        resolve_collection(outputs, destination, placeholder_env)
    except (ValueError, RuntimeError, TypeError) as exc:
        errors.append(str(exc))
    values = resolve_storage_values(document, base, profile_values, storage_overrides)
    endpoint = values.get("s3_endpoint_url")
    if not _text(endpoint):
        errors.append("Managed jobs require s3.endpoint_url or --s3-endpoint-url (or s3_endpoint_url in the selected profile)")
    else:
        try:
            endpoint = _endpoint(endpoint)
        except ValueError as exc:
            errors.append(str(exc))
    if isinstance(snapshot, dict) and ("source" in snapshot or "archive" in snapshot):
        if not _text(values.get("s3_snapshot_prefix")):
            errors.append("Local snapshot upload requires s3.snapshot_prefix, --snapshot-s3-prefix, or profile s3_snapshot_prefix")
        else:
            try:
                _prefix(values["s3_snapshot_prefix"])
            except ValueError as exc:
                errors.append(str(exc))
        if not _text(values.get("aws_profile")):
            errors.append("Local snapshot upload requires s3.local.profile, --aws-profile, or profile aws_profile")
    if isinstance(snapshot, dict) and "uri" in snapshot:
        try:
            validate_snapshot_uri(snapshot["uri"])
        except ValueError as exc:
            errors.append(str(exc))
    if errors:
        raise ValueError("Invalid managed job configuration:\n- " + "\n- ".join(errors))
    if "uri" in snapshot:
        storage = None
    else:
        storage = resolve_upload_config(values)
    effective_job = {"job_type": "binary", "n_workers": 1, "processes_per_worker": 1, **job, "env_variables": env}
    managed = ManagedSubmission(effective_job, setup, snapshot, endpoint, storage,
                                outputs=outputs, collect_outputs_to=destination)
    if "source" in snapshot:
        protected = [cloudru_config.CONFIG_PATH, cloudru_config.CREDENTIALS_PATH, cloudru_config.TOKEN_CACHE_PATH,
                     *storage.protected_paths]
        managed.capture_plan = prepare_snapshot(
            _local_path(snapshot["source"], base), _local_path(snapshot.get("output_dir", "./snapshots"), base),
            use_gitignore=use_gitignore if use_gitignore is not None else snapshot.get("use_gitignore", True),
            exclude=snapshot.get("exclude"),
            exclude_from=_local_path(snapshot["exclude_from"], base) if snapshot.get("exclude_from") else None,
            max_bytes=snapshot.get("max_bytes", DEFAULT_MAX_BYTES), protected_paths=protected)
    elif "archive" in snapshot:
        managed.archive = inspect_snapshot(_local_path(snapshot["archive"], base))
    if storage:
        if managed.capture_plan:
            # Date, digest, and random suffix have fixed widths. Use their exact
            # filename format to check S3's key limit without creating a backup.
            filename = snapshot_filename(managed.capture_plan.source.name,
                                         datetime(2000, 1, 1, tzinfo=timezone.utc),
                                         "sha256:" + "0" * 64, "0000")
        else:
            filename = Path(managed.archive["archive_path"]).name
        validate_snapshot_uri(storage.s3_snapshot_prefix + "/" + filename)
    return managed
