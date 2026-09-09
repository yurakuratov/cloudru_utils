from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import getpass
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import traceback
from typing import Optional

import typer
import yaml
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from cloudru_config import (
    CONFIG_PATH,
    CREDENTIALS_PATH,
    file_mode,
    list_auth_profiles,
    load_cached_token,
    load_profile,
    load_snapshot_profile,
    load_submit_profile,
    redact,
    save_cached_token,
    save_profile,
)
from cloudru_bot import run_bot
from cloudru_utils import CloudRuAPIClient
from cloudru_snapshot import (
    DEFAULT_MAX_BYTES,
    create_snapshot,
    prepare_snapshot,
)
from cloudru_storage import resolve_upload_config, upload_snapshot


DEFAULT_SOURCE = "auto"
VALID_SOURCES = ["auto", "instance_types_available", "allocations_instance_types_availability"]
COST_GROUP_FIELDS = ["profile", "region", "n_gpus"]
COST_DEFAULT_GROUP_BY = ["profile", "region"]

SUBMIT_JOB_ALLOWED_FIELDS = {
    "script",
    "base_image",
    "instance_type",
    "region",
    "job_type",
    "n_workers",
    "processes_per_worker",
    "job_desc",
    "internet",
    "conda_env",
    "max_retry",
    "priority_class",
    "checkpoint_dir",
    "flags",
    "env_variables",
    "pytorch_use_env",
    "elastic_min_workers",
    "elastic_max_workers",
    "elastic_max_restarts",
    "spark_executor_memory",
    "health_params",
    "stop_timer",
    "allocation_name",
    "queue_name",
}

app = typer.Typer(help="Cloud.ru jobs helper CLI", no_args_is_help=True, add_completion=True)
workspace_app = typer.Typer(help="Workspace commands", no_args_is_help=True)
allocations_app = typer.Typer(help="Allocation inspection commands", no_args_is_help=True)
resources_app = typer.Typer(help="Resources commands", no_args_is_help=True)
jobs_app = typer.Typer(help="Jobs commands", no_args_is_help=True)
bot_app = typer.Typer(help="Telegram bot commands", no_args_is_help=True)

app.add_typer(workspace_app, name="workspace")
app.add_typer(allocations_app, name="allocations")
app.add_typer(resources_app, name="resources")
app.add_typer(jobs_app, name="jobs")
app.add_typer(bot_app, name="bot")


def _prompt(label: str, default: str | None = None, secret: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    prompt = f"{label}{suffix}: "
    value = getpass.getpass(prompt) if secret else input(prompt)
    value = value.strip()
    if not value and default is not None:
        return default
    return value


def _resolve_profile(ctx: typer.Context, profile: Optional[str]) -> str:
    if profile:
        return profile
    if ctx.obj and ctx.obj.get("profile"):
        return ctx.obj["profile"]
    return os.getenv("CLOUDRU_PROFILE", "default")


def _resolve_debug(ctx: typer.Context, debug: bool) -> bool:
    if debug:
        return True
    if ctx.obj:
        return bool(ctx.obj.get("debug", False))
    return False


def _fail(exc: Exception, debug: bool) -> None:
    if debug:
        traceback.print_exc()
    typer.echo(f"Error: {exc}", err=True)
    raise typer.Exit(1)


def _build_client(profile: str) -> tuple[CloudRuAPIClient, dict]:
    cfg = load_profile(profile=profile, include_env=True)
    if not cfg.get("client_id") or not cfg.get("client_secret"):
        raise RuntimeError(
            f"Missing credentials for profile '{profile}'. Run `cloudru init --profile {profile}` first."
        )

    access_token, access_token_expires_at = load_cached_token(profile=profile)

    client = CloudRuAPIClient(
        client_id=cfg["client_id"],
        client_secret=cfg["client_secret"],
        x_api_key=cfg.get("x_api_key") or None,
        x_workspace_id=cfg.get("x_workspace_id") or None,
        access_token=access_token,
        access_token_expires_at=access_token_expires_at,
        token_persist_callback=lambda token, expires_at: save_cached_token(profile, token, expires_at),
    )
    return client, cfg


def _build_ssh_command(
    target: dict,
    identity: Optional[str] = None,
    tty: bool = False,
    remote_command: Optional[list[str]] = None,
) -> list[str]:
    command = ["ssh", "-p", target["port"]]
    if identity:
        command.extend(["-i", os.path.expanduser(identity)])
    if tty:
        command.append("-t")
    command.append(target["destination"])
    if remote_command is not None:
        if not remote_command:
            raise RuntimeError("A remote command is required")
        command.append(f"exec {shlex.join(remote_command)}")
    return command


def _build_ssh_config(
    target: dict,
    rank: int,
    host_alias: Optional[str] = None,
    identity: Optional[str] = None,
) -> str:
    alias = host_alias if host_alias is not None else f"cloudru-{target['job_id'][-8:]}-r{rank}"
    if not re.fullmatch(r"[A-Za-z0-9._-]+", alias):
        raise RuntimeError("SSH config alias may contain only letters, numbers, '.', '_', and '-'")

    user = f"{target['job_id']}-{target['pod']}.{target['namespace']}"
    lines = [
        f"Host {alias}",
        f"  HostName {target['host']}",
        f"  User {user}",
        f"  Port {target['port']}",
    ]
    if identity:
        identity_path = os.path.expanduser(identity)
        if "\n" in identity_path or "\r" in identity_path:
            raise RuntimeError("SSH identity path must not contain newlines")
        quoted_path = identity_path.replace("\\", "\\\\").replace('"', '\\"')
        lines.append(f'  IdentityFile "{quoted_path}"')
    else:
        lines.extend([
            "  # SSH authentication still requires the matching private key.",
            "  # Load it into ssh-agent, use an OpenSSH default identity, or uncomment:",
            "  # IdentityFile ~/.ssh/private_id_rsa_key",
        ])
    return "\n".join(lines)


def _run_ssh_command(command: list[str], dry_run: bool, debug: bool) -> None:
    if dry_run:
        typer.echo(shlex.join(command))
        return

    try:
        result = subprocess.run(command, check=False)
    except FileNotFoundError:
        _fail(RuntimeError("OpenSSH client 'ssh' was not found in PATH"), debug)
    except KeyboardInterrupt:
        raise typer.Exit(130)

    if result.returncode != 0:
        raise typer.Exit(result.returncode)


def _normalize_status_list(values: list[str], arg_name: str) -> list[str]:
    status_map = {s.lower(): s for s in CloudRuAPIClient.JOB_STATUSES}
    normalized = []
    for value in values:
        for status in [part.strip() for part in value.split(",") if part.strip()]:
            key = status.lower()
            if key not in status_map:
                valid = ", ".join(CloudRuAPIClient.JOB_STATUSES)
                raise RuntimeError(f"Unknown {arg_name} '{status}'. Valid values: {valid}")
            normalized.append(status_map[key])
    return normalized


def _parse_csv_options(values: list[str]) -> list[str]:
    parsed = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if item and item not in parsed:
                parsed.append(item)
    return parsed


def _parse_cost_since(value: str, now: Optional[datetime] = None) -> datetime:
    match = re.fullmatch(r"([1-9]\d*)([mhdw])", str(value).strip(), flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("Invalid --since value. Use a positive duration such as 30m, 12h, 30d, or 4w")

    amount = int(match.group(1))
    unit = match.group(2).lower()
    delta = {
        "m": timedelta(minutes=amount),
        "h": timedelta(hours=amount),
        "d": timedelta(days=amount),
        "w": timedelta(weeks=amount),
    }[unit]
    current = now or datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(timezone.utc) - delta


def _format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _parse_cost_datetime(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_duration_seconds(value) -> Decimal:
    raw = str(value or "").strip().lower()
    if raw.endswith("s"):
        raw = raw[:-1]
    try:
        seconds = Decimal(raw)
    except (InvalidOperation, ValueError):
        return Decimal("0")
    if not seconds.is_finite():
        return Decimal("0")
    return max(seconds, Decimal("0"))


def _parse_reported_cost(value) -> Decimal:
    try:
        cost = Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")
    return cost if cost.is_finite() else Decimal("0")


def _normalize_cost_group_by(values: list[str]) -> list[str]:
    if not values:
        return COST_DEFAULT_GROUP_BY[:]

    parsed = []
    for value in values:
        for field in [part.strip() for part in value.split(",") if part.strip()]:
            if field not in COST_GROUP_FIELDS:
                raise RuntimeError(
                    f"Unknown --group-by field '{field}'. Valid values: {', '.join(COST_GROUP_FIELDS)}"
                )
            if field in parsed:
                raise RuntimeError(f"Duplicate --group-by field '{field}'")
            parsed.append(field)
    return parsed or COST_DEFAULT_GROUP_BY[:]


def _normalize_cost_job(
    job: dict,
    profile: str,
    cutoff: datetime,
    n_gpus: Optional[int],
) -> Optional[dict]:
    created_at = _parse_cost_datetime(job.get("created_dt"))
    if created_at is None or created_at < cutoff:
        return None

    try:
        gpu_count = int(job.get("gpu_count", 0))
    except (TypeError, ValueError):
        gpu_count = 0
    if n_gpus is not None and gpu_count != n_gpus:
        return None

    duration_seconds = _parse_duration_seconds(job.get("duration"))
    return {
        "profile": profile,
        "region": str(job.get("region") or ""),
        "n_gpus": gpu_count,
        "gpu_hours": duration_seconds * Decimal(gpu_count) / Decimal(3600),
        "reported_cost": _parse_reported_cost(job.get("cost")),
    }


def _format_decimal(value: Decimal, places: int = 2) -> str:
    return f"{value:.{places}f}"


def _aggregate_cost_jobs(jobs: list[dict], group_by: list[str]) -> tuple[list[dict], dict]:
    region_costs = defaultdict(lambda: Decimal("0"))
    for job in jobs:
        region_costs[(job["profile"], job["region"])] += job["reported_cost"]

    paid_regions = {key for key, cost in region_costs.items() if cost != 0}
    paid_jobs = [job for job in jobs if (job["profile"], job["region"]) in paid_regions]

    groups = {}
    for job in paid_jobs:
        key = tuple(job[field] for field in group_by)
        group = groups.setdefault(
            key,
            {"jobs": 0, "gpu_hours": Decimal("0"), "reported_cost": Decimal("0")},
        )
        group["jobs"] += 1
        group["gpu_hours"] += job["gpu_hours"]
        group["reported_cost"] += job["reported_cost"]

    total_cost = sum((job["reported_cost"] for job in paid_jobs), Decimal("0"))
    total_gpu_hours = sum((job["gpu_hours"] for job in paid_jobs), Decimal("0"))

    rows = []
    for key, group in groups.items():
        row = {field: value for field, value in zip(group_by, key)}
        row.update({
            "jobs": group["jobs"],
            "gpu_hours": float(_format_decimal(group["gpu_hours"])),
            "reported_cost": _format_decimal(group["reported_cost"]),
            "cost_share_pct": (
                round(float(group["reported_cost"] / total_cost * Decimal(100)), 2)
                if total_cost != 0
                else 0.0
            ),
        })
        rows.append(row)

    rows.sort(
        key=lambda row: (
            -Decimal(row["reported_cost"]),
            tuple(str(row.get(field, "")) for field in group_by),
        )
    )
    totals = {
        "jobs": len(paid_jobs),
        "gpu_hours": float(_format_decimal(total_gpu_hours)),
        "reported_cost": _format_decimal(total_cost),
    }
    return rows, totals


def _render_cost_report(report: dict, table_width: int) -> None:
    labels = {"profile": "Profile", "region": "Region", "n_gpus": "GPUs"}
    table = Table(title=f"Job Costs (since {report['cutoff']})")
    for field in report["group_by"]:
        table.add_column(labels[field], style="cyan" if field == "profile" else "yellow")
    table.add_column("Jobs", justify="right")
    table.add_column("GPU-hours", justify="right")
    table.add_column("Reported cost", justify="right", style="green")
    table.add_column("Share", justify="right")

    for row in report["rows"]:
        table.add_row(
            *[str(row[field]) for field in report["group_by"]],
            str(row["jobs"]),
            f"{row['gpu_hours']:.2f}",
            row["reported_cost"],
            f"{row['cost_share_pct']:.2f}%",
        )

    totals = report["totals"]
    summary = Text()
    summary.append("Jobs: ", style="bold")
    summary.append(str(totals["jobs"]))
    summary.append(" | GPU-hours: ", style="bold")
    summary.append(f"{totals['gpu_hours']:.2f}")
    summary.append(" | Reported cost: ", style="bold green")
    summary.append(totals["reported_cost"], style="green")
    summary.append("\nAs of: ", style="bold")
    summary.append(report["as_of"])

    console = Console(width=table_width)
    console.print(table)
    console.print(Panel(summary, title="Job Cost Total"))
    if report["warnings"]:
        warning_text = Text("\n".join(f"- {warning}" for warning in report["warnings"]))
        console.print(Panel(warning_text, title="Warnings"))


def _load_job_document(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = yaml.safe_load(f)
        except yaml.YAMLError:
            raise RuntimeError("Invalid job YAML syntax") from None

    if data is None:
        data = {}

    if not isinstance(data, dict):
        raise RuntimeError("YAML file must contain an object at top level")
    if "snapshot" not in data and "s3" in data:
        raise RuntimeError("YAML s3 settings require a snapshot section")
    return data


def _job_sections(data: dict) -> tuple[dict, dict]:

    setup_cfg = data.get("setup", {})
    if setup_cfg is None and "snapshot" not in data:
        setup_cfg = {}
    if not isinstance(setup_cfg, dict):
        raise RuntimeError("YAML key 'setup' must contain an object")

    job_cfg = data.get("job", data)
    if not isinstance(job_cfg, dict):
        raise RuntimeError("YAML key 'job' must contain an object")

    return setup_cfg, job_cfg


def _parse_env_overrides(values: list[str]) -> dict:
    env = {}
    for item in values:
        if "=" not in item:
            raise RuntimeError(f"Invalid --env value '{item}'. Expected KEY=VALUE")
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise RuntimeError(f"Invalid --env value '{item}'. Empty key")
        env[key] = value
    return env


def _parse_pre_commands(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        out = []
        for item in value:
            if not isinstance(item, str):
                raise RuntimeError("setup.pre_command list must contain strings")
            if item.strip():
                out.append(item)
        return out
    raise RuntimeError("setup.pre_command must be string or list of strings")


def _build_bootstrap_script(setup_cfg: dict, main_script: str) -> str:
    shell_init = setup_cfg.get("shell_init")
    conda_env = setup_cfg.get("conda_env")
    check_hf_auth = bool(setup_cfg.get("check_hf_auth", False))
    workdir = setup_cfg.get("workdir")
    print_pwd = bool(setup_cfg.get("print_pwd", False))
    pre_commands = _parse_pre_commands(setup_cfg.get("pre_command"))

    steps = []

    if shell_init:
        steps.append(shell_init)
    elif conda_env:
        steps.append('eval "$(conda shell.bash hook)"')

    if conda_env:
        steps.append(f"conda activate {shlex.quote(str(conda_env))}")

    steps.extend(pre_commands)

    if check_hf_auth:
        steps.append("hf auth whoami")

    if workdir:
        steps.append(f"cd {shlex.quote(str(workdir))}")

    if print_pwd:
        steps.append('echo "Current directory: $(pwd)"')

    steps.append(main_script)

    # Keep command one-line to avoid API rejecting multiline/special script payloads.
    # Chain commands with && so setup failures stop execution before running main script.
    inner_command = " && ".join(steps)
    return f"bash -c {shlex.quote(inner_command)}"


def _should_use_bootstrap(setup_cfg: dict) -> bool:
    shell_init = setup_cfg.get("shell_init")
    conda_env = setup_cfg.get("conda_env")
    workdir = setup_cfg.get("workdir")
    check_hf_auth = bool(setup_cfg.get("check_hf_auth", False))
    print_pwd = bool(setup_cfg.get("print_pwd", False))
    pre_commands = _parse_pre_commands(setup_cfg.get("pre_command"))

    return any([
        bool(shell_init),
        bool(conda_env),
        bool(workdir),
        check_hf_auth,
        print_pwd,
        len(pre_commands) > 0,
    ])


@app.callback()
def root_callback(
    ctx: typer.Context,
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    ctx.obj = {"profile": profile, "debug": debug}


@app.command("init", help="Initialize or update profile credentials/config")
def cmd_init(
    ctx: typer.Context,
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    client_id: Optional[str] = typer.Option(None, "--client-id"),
    client_secret: Optional[str] = typer.Option(None, "--client-secret"),
    x_api_key: Optional[str] = typer.Option(None, "--x-api-key"),
    x_workspace_id: Optional[str] = typer.Option(None, "--x-workspace-id"),
    region: Optional[str] = typer.Option(None, "--region"),
    source: Optional[str] = typer.Option(None, "--source", help="Resources source", case_sensitive=False),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        profile_name = _resolve_profile(ctx, profile)
        current = load_profile(profile=profile_name, include_env=False)

        init_client_id = client_id or _prompt("client_id", current.get("client_id"))
        init_client_secret = client_secret or _prompt("client_secret", current.get("client_secret"), secret=True)
        init_x_api_key = x_api_key or _prompt("x_api_key", current.get("x_api_key"), secret=True)
        init_x_workspace_id = x_workspace_id or _prompt("x_workspace_id", current.get("x_workspace_id"))
        init_region = region or _prompt("default region", current.get("region") or "SR006")
        init_source = source or _prompt("resources source", current.get("source") or DEFAULT_SOURCE)

        if not init_client_id or not init_client_secret:
            raise RuntimeError("client_id and client_secret are required")

        if init_source not in VALID_SOURCES:
            raise RuntimeError(f"Invalid source '{init_source}'. Valid values: {', '.join(VALID_SOURCES)}")

        save_profile(
            profile=profile_name,
            client_id=init_client_id,
            client_secret=init_client_secret,
            x_api_key=init_x_api_key,
            x_workspace_id=init_x_workspace_id,
            region=init_region,
            source=init_source,
        )

        typer.echo(f"Saved profile '{profile_name}'")
        typer.echo(f"credentials: {CREDENTIALS_PATH} (mode {file_mode(CREDENTIALS_PATH)})")
        typer.echo(f"config: {CONFIG_PATH}")
        typer.echo(f"client_id: {redact(init_client_id)}")
        typer.echo(f"x_api_key: {redact(init_x_api_key)}")
        typer.echo(f"x_workspace_id: {init_x_workspace_id}")
        typer.echo(f"region: {init_region}")
        typer.echo(f"source: {init_source}")
    except typer.Exit:
        raise
    except Exception as exc:
        _fail(exc, debug_mode)


def _snapshot_settings(ctx, profile, s3_prefix, s3_endpoint_url, aws_profile,
                       aws_cli, aws_config_file, aws_credentials_file) -> dict:
    values = load_snapshot_profile(_resolve_profile(ctx, profile))
    overrides = {
        "s3_snapshot_prefix": s3_prefix, "s3_endpoint_url": s3_endpoint_url,
        "aws_profile": aws_profile, "aws_cli": aws_cli,
        "aws_config_file": aws_config_file, "aws_credentials_file": aws_credentials_file,
    }
    values.update({key: value for key, value in overrides.items() if value is not None})
    return values


def _snapshot_protected_paths(values: dict) -> list[Path]:
    # These paths are never source inputs, even when Git tracks or ignores them.
    from cloudru_config import TOKEN_CACHE_PATH

    return [CONFIG_PATH, CREDENTIALS_PATH, TOKEN_CACHE_PATH,
            Path(values.get("aws_config_file") or "~/.aws/config").expanduser(),
            Path(values.get("aws_credentials_file") or "~/.aws/credentials").expanduser()]


def _snapshot_result(result: dict, as_json: bool, *, dry_run=False) -> None:
    if as_json:
        typer.echo(json.dumps(result, ensure_ascii=True, indent=2))
    elif dry_run:
        typer.echo("Snapshot dry run (no files created or uploaded):")
        typer.echo(yaml.safe_dump(result, sort_keys=False))
    else:
        typer.echo(result["archive_path"])
        if result.get("s3_uri"):
            typer.echo(f"Uploaded snapshot: {result['s3_uri']}", err=True)


@app.command("snapshot", help="Capture source as a local archive and optionally upload it to S3")
def cmd_snapshot(
    ctx: typer.Context,
    source: str = typer.Argument(..., help="Directory, repository subtree, or single file"),
    output_dir: str = typer.Option("./snapshots", "--output-dir", "-o"),
    upload: bool = typer.Option(False, "--upload"),
    use_gitignore: bool = typer.Option(True, "--use-gitignore/--no-use-gitignore"),
    exclude: Optional[list[str]] = typer.Option(None, "--exclude", help="Repeatable source-relative exclusion"),
    exclude_from: Optional[str] = typer.Option(None, "--exclude-from", help="File of exclusion patterns"),
    max_bytes: int = typer.Option(DEFAULT_MAX_BYTES, "--max-bytes", min=1),
    s3_prefix: Optional[str] = typer.Option(None, "--s3-prefix"),
    s3_endpoint_url: Optional[str] = typer.Option(None, "--s3-endpoint-url"),
    aws_profile: Optional[str] = typer.Option(None, "--aws-profile"),
    aws_cli: Optional[str] = typer.Option(None, "--aws-cli"),
    aws_config_file: Optional[str] = typer.Option(None, "--aws-config-file"),
    aws_credentials_file: Optional[str] = typer.Option(None, "--aws-credentials-file"),
    profile: Optional[str] = typer.Option(None, "--profile"),
    as_json: bool = typer.Option(False, "--json"),
    dry_run: bool = typer.Option(False, "--dry-run"),
    debug: bool = typer.Option(False, "--debug"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        if not upload and any(value is not None for value in (
                s3_prefix, s3_endpoint_url, aws_profile, aws_cli, aws_config_file, aws_credentials_file)):
            raise RuntimeError("Upload options require --upload")
        values = _snapshot_settings(ctx, profile, s3_prefix, s3_endpoint_url, aws_profile,
                                    aws_cli, aws_config_file, aws_credentials_file)
        storage = resolve_upload_config(values) if upload else None
        plan = prepare_snapshot(source, output_dir, use_gitignore=use_gitignore,
                                exclude=exclude, exclude_from=exclude_from, max_bytes=max_bytes,
                                protected_paths=_snapshot_protected_paths(values))
        if dry_run:
            result = {"dry_run": True, "operation": "create", **plan.public_dict(),
                      "upload": storage.public_dict() if storage else None}
        else:
            typer.echo(f"Capturing source: {plan.source}", err=True)
            result = create_snapshot(plan)
            if storage:
                typer.echo("Uploading snapshot to S3", err=True)
                try:
                    result = upload_snapshot(result, storage)
                except Exception:
                    typer.echo(f"Local snapshot retained: {result['archive_path']}", err=True)
                    raise
        _snapshot_result(result, as_json, dry_run=dry_run)
    except Exception as exc:
        _fail(exc, debug_mode)


@workspace_app.command("info", help="Show current workspace information")
def cmd_workspace_info(
    ctx: typer.Context,
    refresh: bool = typer.Option(False, "--refresh"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        client.workspace_info(refresh=refresh)
    except Exception as exc:
        _fail(exc, debug_mode)


@allocations_app.command("list", help="List allocations available to the workspace")
def cmd_allocations_list(
    ctx: typer.Context,
    as_json: bool = typer.Option(False, "--json", help="Print machine-readable JSON"),
    table_width: int = typer.Option(160, "--table-width"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        data = client.allocations(
            table_width=table_width,
            return_data=as_json,
            show_table=not as_json,
        )
        if as_json:
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as exc:
        _fail(exc, debug_mode)


@allocations_app.command("show", help="Show allocation details")
def cmd_allocations_show(
    ctx: typer.Context,
    allocation: str = typer.Argument(..., help="Allocation UUID or exact name", metavar="ALLOCATION"),
    as_json: bool = typer.Option(False, "--json", help="Print machine-readable JSON"),
    table_width: int = typer.Option(160, "--table-width"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        data = client.allocation_info(
            allocation,
            table_width=table_width,
            return_data=as_json,
            show_table=not as_json,
        )
        if as_json:
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as exc:
        _fail(exc, debug_mode)


@allocations_app.command("status", help="Show allocation resource status")
def cmd_allocations_status(
    ctx: typer.Context,
    allocation: str = typer.Argument(..., help="Allocation UUID or exact name", metavar="ALLOCATION"),
    as_json: bool = typer.Option(False, "--json", help="Print machine-readable JSON"),
    table_width: int = typer.Option(160, "--table-width"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        data = client.allocation_status(
            allocation,
            table_width=table_width,
            return_data=as_json,
            show_table=not as_json,
        )
        if as_json:
            typer.echo(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as exc:
        _fail(exc, debug_mode)


@resources_app.command("instance-types", help="Show supported instance types for region")
def cmd_instance_types(
    ctx: typer.Context,
    region: Optional[str] = typer.Option(None, "--region"),
    refresh_configs: bool = typer.Option(False, "--refresh-configs"),
    table_width: int = typer.Option(160, "--table-width"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, cfg = _build_client(_resolve_profile(ctx, profile))
        target_region = region or cfg.get("region")
        client.instance_types(region=target_region, refresh_configs=refresh_configs, table_width=table_width)
    except Exception as exc:
        _fail(exc, debug_mode)


@resources_app.command("available", help="Show currently available resources")
def cmd_available_resources(
    ctx: typer.Context,
    allocation_id: Optional[str] = typer.Option(None, "--allocation-id"),
    all_resources: bool = typer.Option(False, "--all", help="Show unavailable resources too"),
    refresh_workspace: bool = typer.Option(False, "--refresh-workspace"),
    table_width: int = typer.Option(160, "--table-width"),
    source: Optional[str] = typer.Option(None, "--source", case_sensitive=False),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, cfg = _build_client(_resolve_profile(ctx, profile))
        effective_source = (source or cfg.get("source") or DEFAULT_SOURCE)
        if effective_source not in VALID_SOURCES:
            raise RuntimeError(f"Invalid source '{effective_source}'. Valid values: {', '.join(VALID_SOURCES)}")

        client.available_resources(
            allocation_id=allocation_id,
            only_available=not all_resources,
            refresh_workspace=refresh_workspace,
            table_width=table_width,
            source=effective_source,
        )
    except Exception as exc:
        _fail(exc, debug_mode)


@resources_app.command("used", help="Show currently used GPUs (running/pending)")
def cmd_used_resources(
    ctx: typer.Context,
    region: Optional[list[str]] = typer.Option(None, "--region", help="Repeatable; default from profile"),
    all_profiles: bool = typer.Option(False, "--all", help="Collect from all configured profiles"),
    n: int = typer.Option(1000, "--n", min=1),
    table_width: int = typer.Option(120, "--table-width"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        if not all_profiles:
            client, cfg = _build_client(_resolve_profile(ctx, profile))
            regions = region if region else [cfg.get("region") or "SR006"]
            client.used_resources(regions=regions, n_last=n, table_width=table_width)
            return

        profiles = list_auth_profiles()
        if not profiles:
            raise RuntimeError("No profiles found. Run `cloudru init --profile <name>` first.")

        combined_rows = []
        failed_profiles = []

        total_running_jobs = 0
        total_pending_jobs = 0
        total_running_gpus = 0
        total_pending_gpus = 0

        for profile_name in profiles:
            try:
                client, cfg = _build_client(profile_name)
                regions = region if region else [cfg.get("region") or "SR006"]
                data = client.used_resources(regions=regions, n_last=n, table_width=table_width,
                                             return_data=True, show_table=False)
                if not data:
                    failed_profiles.append((profile_name, "empty response"))
                    continue

                workspace = data.get("workspace", "Unknown workspace")
                for row in data.get("rows", []):
                    combined_rows.append({
                        "profile": profile_name,
                        "workspace": workspace,
                        **row,
                    })

                totals = data.get("totals", {})
                total_running_jobs += int(totals.get("running_jobs", 0))
                total_pending_jobs += int(totals.get("pending_jobs", 0))
                total_running_gpus += int(totals.get("gpus_running", 0))
                total_pending_gpus += int(totals.get("gpus_pending", 0))
            except Exception as exc:
                failed_profiles.append((profile_name, str(exc)))

        if not combined_rows and failed_profiles:
            details = "\n".join([f"- {name}: {err}" for name, err in failed_profiles])
            raise RuntimeError(f"Failed to collect data for all profiles:\n{details}")

        table = Table(title="Used Resources (All Profiles)")
        table.add_column("profile", style="cyan")
        table.add_column("workspace", style="magenta")
        table.add_column("region", style="yellow")
        table.add_column("running_jobs", justify="right")
        table.add_column("pending_jobs", justify="right")
        table.add_column("gpus_running", justify="right", style="green")
        table.add_column("gpus_pending", justify="right", style="yellow")
        table.add_column("gpus_total", justify="right", style="cyan")

        for row in combined_rows:
            table.add_row(
                row["profile"],
                row["workspace"],
                row["region"],
                str(row["running_jobs"]),
                str(row["pending_jobs"]),
                str(row["gpus_running"]),
                str(row["gpus_pending"]),
                str(row["gpus_total"]),
            )

        totals_text = Text()
        totals_text.append("Running jobs: ", style="bold")
        totals_text.append(str(total_running_jobs))
        totals_text.append(" | Pending jobs: ", style="bold")
        totals_text.append(str(total_pending_jobs))
        totals_text.append("\n")
        totals_text.append("GPUs running: ", style="bold green")
        totals_text.append(str(total_running_gpus), style="green")
        totals_text.append(" | GPUs pending: ", style="bold yellow")
        totals_text.append(str(total_pending_gpus), style="yellow")
        totals_text.append(" | GPUs total: ", style="bold cyan")
        totals_text.append(str(total_running_gpus + total_pending_gpus), style="cyan")

        console = Console(width=table_width)
        console.print(table)
        console.print(Panel(totals_text, title="Used Resources Summary (All Profiles)"))

        if failed_profiles:
            failed_text = Text()
            for profile_name, error in failed_profiles:
                failed_text.append(f"- {profile_name}: {error}\n")
            console.print(Panel(failed_text, title="Profiles with errors"))
    except Exception as exc:
        _fail(exc, debug_mode)


@resources_app.command("cost", help="Report Cloud.ru job costs")
def cmd_resources_cost(
    ctx: typer.Context,
    since: str = typer.Option("30d", "--since", help="Creation-time window, e.g. 30m, 12h, 30d, 4w"),
    all_profiles: bool = typer.Option(False, "--all", help="Collect from all configured profiles"),
    group_by: Optional[list[str]] = typer.Option(
        None,
        "--group-by",
        help="Repeatable or comma-separated: profile, region, n_gpus",
    ),
    region: Optional[list[str]] = typer.Option(None, "--region", help="Repeatable; defaults to profile region"),
    n_gpus: Optional[int] = typer.Option(None, "--n-gpus", min=0, help="Exact allocated GPU count"),
    as_json: bool = typer.Option(False, "--json", help="Print machine-readable JSON"),
    table_width: int = typer.Option(140, "--table-width", min=60),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        report_now = datetime.now(timezone.utc)
        cutoff = _parse_cost_since(since, now=report_now)
        normalized_regions = _parse_csv_options(region or [])
        normalized_group_by = _normalize_cost_group_by(group_by or [])

        if all_profiles:
            profile_names = list_auth_profiles()
            if not profile_names:
                raise RuntimeError("No profiles found. Run `cloudru init --profile <name>` first.")
        else:
            profile_names = [_resolve_profile(ctx, profile)]

        all_jobs = []
        warnings = []
        successful_profiles = []
        workspace_region_profiles = {}
        queried_regions = {}

        for profile_name in profile_names:
            try:
                client, cfg = _build_client(profile_name)
                workspace_id = str(client.x_workspace_id or "")
                target_regions = normalized_regions or [cfg.get("region") or "SR006"]
                profile_jobs = []
                profile_regions = []
                for target_region in target_regions:
                    workspace_region = (workspace_id, target_region)
                    if workspace_id and workspace_region in workspace_region_profiles:
                        warnings.append(
                            f"profile '{profile_name}' region '{target_region}' skipped: "
                            "workspace/region already counted by profile "
                            f"'{workspace_region_profiles[workspace_region]}'"
                        )
                        continue

                    raw_jobs = client._get_all_jobs(region=target_region)
                    for raw_job in raw_jobs:
                        normalized_job = _normalize_cost_job(
                            raw_job,
                            profile=profile_name,
                            cutoff=cutoff,
                            n_gpus=n_gpus,
                        )
                        if normalized_job is not None:
                            profile_jobs.append(normalized_job)
                    profile_regions.append(target_region)

                if not profile_regions:
                    continue

                for target_region in profile_regions:
                    if workspace_id:
                        workspace_region_profiles[(workspace_id, target_region)] = profile_name
                successful_profiles.append(profile_name)
                queried_regions[profile_name] = profile_regions
                all_jobs.extend(profile_jobs)
            except Exception as exc:
                if not all_profiles:
                    raise
                if debug_mode:
                    traceback.print_exc()
                warnings.append(f"profile '{profile_name}' failed: {exc}")

        if not successful_profiles:
            details = "\n".join(f"- {warning}" for warning in warnings)
            raise RuntimeError(f"Failed to collect cost data for all profiles:\n{details}")

        rows, totals = _aggregate_cost_jobs(all_jobs, normalized_group_by)
        report = {
            "as_of": _format_utc(report_now),
            "since": since,
            "cutoff": _format_utc(cutoff),
            "group_by": normalized_group_by,
            "filters": {
                "profiles": profile_names,
                "regions": normalized_regions or "profile_default",
                "n_gpus": n_gpus,
            },
            "regions_queried": queried_regions,
            "profiles_succeeded": successful_profiles,
            "totals": totals,
            "rows": rows,
            "warnings": warnings,
        }

        if as_json:
            typer.echo(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            _render_cost_report(report, table_width=table_width)
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("list", help="List jobs by status and region")
def cmd_jobs_list(
    ctx: typer.Context,
    region: Optional[list[str]] = typer.Option(None, "--region", help="Repeatable; default from profile"),
    status: Optional[list[str]] = typer.Option(None, "--status", help="Repeatable or comma-separated"),
    status_not: Optional[list[str]] = typer.Option(None, "--status-not", help="Repeatable or comma-separated"),
    allocation_name: Optional[str] = typer.Option(None, "--allocation-name", help="Filter by allocation name"),
    n: int = typer.Option(20, "--n", min=1),
    table_width: int = typer.Option(160, "--table-width"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        normalized_status = _normalize_status_list(status or [], "--status")
        normalized_status_not = _normalize_status_list(status_not or [], "--status-not")

        client, cfg = _build_client(_resolve_profile(ctx, profile))
        regions = region if region else [cfg.get("region") or "SR006"]
        client.jobs(
            status_in=normalized_status,
            status_not_in=normalized_status_not,
            regions=regions,
            n_last=n,
            table_width=table_width,
            allocation_name=allocation_name,
        )
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("finished", help="Show most recently finished jobs")
def cmd_jobs_finished(
    ctx: typer.Context,
    region: Optional[list[str]] = typer.Option(None, "--region", help="Repeatable; default from profile"),
    status: Optional[list[str]] = typer.Option(None, "--status", help="Repeatable or comma-separated"),
    n: int = typer.Option(20, "--n", min=1),
    table_width: int = typer.Option(160, "--table-width"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        statuses = _normalize_status_list(status or [], "--status") if status else CloudRuAPIClient.TERMINAL_JOB_STATUSES
        client, cfg = _build_client(_resolve_profile(ctx, profile))
        regions = region if region else [cfg.get("region") or "SR006"]
        client.finished_jobs(regions=regions, n_last=n, status_in=statuses, table_width=table_width)
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("status", help="Show detailed status for a job")
def cmd_jobs_status(
    ctx: typer.Context,
    job_id: str,
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        client.job_status(job_id)
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("logs", help="Stream logs for a job")
def cmd_jobs_logs(
    ctx: typer.Context,
    job_id: str,
    tail: int = typer.Option(100, "--tail", min=1),
    verbose: bool = typer.Option(False, "--verbose"),
    region: Optional[str] = typer.Option(None, "--region"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, cfg = _build_client(_resolve_profile(ctx, profile))
        target_region = region or cfg.get("region") or "SR006"
        client.job_logs(job_id, tail=tail, verbose=verbose, region=target_region)
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("ssh", help="Connect to a running job over SSH")
def cmd_jobs_ssh(
    ctx: typer.Context,
    job_id: str,
    rank: int = typer.Option(0, "--rank", min=0, help="Job rank: 0 is master, N is worker N-1"),
    identity: Optional[str] = typer.Option(None, "--identity", "-i", help="Path to a private SSH key"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the SSH command without running it"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        target = client.job_ssh_target(job_id, rank=rank)
        command = _build_ssh_command(target, identity=identity)
    except Exception as exc:
        _fail(exc, debug_mode)

    _run_ssh_command(command, dry_run=dry_run, debug=debug_mode)


@jobs_app.command("ssh-config", help="Print an OpenSSH config block for a running job")
def cmd_jobs_ssh_config(
    ctx: typer.Context,
    job_id: str,
    rank: int = typer.Option(0, "--rank", min=0, help="Job rank: 0 is master, N is worker N-1"),
    identity: Optional[str] = typer.Option(
        None,
        "--identity",
        "-i",
        help="Private key path; omit only when available through ssh-agent or OpenSSH defaults",
    ),
    host_alias: Optional[str] = typer.Option(None, "--alias", help="Host alias for SSH and VS Code"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        target = client.job_ssh_target(job_id, rank=rank)
        config = _build_ssh_config(
            target,
            rank=rank,
            host_alias=host_alias,
            identity=identity,
        )
        typer.echo(config)
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("exec", help="Execute a command on a running job over SSH")
def cmd_jobs_exec(
    ctx: typer.Context,
    job_id: str,
    command_args: list[str] = typer.Argument(
        ...,
        metavar="COMMAND [ARGS]...",
        help="Remote command and arguments",
    ),
    rank: int = typer.Option(0, "--rank", min=0, help="Job rank: 0 is master, N is worker N-1"),
    identity: Optional[str] = typer.Option(None, "--identity", "-i", help="Path to a private SSH key"),
    tty: bool = typer.Option(False, "--tty", "-t", help="Allocate an SSH pseudo-terminal"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the SSH command without running it"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, _ = _build_client(_resolve_profile(ctx, profile))
        target = client.job_ssh_target(job_id, rank=rank)
        command = _build_ssh_command(
            target,
            identity=identity,
            tty=tty,
            remote_command=command_args,
        )
    except Exception as exc:
        _fail(exc, debug_mode)

    _run_ssh_command(command, dry_run=dry_run, debug=debug_mode)


@jobs_app.command("kill", help="Delete one or more jobs")
def cmd_jobs_kill(
    ctx: typer.Context,
    job_ids: list[str] = typer.Argument(..., help="One or more job IDs"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    region: Optional[str] = typer.Option(None, "--region"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        profile_name = _resolve_profile(ctx, profile)
        client, cfg = _build_client(profile_name)
        target_region = region or cfg.get("region") or "SR006"
        workspace_name = "Unknown workspace"
        try:
            workspace_info = client.get_workspace_info(refresh=False)
            if isinstance(workspace_info, dict):
                workspace_name = workspace_info.get("name") or workspace_name
        except Exception:
            pass

        if not yes:
            preview_ids = ", ".join(job_ids)
            confirmed = typer.confirm(
                (
                    f"Delete {len(job_ids)} job(s) in workspace {workspace_name} "
                    f"(profile {profile_name}, region {target_region})?\n{preview_ids}"
                ),
                default=False,
            )
            if not confirmed:
                typer.echo("Cancelled by user.")
                return

        console = Console()
        deleted_count = 0
        failed = []

        for job_id in job_ids:
            try:
                result = client.kill_job(job_id, region=target_region)
                parsed = client.render_job_delete_response(result, console=console)
                if parsed.get("ok"):
                    deleted_count += 1
                else:
                    failed.append((job_id, parsed.get("error_summary", "delete failed")))
            except Exception as exc:
                if debug_mode:
                    traceback.print_exc()
                failed.append((job_id, str(exc)))

        client.render_job_delete_summary(requested=len(job_ids), deleted=deleted_count, failed=failed, console=console)
        if failed:
            raise typer.Exit(1)
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("submit", help="Submit job from YAML with CLI overrides")
def cmd_jobs_submit(
    ctx: typer.Context,
    file: str = typer.Option(..., "-f", "--file", help="Path to YAML config with job settings"),
    script: Optional[str] = typer.Option(None, "--script"),
    base_image: Optional[str] = typer.Option(None, "--base-image"),
    instance_type: Optional[str] = typer.Option(None, "--instance-type"),
    region: Optional[str] = typer.Option(None, "--region"),
    job_type: Optional[str] = typer.Option(None, "--job-type"),
    job_desc: Optional[str] = typer.Option(None, "--job-desc"),
    allocation_name: Optional[str] = typer.Option(None, "--allocation-name"),
    queue_name: Optional[str] = typer.Option(None, "--queue-name"),
    n_workers: Optional[int] = typer.Option(None, "--n-workers", min=1),
    processes_per_worker: Optional[int] = typer.Option(None, "--processes-per-worker", min=1),
    conda_env: Optional[str] = typer.Option(None, "--conda-env"),
    env: Optional[list[str]] = typer.Option(None, "--env", help="Repeatable KEY=VALUE override"),
    workdir: Optional[str] = typer.Option(None, "--workdir"),
    shell_init: Optional[str] = typer.Option(None, "--shell-init"),
    check_hf_auth: Optional[bool] = typer.Option(None, "--check-hf-auth/--no-check-hf-auth"),
    pre_command: Optional[list[str]] = typer.Option(None, "--pre-command", help="Repeatable setup command"),
    no_bootstrap: bool = typer.Option(False, "--no-bootstrap", help="Submit raw script without setup wrapper"),
    snapshot_s3_prefix: Optional[str] = typer.Option(None, "--snapshot-s3-prefix"),
    s3_endpoint_url: Optional[str] = typer.Option(None, "--s3-endpoint-url"),
    aws_profile: Optional[str] = typer.Option(None, "--aws-profile"),
    aws_cli: Optional[str] = typer.Option(None, "--aws-cli"),
    aws_config_file: Optional[str] = typer.Option(None, "--aws-config-file"),
    aws_credentials_file: Optional[str] = typer.Option(None, "--aws-credentials-file"),
    use_gitignore: Optional[bool] = typer.Option(None, "--use-gitignore/--no-use-gitignore"),
    as_json: bool = typer.Option(False, "--json", help="Print raw JSON response"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print merged config and do not submit"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    managed = None
    try:
        selected_profile = _resolve_profile(ctx, profile)
        document = _load_job_document(file)
        setup_cfg, raw_job_cfg = _job_sections(document)
        is_managed = "snapshot" in document
        if is_managed:
            cfg = load_submit_profile(selected_profile)
        else:
            client, cfg = _build_client(selected_profile)
        submit_kwargs = dict(raw_job_cfg) if is_managed else {k: v for k, v in raw_job_cfg.items() if k in SUBMIT_JOB_ALLOWED_FIELDS}
        storage_overrides = {
            "s3_snapshot_prefix": snapshot_s3_prefix, "s3_endpoint_url": s3_endpoint_url,
            "aws_profile": aws_profile, "aws_cli": aws_cli,
            "aws_config_file": aws_config_file, "aws_credentials_file": aws_credentials_file,
        }
        if not is_managed and (use_gitignore is not None or any(value is not None for value in storage_overrides.values())):
            raise RuntimeError("Snapshot CLI options require a snapshot section in YAML")

        if "region" not in submit_kwargs or not submit_kwargs.get("region"):
            submit_kwargs["region"] = cfg.get("region") or "SR006"

        overrides = {
            "script": script,
            "base_image": base_image,
            "instance_type": instance_type,
            "region": region,
            "job_type": job_type,
            "job_desc": job_desc,
            "allocation_name": allocation_name,
            "queue_name": queue_name,
            "n_workers": n_workers,
            "processes_per_worker": processes_per_worker,
            "conda_env": conda_env,
        }
        for key, value in overrides.items():
            if value is not None:
                submit_kwargs[key] = value

        env_overrides = _parse_env_overrides(env or [])
        if env_overrides:
            env_variables = submit_kwargs.get("env_variables", {})
            if env_variables is None and not is_managed:
                env_variables = {}
            if not isinstance(env_variables, dict):
                raise RuntimeError("env_variables in YAML must be an object")
            env_variables = dict(env_variables)
            env_variables.update(env_overrides)
            submit_kwargs["env_variables"] = env_variables

        setup_effective = dict(setup_cfg)
        if conda_env is not None:
            setup_effective["conda_env"] = conda_env
        if workdir is not None:
            setup_effective["workdir"] = workdir
        if shell_init is not None:
            setup_effective["shell_init"] = shell_init
        if check_hf_auth is not None:
            setup_effective["check_hf_auth"] = check_hf_auth
        if pre_command is not None and len(pre_command) > 0:
            setup_effective["pre_command"] = pre_command

        if is_managed:
            from cloudru_job_submit import prepare_snapshot_submission

            managed = prepare_snapshot_submission(
                document, submit_kwargs, setup_effective, base=Path.cwd(),
                profile_values=cfg, storage_overrides=storage_overrides,
                allowed_job_fields=SUBMIT_JOB_ALLOWED_FIELDS, no_bootstrap=no_bootstrap,
                use_gitignore=use_gitignore)
            if dry_run:
                preview = managed.preview()
                typer.echo(json.dumps(preview, ensure_ascii=True, indent=2) if as_json else yaml.safe_dump(preview, sort_keys=False))
                return
            runtime_config, payload = managed.materialize(lambda message: typer.echo(message, err=True))
            client, _ = _build_client(selected_profile)
            typer.echo("Submitting snapshot job", err=True)
            response = client.submit_job(**payload)
            accepted_id = response.get("job_name") if isinstance(response, dict) else None
            if not isinstance(accepted_id, str) or not accepted_id.strip():
                accepted_id = None
            result = {
                "job_id": accepted_id,
                "snapshot_uri": runtime_config["snapshot"]["uri"],
                "job_dir": runtime_config["job"]["env_variables"]["CLOUDRU_JOB_DIR"],
                "response": response,
            }
            if managed.archive:
                result["archive_path"] = managed.archive["archive_path"]
            if as_json:
                typer.echo(json.dumps(result, ensure_ascii=True, indent=2))
            else:
                typer.echo(f"Job ID: {result['job_id'] or 'not returned by API'}")
                typer.echo(f"Job directory: {result['job_dir']}")
                typer.echo(f"Snapshot URI: {result['snapshot_uri']}")
            if accepted_id is None:
                raise RuntimeError("API response did not include an accepted job_name; submission is unconfirmed and will not be retried")
            return

        required = ["script", "base_image", "instance_type", "region"]
        missing = [k for k in required if not submit_kwargs.get(k)]
        if missing:
            raise RuntimeError(f"Missing required submit fields: {', '.join(missing)}")

        raw_script = submit_kwargs["script"]
        final_script = raw_script
        if not no_bootstrap and _should_use_bootstrap(setup_effective):
            final_script = _build_bootstrap_script(setup_effective, final_script)
        submit_kwargs["script"] = final_script

        if dry_run:
            dry_run_job = dict(submit_kwargs)
            dry_run_job["script"] = raw_script
            typer.echo("Dry run payload:")
            typer.echo(yaml.safe_dump({"setup": setup_effective, "job": dry_run_job}, sort_keys=False))
            typer.echo("Command to run:")
            typer.echo(final_script)
            return

        result = client.submit_job(**submit_kwargs)
        if as_json:
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
            return

        console = Console()
        parsed = client.render_submit_response(result, console=console)
        job_name = parsed.get("job_id")
        if job_name:
            console.print(f"Next: cloudru jobs status {job_name}")
            console.print(f"Next: cloudru jobs logs {job_name}")
    except Exception as exc:
        if managed is not None and managed.archive:
            typer.echo(f"Local snapshot retained: {managed.archive['archive_path']}", err=True)
        _fail(exc, debug_mode)


@bot_app.command("run", help="Run Telegram bot with polling and notifications")
def cmd_bot_run(
    ctx: typer.Context,
    profile: Optional[str] = typer.Option(None, "--profile", help="Single profile mode"),
    all_profiles: bool = typer.Option(True, "--all/--no-all", help="Use all configured profiles by default"),
    poll_interval: Optional[int] = typer.Option(None, "--poll-interval", min=10, help="Polling interval seconds"),
    debug: bool = typer.Option(False, "--debug", help="Show debug logs"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        run_bot(profile=profile, all_profiles=all_profiles, poll_interval_sec=poll_interval, debug=debug_mode)
    except Exception as exc:
        _fail(exc, debug_mode)


def main() -> int:
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
