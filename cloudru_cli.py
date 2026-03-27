from __future__ import annotations

import getpass
import json
import os
import shlex
import traceback
from typing import Optional
from datetime import datetime

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
    list_profiles,
    load_cached_token,
    load_profile,
    redact,
    save_cached_token,
    save_profile,
)
from cloudru_utils import CloudRuAPIClient


DEFAULT_SOURCE = "auto"
VALID_SOURCES = ["auto", "instance_types_available", "allocations_instance_types_availability"]

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
}

app = typer.Typer(help="Cloud.ru jobs helper CLI", no_args_is_help=True, add_completion=True)
workspace_app = typer.Typer(help="Workspace commands", no_args_is_help=True)
resources_app = typer.Typer(help="Resources commands", no_args_is_help=True)
jobs_app = typer.Typer(help="Jobs commands", no_args_is_help=True)

app.add_typer(workspace_app, name="workspace")
app.add_typer(resources_app, name="resources")
app.add_typer(jobs_app, name="jobs")


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


def _load_job_yaml(path: str) -> tuple[dict, dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if not isinstance(data, dict):
        raise RuntimeError("YAML file must contain an object at top level")

    setup_cfg = data.get("setup", {})
    if setup_cfg is None:
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


@app.command("init")
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
    except Exception as exc:
        _fail(exc, debug_mode)


@workspace_app.command("info")
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


@resources_app.command("instance-types")
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


@resources_app.command("available")
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


@resources_app.command("used")
def cmd_used_resources(
    ctx: typer.Context,
    region: Optional[list[str]] = typer.Option(None, "--region", help="Repeatable; default from profile"),
    all_profiles: bool = typer.Option(False, "--all", help="Collect from all configured profiles"),
    n: int = typer.Option(1000, "--n", min=1),
    table_width: int = typer.Option(160, "--table-width"),
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

        profiles = list_profiles()
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


@jobs_app.command("list")
def cmd_jobs_list(
    ctx: typer.Context,
    region: Optional[list[str]] = typer.Option(None, "--region", help="Repeatable; default from profile"),
    status: Optional[list[str]] = typer.Option(None, "--status", help="Repeatable or comma-separated"),
    status_not: Optional[list[str]] = typer.Option(None, "--status-not", help="Repeatable or comma-separated"),
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
        )
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("status")
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


@jobs_app.command("logs")
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


@jobs_app.command("kill")
def cmd_jobs_kill(
    ctx: typer.Context,
    job_id: str,
    region: Optional[str] = typer.Option(None, "--region"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, cfg = _build_client(_resolve_profile(ctx, profile))
        target_region = region or cfg.get("region") or "SR006"
        result = client.kill_job(job_id, region=target_region)
        typer.echo(result)
    except Exception as exc:
        _fail(exc, debug_mode)


@jobs_app.command("submit")
def cmd_jobs_submit(
    ctx: typer.Context,
    file: str = typer.Option(..., "-f", "--file", help="Path to YAML config with job settings"),
    script: Optional[str] = typer.Option(None, "--script"),
    base_image: Optional[str] = typer.Option(None, "--base-image"),
    instance_type: Optional[str] = typer.Option(None, "--instance-type"),
    region: Optional[str] = typer.Option(None, "--region"),
    job_type: Optional[str] = typer.Option(None, "--job-type"),
    job_desc: Optional[str] = typer.Option(None, "--job-desc"),
    n_workers: Optional[int] = typer.Option(None, "--n-workers", min=1),
    processes_per_worker: Optional[int] = typer.Option(None, "--processes-per-worker", min=1),
    conda_env: Optional[str] = typer.Option(None, "--conda-env"),
    env: Optional[list[str]] = typer.Option(None, "--env", help="Repeatable KEY=VALUE override"),
    workdir: Optional[str] = typer.Option(None, "--workdir"),
    shell_init: Optional[str] = typer.Option(None, "--shell-init"),
    check_hf_auth: Optional[bool] = typer.Option(None, "--check-hf-auth/--no-check-hf-auth"),
    pre_command: Optional[list[str]] = typer.Option(None, "--pre-command", help="Repeatable setup command"),
    no_bootstrap: bool = typer.Option(False, "--no-bootstrap", help="Submit raw script without setup wrapper"),
    as_json: bool = typer.Option(False, "--json", help="Print raw JSON response"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print merged config and do not submit"),
    profile: Optional[str] = typer.Option(None, "--profile", help="Profile name"),
    debug: bool = typer.Option(False, "--debug", help="Show full traceback on errors"),
) -> None:
    debug_mode = _resolve_debug(ctx, debug)
    try:
        client, cfg = _build_client(_resolve_profile(ctx, profile))

        setup_cfg, raw_job_cfg = _load_job_yaml(file)
        submit_kwargs = {k: v for k, v in raw_job_cfg.items() if k in SUBMIT_JOB_ALLOWED_FIELDS}

        if "region" not in submit_kwargs or not submit_kwargs.get("region"):
            submit_kwargs["region"] = cfg.get("region") or "SR006"

        overrides = {
            "script": script,
            "base_image": base_image,
            "instance_type": instance_type,
            "region": region,
            "job_type": job_type,
            "job_desc": job_desc,
            "n_workers": n_workers,
            "processes_per_worker": processes_per_worker,
            "conda_env": conda_env,
        }
        for key, value in overrides.items():
            if value is not None:
                submit_kwargs[key] = value

        env_overrides = _parse_env_overrides(env or [])
        if env_overrides:
            env_variables = submit_kwargs.get("env_variables") or {}
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
        if isinstance(result, dict) and result.get("job_name"):
            status = str(result.get("status", "Unknown"))
            status_style = CloudRuAPIClient.STATUS_STYLES.get(status.capitalize(), "white")

            created_at = result.get("created_at")
            created_str = "Unknown"
            try:
                if created_at is not None:
                    created_str = datetime.fromtimestamp(float(created_at)).strftime("%Y-%m-%d %H:%M:%S")
            except (TypeError, ValueError, OSError):
                created_str = str(created_at)

            info = Text()
            info.append("Job ID: ", style="bold")
            info.append(f"{result.get('job_name')}\n")
            info.append("Status: ", style="bold")
            info.append(f"{status}\n", style=status_style)
            info.append("Created: ", style="bold")
            info.append(created_str)

            console.print(Panel(info, title="Job Submitted"))
            console.print(f"Next: cloudru jobs status {result.get('job_name')}")
            console.print(f"Next: cloudru jobs logs {result.get('job_name')}")
        else:
            console.print(Panel(json.dumps(result, ensure_ascii=False, indent=2), title="Submit Response"))
    except Exception as exc:
        _fail(exc, debug_mode)


def main() -> int:
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
