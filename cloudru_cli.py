from __future__ import annotations

import getpass
import os
import traceback
from typing import Optional

import typer

from cloudru_config import (
    CONFIG_PATH,
    CREDENTIALS_PATH,
    file_mode,
    load_cached_token,
    load_profile,
    redact,
    save_cached_token,
    save_profile,
)
from cloudru_utils import CloudRuAPIClient


DEFAULT_SOURCE = "auto"
VALID_SOURCES = ["auto", "instance_types_available", "allocations_instance_types_availability"]

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


def main() -> int:
    app()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
