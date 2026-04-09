from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests

from cloudru_config import load_bot_config, list_auth_profiles, load_cached_token, load_profile, save_cached_token
from cloudru_utils import CloudRuAPIClient


STATE_PATH = Path.home() / ".cloudru" / "bot_state.json"
MAX_TELEGRAM_MESSAGE_LEN = 3900


def _truncate(text: str, limit: int = MAX_TELEGRAM_MESSAGE_LEN) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _split_lines(text: str, max_lines: int = 40) -> str:
    lines = text.splitlines()
    if len(lines) <= max_lines:
        return text
    return "\n".join(lines[:max_lines]) + "\n..."


class TelegramClient:
    def __init__(self, token: str, debug: bool = False):
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.debug = debug

    def get_updates(self, offset: int | None = None, timeout: int = 20) -> list[dict]:
        params = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        response = requests.get(f"{self.base_url}/getUpdates", params=params, timeout=timeout + 5)
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram getUpdates failed: {data}")
        updates = data.get("result", [])
        if self.debug and updates:
            print(f"[bot] received updates: count={len(updates)}")
        return updates

    def send_message(self, chat_id: str, text: str, reply_markup: dict | None = None) -> None:
        payload_text = _truncate(text)
        if self.debug:
            print(f"[bot] outgoing -> chat_id={chat_id}, text={payload_text!r}")
        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": payload_text,
            "disable_web_page_preview": True,
        }
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        response = requests.post(f"{self.base_url}/sendMessage", json=payload, timeout=30)
        data = response.json()
        if not data.get("ok"):
            raise RuntimeError(f"Telegram sendMessage failed: {data}")

    def answer_callback_query(self, callback_query_id: str, text: str | None = None) -> None:
        payload: dict[str, Any] = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = _truncate(text, 150)
        try:
            requests.post(f"{self.base_url}/answerCallbackQuery", json=payload, timeout=10)
        except Exception:
            pass


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {"initialized": False, "jobs": {}, "offset": None, "chat_context": {}}
    try:
        data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return {"initialized": False, "jobs": {}, "offset": None, "chat_context": {}}
        data.setdefault("initialized", False)
        data.setdefault("jobs", {})
        data.setdefault("offset", None)
        data.setdefault("chat_context", {})
        return data
    except Exception:
        return {"initialized": False, "jobs": {}, "offset": None, "chat_context": {}}


def _save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        STATE_PATH.parent.chmod(0o700)
    except Exception:
        pass
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    try:
        STATE_PATH.chmod(0o600)
    except Exception:
        pass


def _build_client(profile: str) -> tuple[CloudRuAPIClient, dict]:
    cfg = load_profile(profile=profile, include_env=True)
    if not cfg.get("client_id") or not cfg.get("client_secret"):
        raise RuntimeError(f"Missing credentials for profile '{profile}'")

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


class CloudruBotRunner:
    def __init__(self, profiles: list[str], poll_interval_sec: int, token: str, allowed_chat_ids: list[str],
                 debug: bool = False):
        self.poll_interval_sec = max(10, poll_interval_sec)
        self.debug = debug
        self.telegram = TelegramClient(token, debug=debug)
        self.allowed_chat_ids = {str(x) for x in allowed_chat_ids}
        self.state = _load_state()
        self.clients: dict[str, CloudRuAPIClient] = {}
        self.profile_cfg: dict[str, dict] = {}
        self.workspace_name: dict[str, str] = {}

        for profile in profiles:
            try:
                client, cfg = _build_client(profile)
                self.clients[profile] = client
                self.profile_cfg[profile] = cfg
                try:
                    w = client.get_workspace_info(refresh=False)
                    self.workspace_name[profile] = w.get("name", "Unknown workspace") if isinstance(w, dict) else "Unknown workspace"
                except Exception:
                    self.workspace_name[profile] = "Unknown workspace"
            except Exception as exc:
                if self.debug:
                    print(f"[bot] skip profile {profile}: {exc}")

        self.profiles = self._ordered_profiles(self.clients.keys())
        if not self.profiles:
            raise RuntimeError("No valid profiles loaded for bot")
        self.default_profile_scope = "default" if "default" in self.profiles else self.profiles[0]
        self._normalize_chat_contexts()

    @staticmethod
    def _ordered_profiles(profiles) -> list[str]:
        profiles_list = sorted(set(str(p) for p in profiles))
        if "default" in profiles_list:
            profiles_list.remove("default")
            profiles_list.insert(0, "default")
        return profiles_list

    def _chat_ctx(self, chat_id: str) -> dict:
        chat_context = self.state.setdefault("chat_context", {})
        ctx = chat_context.setdefault(chat_id, {"profile_scope": self.default_profile_scope, "pending": None})
        if "profile_scope" not in ctx:
            ctx["profile_scope"] = self.default_profile_scope
        if "pending" not in ctx:
            ctx["pending"] = None
        return ctx

    def _normalize_chat_contexts(self) -> None:
        chat_context = self.state.setdefault("chat_context", {})
        changed = False
        for chat_id, ctx_raw in list(chat_context.items()):
            if not isinstance(ctx_raw, dict):
                chat_context[chat_id] = {"profile_scope": self.default_profile_scope, "pending": None}
                changed = True
                continue

            scope = str(ctx_raw.get("profile_scope", "")).strip()
            if scope == "all":
                # Legacy state used all-profiles by default; migrate to explicit default profile.
                ctx_raw["profile_scope"] = self.default_profile_scope
                changed = True
            elif scope not in self.profiles:
                ctx_raw["profile_scope"] = self.default_profile_scope
                changed = True

            if "pending" not in ctx_raw:
                ctx_raw["pending"] = None
                changed = True

        if changed:
            _save_state(self.state)

    def _profile_regions(self, profile: str) -> list[str]:
        cfg = self.profile_cfg.get(profile, {})
        return [cfg.get("region") or "SR006"]

    def _notify(self, text: str) -> None:
        for chat_id in self.allowed_chat_ids:
            try:
                self.telegram.send_message(chat_id, text)
            except Exception as exc:
                if self.debug:
                    print(f"[bot] notify failed for chat {chat_id}: {exc}")

    def _notify_profile(self, profile: str, text: str) -> None:
        for chat_id in self.allowed_chat_ids:
            try:
                if profile not in self._scope_profiles(chat_id, default_all=False):
                    continue
                self.telegram.send_message(chat_id, text)
            except Exception as exc:
                if self.debug:
                    print(f"[bot] scoped notify failed for chat {chat_id}, profile {profile}: {exc}")

    def _watch_once(self) -> None:
        jobs_state = self.state.setdefault("jobs", {})
        current = {}

        for profile in self.profiles:
            client = self.clients[profile]
            try:
                rows = client.jobs(
                    status_in=[],
                    status_not_in=[],
                    regions=self._profile_regions(profile),
                    n_last=500,
                    return_data=True,
                    show_table=False,
                ) or []
            except Exception as exc:
                if self.debug:
                    print(f"[bot] jobs fetch failed profile={profile}: {exc}")
                continue

            for row in rows:
                job_id = str(row.get("job_id") or "")
                if not job_id:
                    continue
                key = f"{profile}:{job_id}"
                current[key] = {
                    "profile": profile,
                    "workspace": self.workspace_name.get(profile, "Unknown workspace"),
                    "region": row.get("region", ""),
                    "job_id": job_id,
                    "job_desc": str(row.get("job_desc") or "-").strip() or "-",
                    "status": str(row.get("status", "Unknown")),
                    "gpu_count": int(row.get("gpu_count", 0) or 0),
                }

        initialized = bool(self.state.get("initialized", False))
        if not initialized:
            self.state["jobs"] = current
            self.state["initialized"] = True
            _save_state(self.state)
            return

        terminal = set(CloudRuAPIClient.TERMINAL_JOB_STATUSES)
        for key, item in current.items():
            prev = jobs_state.get(key)
            prev_status = prev.get("status") if isinstance(prev, dict) else None
            new_status = item.get("status")
            if prev_status and prev_status != new_status and new_status in terminal:
                text = (
                    f"[{item['profile']}/{item['workspace']}] Job finished\n"
                    f"Job: {item['job_id']}\n"
                    f"Description: {item.get('job_desc', '-')}\n"
                    f"Status: {new_status}\n"
                    f"Region: {item['region']} | GPUs: {item['gpu_count']}"
                )
                self._notify_profile(item["profile"], text)

        self.state["jobs"] = current
        _save_state(self.state)

    @staticmethod
    def _menu_main() -> dict:
        return {
            "inline_keyboard": [
                [{"text": "Workspace", "callback_data": "m:workspace"},
                 {"text": "Resources", "callback_data": "m:resources"}],
                [{"text": "Jobs", "callback_data": "m:jobs"}],
                [{"text": "Profile Scope", "callback_data": "m:profiles:main"}],
                [{"text": "Help", "callback_data": "a:help"}],
            ]
        }

    @staticmethod
    def _menu_workspace() -> dict:
        return {
            "inline_keyboard": [
                [{"text": "Info", "callback_data": "a:workspace:info"}],
                [{"text": "Profile Scope", "callback_data": "m:profiles:workspace"}],
                [{"text": "Back", "callback_data": "m:main"}],
            ]
        }

    @staticmethod
    def _menu_resources() -> dict:
        return {
            "inline_keyboard": [
                [{"text": "Instance Types", "callback_data": "a:resources:instance_types"}],
                [{"text": "Available", "callback_data": "a:resources:available"}],
                [{"text": "Used", "callback_data": "a:resources:used"}],
                [{"text": "Profile Scope", "callback_data": "m:profiles:resources"}],
                [{"text": "Back", "callback_data": "m:main"}],
            ]
        }

    @staticmethod
    def _menu_jobs() -> dict:
        return {
            "inline_keyboard": [
                [{"text": "List", "callback_data": "a:jobs:list"},
                 {"text": "Finished", "callback_data": "a:jobs:finished"}],
                [{"text": "Running Only", "callback_data": "a:jobs:running"}],
                [{"text": "Status (by id)", "callback_data": "a:jobs:status"}],
                [{"text": "Logs (by id)", "callback_data": "a:jobs:logs"}],
                [{"text": "Kill (by id)", "callback_data": "a:jobs:kill"}],
                [{"text": "Profile Scope", "callback_data": "m:profiles:jobs"}],
                [{"text": "Back", "callback_data": "m:main"}],
            ]
        }

    def _menu_profiles(self, back_menu: str) -> dict:
        rows = [
            [{"text": "All Profiles", "callback_data": f"p:set:all:{back_menu}"}],
        ]
        for profile in self._ordered_profiles(self.profiles):
            rows.append([{"text": profile, "callback_data": f"p:set:{profile}:{back_menu}"}])
        rows.append([{"text": "Back", "callback_data": f"m:{back_menu}"}])
        return {"inline_keyboard": rows}

    @staticmethod
    def _menu_confirm_kill() -> dict:
        return {
            "inline_keyboard": [
                [{"text": "Confirm Kill", "callback_data": "k:yes"},
                 {"text": "Cancel", "callback_data": "k:no"}],
            ]
        }

    def _scope_profiles(self, chat_id: str, default_all: bool = True) -> list[str]:
        ctx = self._chat_ctx(chat_id)
        scope = ctx.get("profile_scope", self.default_profile_scope)
        if scope == "all":
            return self.profiles
        if scope in self.profiles:
            return [scope]
        return self.profiles if default_all else [self.default_profile_scope]

    def _format_jobs_summary(self, profile: str, n: int = 5, finished: bool = False) -> str:
        client = self.clients[profile]
        if finished:
            rows = client.finished_jobs(
                regions=self._profile_regions(profile),
                n_last=max(n, 20),
                status_in=CloudRuAPIClient.TERMINAL_JOB_STATUSES,
                return_data=True,
                show_table=False,
            ) or []
        else:
            rows = client.jobs(
                status_in=[],
                status_not_in=[],
                regions=self._profile_regions(profile),
                n_last=max(n, 20),
                return_data=True,
                show_table=False,
            ) or []
        rows = rows[:n]
        header = "recent finished jobs" if finished else "recent jobs"
        lines = [f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}] {header}:"]
        for row in rows:
            desc = str(row.get('job_desc') or '-').strip()
            if finished:
                time_str = str(row.get('finished_dt_display') or row.get('time_display') or 'unknown')
                time_label = 'finished'
            else:
                time_str = str(row.get('created_dt_display') or row.get('time_display') or 'unknown')
                time_label = 'started'
            lines.append(
                f"- {row.get('status')} | {time_label}={time_str} | {desc} | gpus={row.get('gpu_count', 0)} | id={row.get('job_id')}"
            )
        return "\n".join(lines) if lines else f"[{profile}] no jobs found"

    def _format_running_jobs_summary(self, profile: str, n: int = 5) -> tuple[str, int]:
        client = self.clients[profile]
        rows = client.jobs(
            status_in=['Running'],
            status_not_in=[],
            regions=self._profile_regions(profile),
            n_last=max(n, 20),
            return_data=True,
            show_table=False,
        ) or []
        rows = rows[:n]
        running_count = len(rows)
        lines = [f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}] running jobs: {running_count}"]
        for row in rows:
            desc = str(row.get('job_desc') or '-').strip()
            started = str(row.get('created_dt_display') or row.get('time_display') or 'unknown')
            lines.append(
                f"- started={started} | {desc} | gpus={row.get('gpu_count', 0)} | id={row.get('job_id')}"
            )
        return "\n".join(lines), running_count

    @staticmethod
    def _help_text() -> str:
        return (
            "Use buttons below.\n"
            "Text commands still supported:\n"
            "/help\n"
            "/jobs [n] [profile]\n"
            "/status <job_id> [profile]\n"
            "/logs <job_id> [tail] [profile]\n"
            "/resources_used [profile|all]\n"
            "/resources_available [profile|all] [region]\n"
            "/instance_types [profile|all] [region]"
        )

    def _resolve_profiles_arg(self, args: list[str], chat_id: str, default_all: bool = True) -> list[str]:
        if args and args[0] in self.profiles:
            return [args[0]]
        if args and args[0].lower() == "all":
            return self._ordered_profiles(self.profiles)
        return self._ordered_profiles(self._scope_profiles(chat_id, default_all=default_all))

    def _find_job_matches(self, job_id: str, profiles: list[str]) -> list[str]:
        matches = []
        for profile in profiles:
            client = self.clients[profile]
            try:
                status = client.job_status(job_id, return_data=True, show_output=False)
                if isinstance(status, dict) and status.get("job_id"):
                    matches.append(profile)
            except Exception:
                continue
        return matches

    def _handle_text_command(self, chat_id: str, text: str) -> tuple[str, dict | None]:
        parts = text.strip().split()
        if not parts:
            return "", None
        cmd = parts[0].lower()
        args = parts[1:]

        if cmd in {"/help", "/start", "/menu"}:
            return self._help_text(), self._menu_main()

        if cmd == "/jobs":
            n = 5
            rest = args
            if args and args[0].isdigit():
                n = max(1, min(50, int(args[0])))
                rest = args[1:]
            profiles = self._resolve_profiles_arg(rest, chat_id, default_all=True)
            return _truncate("\n\n".join(self._format_jobs_summary(p, n=n) for p in profiles)), None

        if cmd == "/status":
            if not args:
                return "Usage: /status <job_id> [profile]", None
            job_id = args[0]
            profiles = self._resolve_profiles_arg(args[1:], chat_id, default_all=True)
            matches = self._find_job_matches(job_id, profiles)
            if not matches:
                return f"Job not found: {job_id}", None
            if len(matches) > 1:
                profiles_list = ", ".join(matches)
                return f"Job found in multiple profiles: {profiles_list}. Use /status {job_id} <profile>.", None

            profile = matches[0]
            status = self.clients[profile].job_status(job_id, return_data=True, show_output=False) or {}
            return (
                f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}]\n"
                f"Job: {status.get('job_id')}\n"
                f"Status: {status.get('status')}\n"
                f"Error: {status.get('error_code')} {status.get('error_message', '')}"
            ), None

        if cmd == "/logs":
            if not args:
                return "Usage: /logs <job_id> [tail] [profile]", None
            job_id = args[0]
            tail = 30
            rest = args[1:]
            if rest and rest[0].isdigit():
                tail = max(1, min(200, int(rest[0])))
                rest = rest[1:]
            profiles = self._resolve_profiles_arg(rest, chat_id, default_all=True)

            for profile in profiles:
                client = self.clients[profile]
                region = self._profile_regions(profile)[0]
                try:
                    lines = client.job_logs(job_id, tail=tail, verbose=False, region=region, return_data=True, show_output=False) or []
                    if lines:
                        out = "\n".join(lines)
                        return _truncate(f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}]\n" + _split_lines(out, 50)), None
                except Exception:
                    continue
            return f"Logs not available for job: {job_id}", None

        if cmd == "/resources_used":
            if args and args[0] in self.profiles:
                profiles = [args[0]]
            else:
                # Keep bot behavior aligned with CLI: no explicit profile means aggregate all profiles.
                profiles = self._ordered_profiles(self.profiles)

            out = []
            total_running_jobs = 0
            total_pending_jobs = 0
            total_gpus_running = 0
            total_gpus_pending = 0

            for profile in profiles:
                client = self.clients[profile]
                data = client.used_resources(regions=self._profile_regions(profile), n_last=500, return_data=True, show_table=False)
                totals = data.get("totals", {}) if isinstance(data, dict) else {}
                running_jobs = int(totals.get("running_jobs", 0) or 0)
                pending_jobs = int(totals.get("pending_jobs", 0) or 0)
                gpus_running = int(totals.get("gpus_running", 0) or 0)
                gpus_pending = int(totals.get("gpus_pending", 0) or 0)
                gpus_total = int(totals.get("gpus_total", gpus_running + gpus_pending) or 0)

                total_running_jobs += running_jobs
                total_pending_jobs += pending_jobs
                total_gpus_running += gpus_running
                total_gpus_pending += gpus_pending

                out.append(
                    "\n".join([
                        f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}]",
                        f"running_jobs: {running_jobs}",
                        f"pending_jobs: {pending_jobs}",
                        f"gpus_running: {gpus_running}",
                        f"gpus_pending: {gpus_pending}",
                        f"gpus_total: {gpus_total}",
                    ])
                )

            if len(profiles) > 1:
                out.append(
                    "\n".join([
                        "[all_profiles/total]",
                        f"running_jobs: {total_running_jobs}",
                        f"pending_jobs: {total_pending_jobs}",
                        f"gpus_running: {total_gpus_running}",
                        f"gpus_pending: {total_gpus_pending}",
                        f"gpus_total: {total_gpus_running + total_gpus_pending}",
                    ])
                )
            return "\n\n".join(out), None

        if cmd == "/resources_available":
            profiles = self._resolve_profiles_arg(args[:1], chat_id, default_all=True)
            region_override = args[1] if len(args) > 1 else None
            out = []
            for profile in profiles:
                client = self.clients[profile]
                try:
                    data = client.available_resources(only_available=True, return_data=True, show_table=False)
                    rows_out = []
                    if isinstance(data, dict):
                        for _, rows in data.items():
                            for row in rows:
                                if region_override and str(row.get("region")) != region_override:
                                    continue
                                rows_out.append(row)

                    rows_out = sorted(rows_out, key=lambda r: (-int(r.get("available", 0)), str(r.get("instance_type", ""))))
                    header = f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}] available resources"
                    lines = [header]
                    if not rows_out:
                        lines.append("- no available resources")
                    else:
                        for row in rows_out:
                            lines.append(
                                f"- {row.get('instance_type', '')}: {row.get('available', 0)}"
                            )
                    out.append("\n".join(lines))
                except Exception as exc:
                    out.append(f"[{profile}] error: {exc}")
            return _truncate("\n\n".join(out)), None

        if cmd == "/instance_types":
            profiles = self._resolve_profiles_arg(args[:1], chat_id, default_all=True)
            region_override = args[1] if len(args) > 1 else None
            out = []
            for profile in profiles:
                client = self.clients[profile]
                region = region_override or self._profile_regions(profile)[0]
                try:
                    rows = client.instance_types(region=region, return_data=True, show_table=False)
                    out.append(
                        f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}] "
                        f"region={region}, instance_types={len(rows or [])}"
                    )
                except Exception as exc:
                    out.append(f"[{profile}] error: {exc}")
            return "\n".join(out), None

        return "Unknown command. Use /help or buttons.", self._menu_main()

    def _render_menu(self, menu_name: str, chat_id: str) -> tuple[str, dict]:
        scope = self._chat_ctx(chat_id).get("profile_scope", self.default_profile_scope)
        scope_text = f"Profile scope: {scope}"
        if menu_name == "main":
            return f"Main menu\n{scope_text}", self._menu_main()
        if menu_name == "workspace":
            return f"Workspace menu\n{scope_text}", self._menu_workspace()
        if menu_name == "resources":
            return f"Resources menu\n{scope_text}", self._menu_resources()
        if menu_name == "jobs":
            return f"Jobs menu\n{scope_text}", self._menu_jobs()
        return f"Main menu\n{scope_text}", self._menu_main()

    def _handle_pending_text(self, chat_id: str, text: str) -> tuple[str, dict | None]:
        ctx = self._chat_ctx(chat_id)
        pending = ctx.get("pending")
        if not pending:
            return "", None

        action = pending.get("action")
        if action == "jobs_status":
            ctx["pending"] = None
            _save_state(self.state)
            job_id = text.strip().split()[0]
            return self._handle_text_command(chat_id, f"/status {job_id}")

        if action == "jobs_logs":
            ctx["pending"] = None
            _save_state(self.state)
            parts = text.strip().split()
            if not parts:
                return "Usage: <job_id> [tail]", self._menu_jobs()
            job_id = parts[0]
            tail = parts[1] if len(parts) > 1 and parts[1].isdigit() else "30"
            return self._handle_text_command(chat_id, f"/logs {job_id} {tail}")

        if action == "jobs_kill":
            job_ids = [x.strip() for x in text.split() if x.strip()]
            if not job_ids:
                return "Send one or more job IDs separated by spaces.", self._menu_jobs()
            ctx["pending"] = {"action": "jobs_kill_confirm", "job_ids": job_ids}
            _save_state(self.state)
            return (
                f"Confirm delete for {len(job_ids)} job(s):\n" + ", ".join(job_ids),
                self._menu_confirm_kill(),
            )

        return "", None

    def _execute_kill(self, chat_id: str, job_ids: list[str]) -> str:
        profiles = self._scope_profiles(chat_id, default_all=True)
        deleted = 0
        failed = []

        for job_id in job_ids:
            matches = self._find_job_matches(job_id, profiles)
            if not matches:
                failed.append(f"{job_id}: not found")
                continue
            if len(matches) > 1:
                failed.append(f"{job_id}: found in multiple profiles ({', '.join(matches)}), choose specific profile scope")
                continue

            profile = matches[0]
            client = self.clients[profile]
            region = self._profile_regions(profile)[0]
            try:
                res = client.kill_job(job_id, region=region)
                error_code = str(res.get("error_code", "1")) if isinstance(res, dict) else "1"
                status = str(res.get("status", "")) if isinstance(res, dict) else ""
                if error_code in {"0", "0.0"} and status.lower() == "deleted":
                    deleted += 1
                else:
                    failed.append(f"{job_id}: error_code={error_code}, status={status}")
            except Exception as exc:
                failed.append(f"{job_id}: {exc}")

        msg = f"Delete summary: requested={len(job_ids)}, deleted={deleted}, failed={len(failed)}"
        if failed:
            msg += "\n" + "\n".join(f"- {x}" for x in failed)
        return _truncate(msg)

    def _handle_callback(self, chat_id: str, data: str) -> tuple[str, dict | None]:
        ctx = self._chat_ctx(chat_id)

        if data.startswith("m:"):
            parts = data.split(":")
            if len(parts) >= 2 and parts[1] == "profiles":
                back_menu = parts[2] if len(parts) > 2 else "main"
                return "Choose profile scope", self._menu_profiles(back_menu)
            menu_name = parts[1] if len(parts) > 1 else "main"
            return self._render_menu(menu_name, chat_id)

        if data.startswith("p:set:"):
            # p:set:<scope>:<back>
            parts = data.split(":", 3)
            if len(parts) < 4:
                return "Invalid profile action", self._menu_main()
            scope = parts[2]
            back = parts[3]
            if scope == "all" or scope in self.profiles:
                ctx["profile_scope"] = scope
                _save_state(self.state)
            text, keyboard = self._render_menu(back, chat_id)
            return f"Profile scope set to: {ctx.get('profile_scope', self.default_profile_scope)}\n\n{text}", keyboard

        if data.startswith("a:"):
            if data == "a:help":
                return self._help_text(), self._menu_main()

            if data == "a:workspace:info":
                profiles = self._scope_profiles(chat_id, default_all=True)
                out = []
                for profile in profiles:
                    client = self.clients[profile]
                    info = client.get_workspace_info(refresh=False)
                    if isinstance(info, dict):
                        out.append(
                            f"[{profile}] {info.get('name')}\n"
                            f"id={info.get('id')}\nnamespace={info.get('namespace')}\n"
                            f"project={info.get('project_name')}"
                        )
                return "\n\n".join(out) if out else "No workspace info", self._menu_workspace()

            if data == "a:resources:instance_types":
                profiles = self._scope_profiles(chat_id, default_all=True)
                out = []
                for profile in profiles:
                    client = self.clients[profile]
                    region = self._profile_regions(profile)[0]
                    rows = client.instance_types(region=region, return_data=True, show_table=False)
                    out.append(f"[{profile}/{self.workspace_name.get(profile, 'Unknown workspace')}] region={region}, instance_types={len(rows or [])}")
                return "\n".join(out), self._menu_resources()

            if data == "a:resources:available":
                return self._handle_text_command(chat_id, "/resources_available")[0], self._menu_resources()

            if data == "a:resources:used":
                return self._handle_text_command(chat_id, "/resources_used")[0], self._menu_resources()

            if data == "a:jobs:list":
                return self._handle_text_command(chat_id, "/jobs 10")[0], self._menu_jobs()

            if data == "a:jobs:finished":
                profiles = self._scope_profiles(chat_id, default_all=True)
                return _truncate("\n\n".join(self._format_jobs_summary(p, n=10, finished=True) for p in profiles)), self._menu_jobs()

            if data == "a:jobs:running":
                profiles = self._scope_profiles(chat_id, default_all=True)
                blocks = []
                total_running = 0
                for profile in profiles:
                    block, count = self._format_running_jobs_summary(profile, n=10)
                    blocks.append(block)
                    total_running += count
                text = f"Total running jobs: {total_running}"
                if blocks:
                    text += "\n\n" + "\n\n".join(blocks)
                return _truncate(text), self._menu_jobs()

            if data == "a:jobs:status":
                ctx["pending"] = {"action": "jobs_status"}
                _save_state(self.state)
                return "Send job_id for status", self._menu_jobs()

            if data == "a:jobs:logs":
                ctx["pending"] = {"action": "jobs_logs"}
                _save_state(self.state)
                return "Send: <job_id> [tail]", self._menu_jobs()

            if data == "a:jobs:kill":
                ctx["pending"] = {"action": "jobs_kill"}
                _save_state(self.state)
                return "Send one or more job IDs (space-separated) to delete", self._menu_jobs()

        if data.startswith("k:"):
            if data == "k:no":
                ctx["pending"] = None
                _save_state(self.state)
                return "Kill cancelled.", self._menu_jobs()
            if data == "k:yes":
                pending = ctx.get("pending") or {}
                if pending.get("action") != "jobs_kill_confirm":
                    return "No pending kill confirmation.", self._menu_jobs()
                job_ids = pending.get("job_ids", [])
                ctx["pending"] = None
                _save_state(self.state)
                return self._execute_kill(chat_id, job_ids), self._menu_jobs()

        return "Unknown action", self._menu_main()

    def run(self) -> None:
        self._notify(f"cloudru bot started. Profiles: {', '.join(self.profiles)}")
        offset = self.state.get("offset")
        next_watch = time.time()

        while True:
            now = time.time()
            if now >= next_watch:
                self._watch_once()
                next_watch = now + self.poll_interval_sec

            timeout = max(1, min(20, int(next_watch - time.time())))
            try:
                updates = self.telegram.get_updates(offset=offset, timeout=timeout)
            except Exception as exc:
                if self.debug:
                    print(f"[bot] getUpdates error: {exc}")
                time.sleep(2)
                continue

            for update in updates:
                offset = update.get("update_id", 0) + 1

                callback = update.get("callback_query")
                if callback:
                    callback_id = callback.get("id")
                    message = callback.get("message") or {}
                    chat = message.get("chat") or {}
                    chat_id = str(chat.get("id", ""))
                    data = callback.get("data", "")

                    if self.debug:
                        chat_type = chat.get("type", "unknown")
                        chat_title = chat.get("title") or chat.get("username") or chat.get("first_name") or "unknown"
                        print(
                            "[bot] incoming callback <- "
                            f"chat_id={chat_id}, chat_type={chat_type}, chat_name={chat_title}, data={data!r}"
                        )

                    if chat_id in self.allowed_chat_ids:
                        try:
                            response_text, keyboard = self._handle_callback(chat_id, data)
                            self.telegram.send_message(chat_id, response_text, reply_markup=keyboard)
                        except Exception as exc:
                            if self.debug:
                                print(f"[bot] callback error: {exc}")
                            self.telegram.send_message(chat_id, f"Error: {exc}")
                    elif self.debug:
                        print(f"[bot] unauthorized callback from chat_id={chat_id}")

                    if callback_id:
                        self.telegram.answer_callback_query(callback_id)
                    continue

                message = update.get("message") or {}
                chat = message.get("chat") or {}
                chat_id = str(chat.get("id", ""))
                text = message.get("text", "")

                if self.debug:
                    chat_type = chat.get("type", "unknown")
                    chat_title = chat.get("title") or chat.get("username") or chat.get("first_name") or "unknown"
                    print(
                        "[bot] incoming <- "
                        f"chat_id={chat_id}, chat_type={chat_type}, chat_name={chat_title}, text={text!r}"
                    )

                if chat_id not in self.allowed_chat_ids:
                    if self.debug:
                        print(f"[bot] unauthorized chat message: chat_id={chat_id}, text={text!r}")
                    continue

                if text.startswith("/"):
                    try:
                        response_text, keyboard = self._handle_text_command(chat_id, text)
                        if response_text:
                            self.telegram.send_message(chat_id, response_text, reply_markup=keyboard)
                    except Exception as exc:
                        if self.debug:
                            print(f"[bot] command error: {exc}")
                        self.telegram.send_message(chat_id, f"Error: {exc}", reply_markup=self._menu_main())
                else:
                    try:
                        response_text, keyboard = self._handle_pending_text(chat_id, text)
                        if response_text:
                            self.telegram.send_message(chat_id, response_text, reply_markup=keyboard)
                    except Exception as exc:
                        if self.debug:
                            print(f"[bot] pending handler error: {exc}")
                        self.telegram.send_message(chat_id, f"Error: {exc}", reply_markup=self._menu_main())

            self.state["offset"] = offset
            _save_state(self.state)


def run_bot(profile: str | None = None, all_profiles: bool = True, poll_interval_sec: int | None = None,
            debug: bool = False) -> None:
    bot_cfg = load_bot_config()
    token = bot_cfg.get("telegram_bot_token")
    allowed_chat_ids = bot_cfg.get("telegram_allowed_chat_ids") or []
    interval = poll_interval_sec or bot_cfg.get("telegram_poll_interval_sec", 60)

    if not token:
        raise RuntimeError("Telegram bot token is missing. Set [bot].token in ~/.cloudru/telegram.ini or CLOUDRU_TELEGRAM_BOT_TOKEN")
    if not allowed_chat_ids:
        raise RuntimeError("Allowed chat IDs are missing. Set [bot].allowed_chat_ids in ~/.cloudru/telegram.ini or CLOUDRU_TELEGRAM_ALLOWED_CHAT_IDS")

    if profile:
        profiles = [profile]
    elif all_profiles:
        profiles = list_auth_profiles()
    else:
        profiles = ["default"]

    runner = CloudruBotRunner(
        profiles=profiles,
        poll_interval_sec=interval,
        token=token,
        allowed_chat_ids=allowed_chat_ids,
        debug=debug,
    )
    runner.run()
