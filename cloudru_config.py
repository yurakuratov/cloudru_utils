from __future__ import annotations

import configparser
import os
import stat
from pathlib import Path


CONFIG_DIR = Path.home() / ".cloudru"
CONFIG_PATH = CONFIG_DIR / "config"
CREDENTIALS_PATH = CONFIG_DIR / "credentials"
TOKEN_CACHE_PATH = CONFIG_DIR / "token_cache"
TELEGRAM_CONFIG_PATH = CONFIG_DIR / "telegram.ini"


def _read_ini(path: Path) -> configparser.ConfigParser:
    parser = configparser.ConfigParser()
    if path.exists():
        parser.read(path)
    return parser


def ensure_storage() -> None:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        CONFIG_DIR.chmod(0o700)
    except OSError:
        pass

    if not CONFIG_PATH.exists():
        CONFIG_PATH.touch()
    if not CREDENTIALS_PATH.exists():
        CREDENTIALS_PATH.touch()
    if not TOKEN_CACHE_PATH.exists():
        TOKEN_CACHE_PATH.touch()
    if not TELEGRAM_CONFIG_PATH.exists():
        TELEGRAM_CONFIG_PATH.touch()

    try:
        CREDENTIALS_PATH.chmod(0o600)
    except OSError:
        pass

    try:
        TOKEN_CACHE_PATH.chmod(0o600)
    except OSError:
        pass


def load_profile(profile: str = "default", include_env: bool = True) -> dict:
    ensure_storage()
    config = _read_ini(CONFIG_PATH)
    credentials = _read_ini(CREDENTIALS_PATH)

    data = {
        "profile": profile,
        "client_id": None,
        "client_secret": None,
        "x_api_key": None,
        "x_workspace_id": None,
        "region": None,
        "source": None,
    }

    if credentials.has_section(profile):
        section = credentials[profile]
        data["client_id"] = section.get("client_id")
        data["client_secret"] = section.get("client_secret")
        data["x_api_key"] = section.get("x_api_key")
        data["x_workspace_id"] = section.get("x_workspace_id")

    if config.has_section(profile):
        section = config[profile]
        data["region"] = section.get("region")
        data["source"] = section.get("source")

    if include_env:
        data["client_id"] = os.getenv("CLOUDRU_CLIENT_ID", data["client_id"])
        data["client_secret"] = os.getenv("CLOUDRU_CLIENT_SECRET", data["client_secret"])
        data["x_api_key"] = os.getenv("CLOUDRU_X_API_KEY", data["x_api_key"])
        data["x_workspace_id"] = os.getenv("CLOUDRU_X_WORKSPACE_ID", data["x_workspace_id"])
        data["region"] = os.getenv("CLOUDRU_REGION", data["region"])
        data["source"] = os.getenv("CLOUDRU_SOURCE", data["source"])

    return data


def save_profile(
    profile: str,
    client_id: str,
    client_secret: str,
    x_api_key: str | None,
    x_workspace_id: str | None,
    region: str | None,
    source: str | None,
) -> None:
    ensure_storage()
    config = _read_ini(CONFIG_PATH)
    credentials = _read_ini(CREDENTIALS_PATH)

    if not credentials.has_section(profile):
        credentials.add_section(profile)
    credentials[profile]["client_id"] = client_id
    credentials[profile]["client_secret"] = client_secret
    credentials[profile]["x_api_key"] = x_api_key or ""
    credentials[profile]["x_workspace_id"] = x_workspace_id or ""

    if not config.has_section(profile):
        config.add_section(profile)
    if region:
        config[profile]["region"] = region
    if source:
        config[profile]["source"] = source

    with CREDENTIALS_PATH.open("w", encoding="utf-8") as f:
        credentials.write(f)
    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        config.write(f)

    try:
        CREDENTIALS_PATH.chmod(0o600)
    except OSError:
        pass


def load_cached_token(profile: str = "default") -> tuple[str | None, float | None]:
    ensure_storage()
    token_cache = _read_ini(TOKEN_CACHE_PATH)
    if not token_cache.has_section(profile):
        return None, None

    section = token_cache[profile]
    access_token = section.get("access_token") or None
    expires_raw = section.get("access_token_expires_at")
    try:
        expires_at = float(expires_raw) if expires_raw else None
    except ValueError:
        expires_at = None
    return access_token, expires_at


def save_cached_token(profile: str, access_token: str, access_token_expires_at: float) -> None:
    ensure_storage()
    token_cache = _read_ini(TOKEN_CACHE_PATH)
    if not token_cache.has_section(profile):
        token_cache.add_section(profile)

    token_cache[profile]["access_token"] = access_token
    token_cache[profile]["access_token_expires_at"] = str(float(access_token_expires_at))

    with TOKEN_CACHE_PATH.open("w", encoding="utf-8") as f:
        token_cache.write(f)

    try:
        TOKEN_CACHE_PATH.chmod(0o600)
    except OSError:
        pass


def list_profiles() -> list[str]:
    """Return all profile names found in config/credentials files."""
    ensure_storage()
    config = _read_ini(CONFIG_PATH)
    credentials = _read_ini(CREDENTIALS_PATH)
    profiles = set(config.sections()) | set(credentials.sections())
    return sorted(profiles)


def list_auth_profiles() -> list[str]:
    """Return profile names that have cloudru auth credentials."""
    ensure_storage()
    credentials = _read_ini(CREDENTIALS_PATH)
    profiles = []
    for profile in credentials.sections():
        section = credentials[profile]
        if section.get("client_id") and section.get("client_secret"):
            profiles.append(profile)
    return sorted(set(profiles))


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(',') if item.strip()]


def load_bot_config() -> dict:
    """Load Telegram bot config from dedicated INI file and env vars.

    File source:
    - ~/.cloudru/telegram.ini

    INI schema:
      [bot]
      token=...
      allowed_chat_ids=123,-100...
      poll_interval_sec=60

    Env var overrides:
    - CLOUDRU_TELEGRAM_BOT_TOKEN
    - CLOUDRU_TELEGRAM_ALLOWED_CHAT_IDS
    - CLOUDRU_TELEGRAM_POLL_INTERVAL_SEC
    """
    ensure_storage()
    telegram_cfg = _read_ini(TELEGRAM_CONFIG_PATH)
    token = None
    allowed_chat_ids_raw = None
    poll_interval_raw = None
    if telegram_cfg.has_section('bot'):
        bot = telegram_cfg['bot']
        token = bot.get('token')
        allowed_chat_ids_raw = bot.get('allowed_chat_ids')
        poll_interval_raw = bot.get('poll_interval_sec')

    token = os.getenv('CLOUDRU_TELEGRAM_BOT_TOKEN', token)
    allowed_chat_ids_raw = os.getenv('CLOUDRU_TELEGRAM_ALLOWED_CHAT_IDS', allowed_chat_ids_raw)
    poll_interval_raw = os.getenv('CLOUDRU_TELEGRAM_POLL_INTERVAL_SEC', poll_interval_raw)

    allowed_chat_ids = _parse_csv_list(allowed_chat_ids_raw)

    poll_interval_sec = 60
    try:
        if poll_interval_raw is not None and str(poll_interval_raw).strip() != '':
            poll_interval_sec = max(10, int(float(poll_interval_raw)))
    except (TypeError, ValueError):
        poll_interval_sec = 60

    return {
        'telegram_bot_token': token,
        'telegram_allowed_chat_ids': allowed_chat_ids,
        'telegram_poll_interval_sec': poll_interval_sec,
    }


def redact(value: str | None, keep: int = 4) -> str:
    if value is None:
        return ""
    if len(value) <= keep:
        return "*" * len(value)
    return "*" * (len(value) - keep) + value[-keep:]


def file_mode(path: Path) -> str:
    try:
        mode = stat.S_IMODE(path.stat().st_mode)
        return oct(mode)
    except OSError:
        return "unknown"
