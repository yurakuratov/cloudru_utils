"""Validated AWS CLI contexts and single-archive snapshot transfers.

Only static credentials in an explicitly selected shared-credentials profile are
supported. AWS CLI ``s3 cp`` handles transfers, including multipart uploads.
Uploads use ordinary copy semantics, replacing an existing object at the same
key; callers should use unique snapshot names. No AWS output is forwarded: it can contain
credentials, including on failures.
"""

from __future__ import annotations

import configparser
from dataclasses import dataclass
import ipaddress
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit


_FLAGS = {
    "s3_snapshot_prefix": "--s3-prefix",
    "s3_endpoint_url": "--s3-endpoint-url",
    "aws_profile": "--aws-profile",
    "aws_cli": "--aws-cli",
    "aws_config_file": "--aws-config-file",
    "aws_credentials_file": "--aws-credentials-file",
}
_DEFAULTS = {
    "aws_cli": "aws",
    "aws_config_file": "~/.aws/config",
    "aws_credentials_file": "~/.aws/credentials",
}
_STATIC_KEYS = {"aws_access_key_id", "aws_secret_access_key", "aws_session_token"}


@dataclass(frozen=True)
class UploadConfig:
    s3_snapshot_prefix: str
    s3_endpoint_url: str
    aws_profile: str
    aws_cli: str
    aws_config_file: Path
    aws_credentials_file: Path

    @property
    def config_file(self) -> Path:
        return self.aws_config_file

    @property
    def credentials_file(self) -> Path:
        return self.aws_credentials_file

    @property
    def protected_paths(self) -> tuple[Path, Path]:
        """Paths source capture must exclude, including resolved symlink targets."""
        return (self.config_file, self.credentials_file)

    def public_dict(self) -> dict:
        """Resolved settings for plans/dry runs; never contains credential values."""
        return {
            "s3_snapshot_prefix": self.s3_snapshot_prefix,
            "s3_endpoint_url": self.s3_endpoint_url,
            "aws_profile": self.aws_profile,
            "aws_cli": self.aws_cli,
            "aws_config_file": str(self.config_file),
            "aws_credentials_file": str(self.credentials_file),
        }


def _invalid(key: str, reason: str) -> ValueError:
    # Never interpolate input values or parser/OS exceptions into diagnostics.
    return ValueError(f"Invalid {key} ({_FLAGS[key]}): {reason}")


def _text(value: object, key: str) -> str:
    if not isinstance(value, str) or not value.strip() or any(
        ord(c) < 32 or ord(c) == 127 for c in value
    ):
        raise _invalid(key, "expected a nonempty string without control characters")
    return value


def _prefix(value: str) -> str:
    key = "s3_snapshot_prefix"
    try:
        url = urlsplit(value)
        bucket = url.netloc
        if (
            url.scheme != "s3"
            or not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket)
            or ".." in bucket
            or any(c in value for c in "?#\\")
            or any(c.isspace() for c in value)
            or any(part in (".", "..") for part in url.path.split("/"))
        ):
            raise ValueError
        try:
            ipaddress.ip_address(bucket)
        except ValueError:
            pass
        else:
            raise ValueError
    except ValueError:
        raise _invalid(key, "expected s3://bucket/optional-prefix without query or traversal") from None
    return value.rstrip("/")


def _endpoint(value: str) -> str:
    try:
        url = urlsplit(value)
        if (
            url.scheme not in ("https", "http")
            or not url.hostname
            or url.username is not None
            or url.password is not None
            or any(c in value for c in "?#\\")
            or any(c.isspace() for c in value)
            or any(part in (".", "..") for part in url.path.split("/"))
            or url.port == 0
        ):
            raise ValueError
        host = url.hostname
        try:
            ipaddress.ip_address(host)
        except ValueError:
            if len(host) > 253 or not all(
                re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?", label)
                for label in host.rstrip(".").split(".")
            ):
                raise ValueError
    except ValueError:
        raise _invalid("s3_endpoint_url", "expected an HTTP(S) endpoint without credentials or query") from None
    return value.rstrip("/")


def _read_ini(value: str, key: str) -> tuple[Path, configparser.ConfigParser]:
    parser = configparser.ConfigParser(interpolation=None)
    try:
        path = Path(value).expanduser().resolve()
        mode = path.stat().st_mode
        if not stat.S_ISREG(mode) or not mode & 0o444:
            raise OSError
        with path.open(encoding="utf-8") as stream:
            parser.read_file(stream)
        # AWS does not implement ConfigParser's inherited DEFAULT values.
        if parser.defaults():
            raise ValueError
    except (OSError, RuntimeError, UnicodeError, configparser.Error, ValueError):
        raise _invalid(key, "expected a readable INI file without DEFAULT inheritance") from None
    return path, parser


def _validate_profiles(config: configparser.ConfigParser,
                       credentials: configparser.ConfigParser, profile: str) -> None:
    section = "default" if profile == "default" else f"profile {profile}"
    if config.has_section("plugins"):
        raise _invalid("aws_config_file", "AWS CLI plugins are unsupported")
    if config.has_section(section):
        for key in config[section]:
            if key.startswith(("credential_", "sso_", "role_", "web_identity_", "login_")) or key in {
                "source_profile", "mfa_serial", "external_id", "include_profile",
            }:
                raise _invalid("aws_profile", "only static file credentials are supported")
    if not credentials.has_section(profile):
        raise _invalid("aws_profile", "selected profile is absent from aws_credentials_file")
    selected = credentials[profile]
    if set(selected) - _STATIC_KEYS:
        raise _invalid("aws_credentials_file", "selected profile supports only static keys and optional session token")
    if not all(selected.get(key, "").strip() for key in ("aws_access_key_id", "aws_secret_access_key")):
        raise _invalid("aws_credentials_file", "selected profile requires aws_access_key_id and aws_secret_access_key")


def resolve_upload_config(values: dict) -> UploadConfig:
    """Validate already-merged config-name inputs without running AWS or networking.

    Precedence belongs to the caller. None uses the documented defaults for
    optional settings. An omitted AWS config profile is fine if the explicitly
    named shared-credentials profile exists; no default credentials are used.
    """
    if not isinstance(values, dict):
        raise ValueError("Upload settings must be a dict of configuration names")
    missing = [key for key in ("s3_snapshot_prefix", "s3_endpoint_url", "aws_profile")
               if values.get(key) is None or (isinstance(values.get(key), str) and not values[key].strip())]
    if missing:
        raise ValueError("Missing upload settings: " + "; ".join(
            f"{key} ({_FLAGS[key]})" for key in missing
        ) + "\n\nPass the settings as CLI flags, for example:\n"
            "  --s3-prefix s3://my-bucket/my-user/snapshots "
            "--s3-endpoint-url https://s3.cloud.ru --aws-profile my-aws-profile\n"
            "\nOr add them to ~/.cloudru/config:\n"
            "  [default]\n"
            "  s3_snapshot_prefix = s3://my-bucket/my-user/snapshots\n"
            "  s3_endpoint_url = https://s3.cloud.ru\n"
            "  aws_profile = my-aws-profile\n"
            "\nReplace these example values with your bucket/prefix, endpoint, and AWS profile.\n"
            "If using --profile NAME or CLOUDRU_PROFILE, use [NAME] instead of [default].\n"
            "aws_profile selects an existing profile in your AWS credentials/config files.\n"
            "CLI flags override the selected Cloud.ru config profile.")
    if set(values) - set(_FLAGS):
        raise ValueError("Unknown upload settings; use only: " + ", ".join(_FLAGS))
    resolved = {key: _text(values.get(key) if values.get(key) is not None else _DEFAULTS.get(key), key)
                for key in _FLAGS}
    prefix = _prefix(resolved["s3_snapshot_prefix"])
    endpoint = _endpoint(resolved["s3_endpoint_url"])
    profile = resolved["aws_profile"]
    if not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.@+-]*", profile):
        raise _invalid("aws_profile", "expected a static named profile")
    try:
        executable = shutil.which(os.path.expanduser(resolved["aws_cli"]))
        if not executable or not Path(executable).is_file():
            raise OSError
        executable = str(Path(executable).absolute())
    except (OSError, ValueError):
        raise _invalid("aws_cli", "executable is missing or not executable") from None
    config_path, config = _read_ini(resolved["aws_config_file"], "aws_config_file")
    credentials_path, credentials = _read_ini(resolved["aws_credentials_file"], "aws_credentials_file")
    _validate_profiles(config, credentials, profile)
    return UploadConfig(prefix, endpoint, profile, executable, config_path, credentials_path)


def validate_snapshot_uri(uri: str) -> str:
    """Validate one S3 object reference for submission and remote download."""
    _prefix(_text(uri, "s3_snapshot_prefix"))
    key = urlsplit(uri).path[1:]
    if not key or uri.endswith("/"):
        raise ValueError("snapshot.uri must have a nonempty object name")
    if len(key.encode("utf-8")) > 1024:
        raise ValueError("snapshot.uri exceeds the S3 key length limit")
    # Percent escapes and repeated slashes belong to the literal S3 key.
    return uri


def resolve_download_config(uri: str, endpoint_url: str, env: dict) -> UploadConfig:
    """Resolve remote static credentials exclusively from explicit job settings.

    No local upload defaults or ambient AWS selectors fill missing values.
    The runtime installs its job environment before calling this helper, so
    executable lookup and explicit tilde paths use the remote HOME/PATH.
    """
    uri = validate_snapshot_uri(uri)
    if not isinstance(env, dict):
        raise ValueError("Download environment must be a dict")
    selectors = {
        "CLOUDRU_AWS_CLI": "aws_cli",
        "AWS_PROFILE": "aws_profile",
        "AWS_CONFIG_FILE": "aws_config_file",
        "AWS_SHARED_CREDENTIALS_FILE": "aws_credentials_file",
    }
    missing = [name for name in selectors
               if not isinstance(env.get(name), str) or not env[name].strip()]
    if missing:
        raise ValueError("Missing explicit download settings: " + ", ".join(missing))
    return resolve_upload_config({
        "s3_snapshot_prefix": uri.rsplit("/", 1)[0],
        "s3_endpoint_url": _text(endpoint_url, "s3_endpoint_url"),
        **{key: env[name] for name, key in selectors.items()},
    })


def _aws_environment(config: UploadConfig) -> dict[str, str]:
    # Preserve local HOME/PATH/proxies, but inherit *no* AWS settings. In
    # particular, a new AWS credential selector cannot evade a stale denylist.
    env = {key: value for key, value in os.environ.items()
           if not key.upper().startswith("AWS_") and key.upper() not in {"BOTO_CONFIG", "BOTO_PATH"}}
    env.update({
        "AWS_PROFILE": config.aws_profile,
        "AWS_CONFIG_FILE": str(config.config_file),
        "AWS_SHARED_CREDENTIALS_FILE": str(config.credentials_file),
        "AWS_EC2_METADATA_DISABLED": "true",
        "AWS_PAGER": "",
        "AWS_CLI_AUTO_PROMPT": "off",
        "AWS_RETRY_MODE": "standard",
        "AWS_MAX_ATTEMPTS": "3",
        "AWS_IGNORE_CONFIGURED_ENDPOINT_URLS": "true",
        "BOTO_CONFIG": os.devnull,
    })
    return env


def _run(config: UploadConfig, env: dict, operation: str, args: list[str], *,
         retained: str = "local snapshot retained", managed: bool = False) -> subprocess.CompletedProcess:
    command = [config.aws_cli, "--profile", config.aws_profile,
               "--endpoint-url", config.s3_endpoint_url, "--output", "json",
               "--color", "off", "s3", operation, *args]
    if managed:
        return _run_managed(command, env, operation, retained)
    try:
        return subprocess.run(command, env=env, stdin=subprocess.DEVNULL,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, encoding="utf-8", errors="replace", check=False)
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError(f"AWS CLI {operation} could not run; {retained}") from None


def _run_managed(command: list[str], env: dict, operation: str,
                 retained: str) -> subprocess.CompletedProcess:
    try:
        # A private temporary file avoids pipe backpressure while waiting for
        # AWS. Runtime cancellation uses a custom exception outside OSError so
        # subprocess internals cannot mistake it for a retryable syscall error.
        with tempfile.TemporaryFile(mode="w+b") as diagnostics:
            process = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL, stderr=diagnostics,
                                       start_new_session=True)
            try:
                process.wait()
            except BaseException:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                finally:
                    process.wait()
                raise
            diagnostics.seek(0)
            stderr = diagnostics.read(64 * 1024).decode("utf-8", errors="replace")
            return subprocess.CompletedProcess(command, process.returncode, "", stderr)
    except InterruptedError:
        raise
    except (OSError, subprocess.SubprocessError):
        raise RuntimeError(f"AWS CLI {operation} could not run; {retained}") from None


def _error_code(result: subprocess.CompletedProcess, operation: str) -> str:
    match = re.search(r"An error occurred \(([^()\r\n]+)\) when calling the "
                      + re.escape(operation) + r" operation", result.stderr or "")
    return match.group(1) if match else ""


def _failure(operation: str, result: subprocess.CompletedProcess, *,
             api_operation: str = "PutObject", retained: str = "local snapshot retained") -> RuntimeError:
    code = _error_code(result, api_operation)
    # Only fixed known labels are safe: AWS diagnostics can echo secrets or
    # arbitrary endpoint responses, so even unknown error codes are suppressed.
    labels = {"403": "access forbidden", "AccessDenied": "access forbidden",
              "404": "not found", "NoSuchBucket": "bucket not found", "NoSuchKey": "object not found",
              "ExpiredToken": "credentials expired", "InvalidAccessKeyId": "invalid credentials",
              "SignatureDoesNotMatch": "credential signature rejected"}
    label = labels.get(code, "command failed")
    return RuntimeError(f"AWS CLI {operation}: {label}; {retained}")


def _archive_file(value: object, config: UploadConfig) -> tuple[Path, int]:
    try:
        path = Path(value).expanduser().absolute()
        if any(path.samefile(protected) for protected in config.protected_paths):
            raise ValueError
        mode = path.stat().st_mode
        if not stat.S_ISREG(mode) or not mode & 0o444:
            raise ValueError
        with path.open("rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or not info.st_mode & 0o444:
                raise ValueError
        if any(ord(c) < 32 or ord(c) == 127 for c in path.name):
            raise ValueError
    except (OSError, RuntimeError, TypeError, ValueError):
        raise ValueError("Invalid snapshot archive_path: require a readable file, excluding AWS files") from None
    return path, info.st_size


def upload_snapshot(snapshot: dict, config: UploadConfig) -> dict:
    """Copy one archive to S3; retain the local file on success or failure.

    Each invocation uploads again, replacing any object at the same key. Package
    validation belongs to the caller; no receipts or remote checksum checks are
    required. AWS CLI handles multipart transfers without a local upload size cap.
    """
    if not isinstance(config, UploadConfig):
        raise ValueError("config must be a resolved UploadConfig")
    # Revalidate files/executable before each upload, including on reused configs.
    config = resolve_upload_config(config.public_dict())
    if not isinstance(snapshot, dict) or "archive_path" not in snapshot:
        raise ValueError("Snapshot requires archive_path")
    archive, size = _archive_file(snapshot["archive_path"], config)
    uri = config.s3_snapshot_prefix + "/" + archive.name
    # Do not URL-decode S3 keys: '%' sequences are literal object-name bytes.
    if len(urlsplit(uri).path[1:].encode("utf-8")) > 1024:
        raise ValueError("Snapshot destination exceeds the S3 key length limit")
    print(f"Uploading snapshot archive ({size} bytes)...", file=sys.stderr)
    result = _run(config, _aws_environment(config), "cp", [str(archive), uri, "--only-show-errors"])
    if result.returncode:
        raise _failure("cp", result)
    print("Snapshot archive uploaded.", file=sys.stderr)
    return {**snapshot, "s3_uri": uri}


def output_destination(source: str, destination: str) -> str:
    """Keep the selected directory's name, even when it points elsewhere."""
    prefix = _prefix(destination)
    name = PurePosixPath(source).name
    if not name or name in (".", ".."):
        raise ValueError("Output source must name a directory")
    key = (urlsplit(prefix).path + "/" + name + "/")[1:]
    if len(key.encode("utf-8")) > 1024:
        raise ValueError("Output destination exceeds the S3 key length limit")
    return prefix + "/" + name + "/"


def upload_directory(source: str, destination: str, config: UploadConfig) -> bool:
    """Copy a directory, following symlinks; return False when it is missing."""
    path = Path(source)
    uri = output_destination(source, destination)
    try:
        mode = path.stat().st_mode
    except FileNotFoundError:
        return False
    if not stat.S_ISDIR(mode):
        raise ValueError("Output source is not a directory")
    config = resolve_upload_config(config.public_dict())
    result = _run(config, _aws_environment(config), "cp",
                  [str(path), uri, "--recursive", "--follow-symlinks", "--only-show-errors"],
                  retained="NFS files and partial uploads retained", managed=True)
    if result.returncode:
        raise _failure("cp", result, retained="NFS files and partial uploads retained")
    return True


def download_snapshot(uri: str, destination: str, config: UploadConfig) -> None:
    """Download one object using s3 cp, retaining any partial file on failure.

    The caller creates the destination directory and validates/extracts the
    downloaded package. No listing, metadata, or checksum API calls are issued
    by this helper; AWS CLI manages the transfer itself.
    """
    uri = validate_snapshot_uri(uri)
    if not isinstance(config, UploadConfig):
        raise ValueError("config must be a resolved UploadConfig")
    config = resolve_upload_config(config.public_dict())
    try:
        if not isinstance(destination, str) or not destination.strip():
            raise ValueError
        path = Path(destination).expanduser().absolute()
        if (os.path.lexists(path) or not path.parent.is_dir()
                or any(ord(c) < 32 or ord(c) == 127 for c in str(path))
                or path.resolve() in config.protected_paths):
            raise ValueError
    except (OSError, RuntimeError, TypeError, ValueError):
        raise ValueError("Invalid download destination: require a new file in an existing directory, excluding AWS files") from None
    result = _run(config, _aws_environment(config), "cp",
                  [uri, str(path), "--only-show-errors"], retained="partial download retained", managed=True)
    if result.returncode:
        raise _failure("cp", result, api_operation="GetObject", retained="partial download retained")
