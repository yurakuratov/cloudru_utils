"""Portable, immutable source snapshots; no Cloud.ru or storage dependencies."""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fnmatch
import getpass
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import socket
import stat
import subprocess
import tarfile
import tempfile
import uuid


CHUNK_SIZE = 1024 * 1024
DEFAULT_MAX_BYTES = 1024 * 1024 * 1024
MAX_MANIFEST_BYTES = 64 * 1024 * 1024
SCHEMA_VERSION = 1


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=True, indent=2) + "\n").encode()


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def _version(value: os.stat_result) -> tuple:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_size,
            value.st_mtime_ns, value.st_ctime_ns)


def _inside(path: Path, parent: Path) -> bool:
    return path == parent or parent in path.parents


def _same_file(path: Path, other: Path) -> bool:
    try:
        return path.samefile(other)
    except OSError:
        return False


@dataclass
class Entry:
    relative: str
    path: Path
    kind: str
    mode: int
    size: int
    version: tuple
    target: str | None = None


@dataclass
class SnapshotPlan:
    source: Path
    source_root: Path
    output_dir: Path
    use_gitignore: bool
    excludes: list[str]
    max_bytes: int
    protected_paths: list[Path]
    git_root: Path | None
    entries: list[Entry]
    git_paths: list[str]

    def public_dict(self) -> dict:
        return {
            "source": str(self.source), "output_dir": str(self.output_dir),
            "source_type": self.source_type, "use_gitignore": self.use_gitignore,
            "exclude": self.excludes, "max_bytes": self.max_bytes,
            "source_bytes": sum(e.size for e in self.entries if e.kind == "file"),
            "entry_count": len(self.entries), "source_digest": None,
        }

    @property
    def source_type(self) -> str:
        return "git" if self.git_root else ("directory" if self.source.is_dir() else "file")


def _git(root: Path, args: list[str], *, output=None, allow_failure=False):
    result = subprocess.run(["git", "-C", str(root), *args], stdout=output or subprocess.PIPE,
                            stderr=subprocess.PIPE, check=False)
    if result.returncode and not allow_failure:
        # Git errors can contain configuration contents. Do not echo stderr.
        raise RuntimeError(f"Git command failed while inspecting source ({args[0]})")
    return result


def _find_git_root(source: Path) -> Path | None:
    base = source if source.is_dir() else source.parent
    marker = any((p / ".git").exists() or (p / ".git").is_symlink() for p in [base, *base.parents])
    if shutil.which("git") is None:
        if marker:
            raise RuntimeError("Git is required to snapshot this repository; it was not found in PATH")
        return None
    result = _git(base, ["rev-parse", "--show-toplevel"], allow_failure=True)
    if result.returncode:
        if marker or b"not a git repository" not in result.stderr:
            raise RuntimeError("Git could not inspect the source repository")
        return None
    return Path(os.fsdecode(result.stdout).strip()).resolve()


def _excluded(plan: SnapshotPlan, path: Path, relative: str) -> bool:
    parts = PurePosixPath(relative).parts
    if ".git" in parts:
        return True
    # A matching parent excludes its complete subtree, also for Git-selected files.
    for index in range(1, len(parts) + 1):
        candidate = "/".join(parts[:index])
        for raw in plan.excludes:
            pattern = raw.rstrip("/")
            if pattern.startswith("./"):
                pattern = pattern[2:]
            subject = candidate if "/" in pattern else parts[index - 1]
            if fnmatch.fnmatchcase(subject, pattern):
                return True
    absolute = path.absolute()
    if _inside(absolute, plan.output_dir):
        return True
    resolved = path.resolve()
    if _inside(resolved, plan.output_dir):
        return True
    for protected in plan.protected_paths:
        if absolute == protected or resolved == protected or _same_file(path, protected):
            return True
    return False


def _pathspec(plan: SnapshotPlan) -> list[str]:
    relative = plan.source.relative_to(plan.git_root).as_posix()
    return [] if relative == "." else [f":(literal){relative}"]


def _selected(plan: SnapshotPlan) -> tuple[list[Path], list[str]]:
    git_paths: list[str] = []
    if plan.git_root:
        args = ["ls-files", "--cached", "--others", "--full-name", "-z"]
        if plan.use_gitignore:
            args.append("--exclude-standard")
        listing = _git(plan.git_root, args + ["--", *_pathspec(plan)]).stdout
        selected = []
        for name in listing.split(b"\0"):
            if not name:
                continue
            git_path = os.fsdecode(name)
            path = plan.git_root / git_path
            relative = path.relative_to(plan.source_root).as_posix()
            if not _excluded(plan, path, relative):
                git_paths.append(git_path)
                if path.exists() or path.is_symlink():
                    selected.append(path)
        # A staged deletion has disappeared from the index, but is still part
        # of the captured working-tree provenance relative to HEAD.
        head = _git(plan.git_root, ["rev-parse", "--verify", "HEAD"], allow_failure=True)
        if head.returncode == 0:
            deleted = _git(plan.git_root, ["diff", "--name-only", "-z", "--diff-filter=D",
                                           "HEAD", "--", *_pathspec(plan)]).stdout
            for name in deleted.split(b"\0"):
                if name:
                    git_path = os.fsdecode(name)
                    path = plan.git_root / git_path
                    if not _excluded(plan, path, path.relative_to(plan.source_root).as_posix()):
                        git_paths.append(git_path)
        staged = _git(plan.git_root, ["ls-files", "--stage", "--full-name", "-z", "--",
                                     *_pathspec(plan)]).stdout
        for row in staged.split(b"\0"):
            if row.startswith(b"160000 "):
                path = plan.git_root / os.fsdecode(row.split(b"\t", 1)[1])
                relative = path.relative_to(plan.source_root).as_posix()
                if not _excluded(plan, path, relative):
                    raise RuntimeError(f"Selected submodule is unsupported: {relative}; exclude it or prepare a plain source tree")
        if plan.use_gitignore:
            return selected, sorted(set(git_paths))
    if not plan.source.is_dir():
        return [plan.source], sorted(set(git_paths))
    selected = []
    def unreadable(error):
        raise RuntimeError("Cannot read a directory in the snapshot source") from None

    for root, dirs, files in os.walk(plan.source, followlinks=False, onerror=unreadable):
        base = Path(root)
        for name in list(dirs):
            path = base / name
            if _excluded(plan, path, path.relative_to(plan.source_root).as_posix()):
                dirs.remove(name)
            else:
                selected.append(path)
                if path.is_symlink():
                    dirs.remove(name)
        for name in files:
            path = base / name
            if not _excluded(plan, path, path.relative_to(plan.source_root).as_posix()):
                selected.append(path)
    return selected, sorted(set(git_paths))


def _scan(plan: SnapshotPlan) -> tuple[list[Entry], list[str]]:
    selected, git_paths = _selected(plan)
    paths = set(selected)
    for path in selected:
        for parent in path.parents:
            if parent == plan.source_root:
                break
            if not _inside(parent, plan.source_root):
                raise RuntimeError("Selected source path escapes snapshot root")
            paths.add(parent)
    entries = []
    total = 0
    for path in sorted(paths, key=lambda p: os.fsencode(str(p))):
        relative = path.relative_to(plan.source_root).as_posix()
        if "\\" in relative:
            raise RuntimeError(f"Backslashes in source paths are not supported: {relative}")
        if _excluded(plan, path, relative):
            continue
        info = path.lstat()
        target = None
        size = 0
        if stat.S_ISLNK(info.st_mode):
            kind = "symlink"
            target = os.readlink(path)
            if os.path.isabs(target) or not _inside(path.resolve(), plan.source_root):
                raise RuntimeError(f"Symlink escapes snapshot root: {relative}")
        elif stat.S_ISREG(info.st_mode):
            kind, size = "file", info.st_size
            total += size
            if total > plan.max_bytes:
                raise RuntimeError(f"Snapshot source exceeds --max-bytes limit ({plan.max_bytes} bytes)")
        elif stat.S_ISDIR(info.st_mode):
            kind = "directory"
        else:
            raise RuntimeError(f"Unsupported special file in snapshot: {relative}")
        entries.append(Entry(relative, path, kind, stat.S_IMODE(info.st_mode), size,
                             _version(info), target))
    return entries, git_paths


def prepare_snapshot(source: str, output_dir: str = "./snapshots", *, use_gitignore: bool = True,
                     exclude: list[str] | None = None, exclude_from: str | None = None,
                     max_bytes: int = DEFAULT_MAX_BYTES,
                     protected_paths: list[str | Path] | None = None) -> SnapshotPlan:
    """Validate and select source without creating files; also used by dry-run."""
    path = Path(source).expanduser().absolute()
    if not path.exists() and not path.is_symlink():
        raise RuntimeError(f"Snapshot source does not exist: {path}")
    if path.is_dir() or not path.is_symlink():
        path = path.resolve()
    output = Path(output_dir).expanduser().resolve()
    if output == path:
        raise RuntimeError("Snapshot output directory must not be the source directory")
    if output.exists() and not output.is_dir():
        raise RuntimeError(f"Snapshot output is not a directory: {output}")
    if type(max_bytes) is not int or max_bytes <= 0:
        raise RuntimeError("--max-bytes must be a positive integer")
    patterns = list(exclude or [])
    if exclude_from:
        try:
            lines = Path(exclude_from).expanduser().read_text().splitlines()
        except OSError:
            raise RuntimeError(f"Cannot read exclusion file: {exclude_from}") from None
        patterns.extend(line.strip() for line in lines if line.strip() and not line.lstrip().startswith("#"))
    if any(not pattern or pattern in ("/", "./") for pattern in patterns):
        raise RuntimeError("Exclusion patterns must be nonempty source-relative patterns")
    protected = [Path(p).expanduser().resolve() for p in (protected_paths or [])]
    if path.resolve() in protected or any(_same_file(path, p) for p in protected):
        raise RuntimeError("The requested source is a protected configuration/credential file")
    plan = SnapshotPlan(path, path if path.is_dir() else path.parent, output,
                        use_gitignore, patterns, max_bytes, protected, _find_git_root(path), [], [])
    plan.entries, plan.git_paths = _scan(plan)
    return plan


def _provenance(plan: SnapshotPlan, destination: Path) -> dict:
    destination.mkdir()
    if not plan.git_root:
        return {}
    head = _git(plan.git_root, ["rev-parse", "--verify", "HEAD"], allow_failure=True)
    commit = head.stdout.decode().strip() if head.returncode == 0 else None
    branch = _git(plan.git_root, ["symbolic-ref", "--quiet", "--short", "HEAD"], allow_failure=True)
    metadata = {"commit": commit, "branch": branch.stdout.decode().strip() or None,
                "root": str(plan.git_root)}
    (destination / "git.json").write_bytes(_json_bytes(metadata))
    # Bound each argv; write potentially large binary diffs directly to disk.
    groups: list[list[str]] = []
    group: list[str] = []
    size = 0
    for path in plan.git_paths:
        literal = f":(literal){path}"
        if group and size + len(os.fsencode(literal)) > 16000:
            groups.append(group)
            group, size = [], 0
        group.append(literal)
        size += len(os.fsencode(literal)) + 1
    if group:
        groups.append(group)
    for name, args in (("git-status.txt", ["status", "--porcelain=v1", "--untracked-files=all"]),
                       ("git-diff.patch", ["diff", "--binary", "--no-ext-diff", "--no-textconv", *(["HEAD"] if commit else ["--cached"])]),
                       ("git-worktree.patch", ["diff", "--binary", "--no-ext-diff", "--no-textconv"])):
        with (destination / name).open("wb") as stream:
            for group in groups:
                _git(plan.git_root, [*args, "--", *group], output=stream)
    with (destination / "git-untracked.txt").open("wb") as stream:
        for group in groups:
            args = ["ls-files", "--others"]
            if plan.use_gitignore:
                args.append("--exclude-standard")
            _git(plan.git_root, [*args, "--", *group], output=stream)
    return metadata


def _source_digest(files: list[dict]) -> str:
    return "sha256:" + hashlib.sha256(_json_bytes(files)).hexdigest()


def _publish_file(temporary: Path, destination: Path) -> None:
    # Same filesystem: hard-link publication is atomic and cannot overwrite.
    try:
        os.link(temporary, destination)
    except FileExistsError:
        raise RuntimeError(f"Refusing to overwrite finalized snapshot: {destination}") from None


def snapshot_filename(source_name: str, created: datetime, source_digest: str, suffix: str) -> str:
    """Format the archive name; also used to validate destinations before capture."""
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", source_name).strip("._") or "source"
    return f"{name[:80]}-{created:%Y%m%d-%H%M%S}-{source_digest[7:19]}-{suffix}.tar.gz"


def create_snapshot(plan: SnapshotPlan) -> dict:
    """Capture a validated plan and return finalized archive information."""
    current, git_paths = _scan(plan)
    if current != plan.entries or git_paths != plan.git_paths:
        raise RuntimeError("Source changed during capture; prepare a new snapshot plan")
    plan.output_dir.mkdir(parents=True, exist_ok=True)
    # Creating an excluded nested output directory can change source ancestor
    # directory metadata. Establish the capture baseline after that operation.
    refreshed, git_paths = _scan(plan)
    originals = {entry.relative: entry for entry in plan.entries}
    for entry in refreshed:
        original = originals.get(entry.relative)
        if entry.kind == "directory" and entry.path in plan.output_dir.parents:
            if original is not None and entry.version[:3] != original.version[:3]:
                raise RuntimeError("Source directory changed during capture")
        elif entry != original:
            raise RuntimeError("Source changed during capture")
    if not set(originals) <= {entry.relative for entry in refreshed} or git_paths != plan.git_paths:
        raise RuntimeError("Source selection changed during capture")
    plan.entries = refreshed
    with tempfile.TemporaryDirectory(prefix=".cloudru-snapshot-", dir=plan.output_dir) as temp:
        temporary = Path(temp)
        package = temporary / "package"
        source_copy = package / "source"
        source_copy.mkdir(parents=True)
        provenance = _provenance(plan, package / "provenance")
        files = []
        total = 0
        for entry in plan.entries:
            if _version(entry.path.lstat()) != entry.version:
                raise RuntimeError(f"Source changed during capture: {entry.relative}")
            destination = source_copy / entry.relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            item = {"path": entry.relative, "type": entry.kind, "mode": entry.mode}
            if entry.kind == "directory":
                destination.mkdir(exist_ok=True)
            elif entry.kind == "symlink":
                destination.symlink_to(entry.target)
                item["target"] = entry.target
            else:
                digest = hashlib.sha256()
                copied = 0
                fd = os.open(entry.path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
                with os.fdopen(fd, "rb") as incoming, destination.open("xb") as outgoing:
                    if _version(os.fstat(incoming.fileno())) != entry.version:
                        raise RuntimeError(f"Source changed during capture: {entry.relative}")
                    for block in iter(lambda: incoming.read(CHUNK_SIZE), b""):
                        total += len(block)
                        copied += len(block)
                        if total > plan.max_bytes:
                            raise RuntimeError(f"Snapshot source exceeds --max-bytes limit ({plan.max_bytes} bytes)")
                        outgoing.write(block)
                        digest.update(block)
                    if copied != entry.size or _version(os.fstat(incoming.fileno())) != entry.version:
                        raise RuntimeError(f"Source changed during capture: {entry.relative}")
                destination.chmod(entry.mode)
                item.update(size=copied, sha256=digest.hexdigest())
            if _version(entry.path.lstat()) != entry.version:
                raise RuntimeError(f"Source changed during capture: {entry.relative}")
            files.append(item)
        rescanned, git_paths = _scan(plan)
        if [(e.relative, e.version) for e in rescanned] != [(e.relative, e.version) for e in plan.entries] or git_paths != plan.git_paths:
            raise RuntimeError("Source selection changed during capture")
        _provenance(plan, temporary / "provenance-after")
        before = package / "provenance"
        after = temporary / "provenance-after"
        if {p.name: _hash_file(p) for p in before.iterdir()} != {p.name: _hash_file(p) for p in after.iterdir()}:
            raise RuntimeError("Git provenance changed during capture")
        created = datetime.now(timezone.utc)
        manifest = {
            "schema_version": SCHEMA_VERSION, "created_at": created.isoformat(),
            "created_by": f"{getpass.getuser()}@{socket.gethostname()}",
            "source": str(plan.source), "source_type": plan.source_type,
            "use_gitignore": plan.use_gitignore, "exclude": plan.excludes,
            "source_bytes": total, "source_digest": _source_digest(files),
            "files": files, "git": provenance,
        }
        serialized = _json_bytes(manifest)
        if len(serialized) > MAX_MANIFEST_BYTES:
            raise RuntimeError("Snapshot manifest exceeds the supported 64 MiB limit")
        (package / "manifest.json").write_bytes(serialized)
        filename = snapshot_filename(plan.source.name, created, manifest["source_digest"], uuid.uuid4().hex[:4])
        archive = temporary / filename
        entry_by_path = {entry.relative: entry for entry in plan.entries}
        with tarfile.open(archive, "w:gz", compresslevel=6) as tar:
            for path in sorted(package.rglob("*")):
                relative = path.relative_to(package).as_posix()
                info = tar.gettarinfo(str(path), arcname=relative)
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = 0
                if relative.startswith("source/"):
                    original = entry_by_path.get(relative[7:])
                    if original:
                        info.mode = original.mode
                if info.isfile():
                    with path.open("rb") as stream:
                        tar.addfile(info, stream)
                else:
                    tar.addfile(info)
        result = {
            "schema_version": SCHEMA_VERSION, "archive_path": str(plan.output_dir / filename),
            "source_digest": manifest["source_digest"],
            "size_bytes": archive.stat().st_size, "source_bytes": total,
        }
        _publish_file(archive, Path(result["archive_path"]))
        return result


def _member_name(name: str) -> str:
    if not name or name.startswith("/") or "\\" in name or "\0" in name:
        raise RuntimeError("Unsafe snapshot archive member")
    name = name.rstrip("/")
    if any(p in ("", ".", "..") for p in name.split("/")):
        raise RuntimeError("Unsafe snapshot archive member")
    if name != "manifest.json" and name.split("/")[0] not in ("source", "provenance"):
        raise RuntimeError("Archive is not a supported snapshot package")
    return name


def inspect_snapshot(archive_path: str) -> dict:
    """Validate the package against its internal manifest without extracting or writing."""
    path = Path(archive_path).expanduser().resolve()
    if not path.is_file():
        raise RuntimeError(f"Snapshot archive is not a readable file: {path}")
    original_version = _version(path.stat())
    files = []
    members: dict[str, str] = {}
    manifest = None
    try:
        with tarfile.open(path, "r|gz") as tar:
            for member in tar:
                name = _member_name(member.name)
                if name in members:
                    raise RuntimeError("Duplicate snapshot archive member")
                if not (member.isfile() or member.isdir() or member.issym()):
                    raise RuntimeError("Unsupported special file in snapshot archive")
                if member.issparse() or not 0 <= member.mode <= 0o7777:
                    raise RuntimeError("Unsupported sparse file or mode in snapshot archive")
                if name in ("source", "provenance") and not member.isdir():
                    raise RuntimeError("Snapshot package roots must be directories")
                if not name.startswith("source/") and member.issym():
                    raise RuntimeError("Symlinks are allowed only inside snapshot source")
                members[name] = "symlink" if member.issym() else ("directory" if member.isdir() else "file")
                if name == "manifest.json":
                    if not member.isfile() or member.size > MAX_MANIFEST_BYTES:
                        raise RuntimeError("Invalid snapshot manifest")
                    stream = tar.extractfile(member)
                    manifest = json.load(stream)
                elif name.startswith("source/"):
                    item = {"path": name[7:], "type": members[name], "mode": member.mode}
                    if member.isfile():
                        digest = hashlib.sha256()
                        stream = tar.extractfile(member)
                        size = 0
                        for block in iter(lambda: stream.read(CHUNK_SIZE), b""):
                            digest.update(block)
                            size += len(block)
                        if size != member.size:
                            raise RuntimeError("Truncated snapshot member")
                        item.update(size=size, sha256=digest.hexdigest())
                    elif member.issym():
                        item["target"] = member.linkname
                    files.append(item)
        for name in members:
            for parent in PurePosixPath(name).parents:
                if str(parent) == ".":
                    break
                if members.get(str(parent)) != "directory":
                    raise RuntimeError("Archive member has a missing or non-directory parent")
        _validate_links(files)
        files.sort(key=lambda item: os.fsencode(item["path"]))
        total = sum(item.get("size", 0) for item in files)
        if (not isinstance(manifest, dict) or manifest.get("schema_version") != SCHEMA_VERSION
                or manifest.get("files") != files or manifest.get("source_digest") != _source_digest(files)
                or manifest.get("source_bytes") != total
                or members.get("source") != "directory" or members.get("provenance") != "directory"):
            raise RuntimeError("Snapshot manifest does not match archive contents")
        result = {
            "schema_version": SCHEMA_VERSION, "archive_path": str(path),
            "source_digest": manifest["source_digest"], "size_bytes": path.stat().st_size,
            "source_bytes": total,
        }
        if _version(path.stat()) != original_version:
            raise RuntimeError("Snapshot archive changed during validation")
        return result
    except (tarfile.TarError, EOFError, OSError, ValueError) as exc:
        raise RuntimeError(f"Cannot validate snapshot archive ({type(exc).__name__})") from None


def _validate_links(files: list[dict]) -> None:
    links = {item["path"]: item["target"] for item in files if item["type"] == "symlink"}
    for name, target in links.items():
        if not target:
            raise RuntimeError("Empty symlink target in snapshot")
        if "\0" in target or "\\" in target:
            raise RuntimeError("Unsafe symlink target in snapshot")
        parts = list(PurePosixPath(name).parent.parts)
        pending = target.split("/")
        followed = {name}
        while pending:
            part = pending.pop(0)
            if part == "" and not parts:
                raise RuntimeError("Absolute symlink in snapshot")
            if part in ("", "."):
                continue
            if part == "..":
                if not parts:
                    raise RuntimeError("Symlink escapes snapshot root")
                parts.pop()
            else:
                parts.append(part)
                candidate = "/".join(parts)
                if candidate in links:
                    if candidate in followed:
                        raise RuntimeError("Symlink cycle in snapshot")
                    followed.add(candidate)
                    parts.pop()
                    next_target = links[candidate]
                    if next_target.startswith("/"):
                        raise RuntimeError("Absolute symlink in snapshot")
                    pending = next_target.split("/") + pending
        if target.startswith("/"):
            raise RuntimeError("Absolute symlink in snapshot")


@contextmanager
def _extraction_parent(root_fd: int, name: str):
    """Walk validated member parents without following filesystem symlinks."""
    fd = os.dup(root_fd)
    try:
        for part in name.split("/")[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd, name.rsplit("/", 1)[-1]
    finally:
        os.close(fd)


def extract_snapshot(archive_path: str, destination: str) -> dict:
    """Validate, then unpack into an existing fresh job directory.

    Only a real .cloudru directory may already exist (including the downloaded
    archive). Return inspect_snapshot's metadata; keep the archive and any
    partial extraction on failure. Directory modes are applied last so that
    read-only source directories can be populated. Symlink modes are preserved
    where the OS supports changing them without following the link.
    """
    path = Path(archive_path).expanduser().absolute()
    root = Path(destination).expanduser().absolute()
    try:
        original = _version(path.lstat())
        if not stat.S_ISREG(original[2]):
            raise RuntimeError("Snapshot archive must be a regular file")
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, "rb") as incoming:
            def unchanged():
                if (_version(path.lstat()) != original
                        or _version(os.fstat(incoming.fileno())) != original):
                    raise RuntimeError("Snapshot archive changed during extraction")

            unchanged()
            result = inspect_snapshot(str(path))
            unchanged()
            # Read all headers before touching the destination. Keep file order
            # to avoid repeatedly seeking backwards through a gzip stream.
            with tarfile.open(fileobj=incoming, mode="r:gz") as tar:
                members = [(_member_name(m.name), m) for m in tar.getmembers()]
                unchanged()
                root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                try:
                    root_identity = os.fstat(root_fd)
                    for name in os.listdir(root_fd):
                        if (name != ".cloudru" or not stat.S_ISDIR(
                                os.stat(name, dir_fd=root_fd, follow_symlinks=False).st_mode)):
                            raise RuntimeError("Snapshot destination must be fresh; existing extraction targets are forbidden")
                    directories = sorted(((n, m) for n, m in members if m.isdir()),
                                         key=lambda row: row[0].count("/"))
                    for name, member in directories:
                        unchanged()
                        with _extraction_parent(root_fd, name) as (parent, leaf):
                            os.mkdir(leaf, 0o700, dir_fd=parent)
                    for name, member in members:
                        if not member.isfile():
                            continue
                        unchanged()
                        with _extraction_parent(root_fd, name) as (parent, leaf):
                            output_fd = os.open(leaf, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                                                | os.O_NOFOLLOW, 0o600, dir_fd=parent)
                            with os.fdopen(output_fd, "wb") as outgoing, tar.extractfile(member) as stream:
                                remaining = member.size
                                while remaining:
                                    block = stream.read(min(CHUNK_SIZE, remaining))
                                    if not block or len(block) > remaining:
                                        raise RuntimeError("Truncated or changed snapshot member")
                                    outgoing.write(block)
                                    remaining -= len(block)
                                outgoing.flush()
                                unchanged()
                                os.fchmod(outgoing.fileno(), member.mode)
                    # All validation and regular-file writes precede symlinks.
                    unchanged()
                    for name, member in members:
                        if member.issym():
                            with _extraction_parent(root_fd, name) as (parent, leaf):
                                os.symlink(member.linkname, leaf, dir_fd=parent)
                                if os.chmod in os.supports_follow_symlinks:
                                    os.chmod(leaf, member.mode, dir_fd=parent, follow_symlinks=False)
                    for name, member in reversed(directories):
                        with _extraction_parent(root_fd, name) as (parent, leaf):
                            directory_fd = os.open(leaf, os.O_RDONLY | os.O_DIRECTORY
                                                   | os.O_NOFOLLOW, dir_fd=parent)
                            try:
                                os.fchmod(directory_fd, member.mode)
                            finally:
                                os.close(directory_fd)
                    unchanged()
                    current_root = root.lstat()
                    if (current_root.st_dev, current_root.st_ino) != (root_identity.st_dev, root_identity.st_ino):
                        raise RuntimeError("Snapshot destination changed during extraction")
                finally:
                    os.close(root_fd)
        return result
    except (tarfile.TarError, EOFError, OSError, ValueError) as exc:
        raise RuntimeError(f"Cannot extract snapshot archive ({type(exc).__name__}); partial extraction retained") from None
