# -*- coding: utf-8 -*-
"""Standalone bootstrap for an isolated Channel Runner process."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any


_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "channel_key",
        "code_root_sha256",
        "entrypoint",
    },
)
_ENTRYPOINT_FIELDS = frozenset({"module", "qualname"})
_CHANNEL_KEY_PATTERN = re.compile(
    r"^[a-z0-9](?:[a-z0-9_-]{0,62}[a-z0-9])?$",
)
_DOTTED_NAME_PATTERN = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$",
)
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_PROTOCOL_HANDLE: Any | None = None


class BootstrapError(RuntimeError):
    """Report a stable pre-import Runner bootstrap failure."""

    def __init__(self, reason_code: str, message: str) -> None:
        self.reason_code = reason_code
        super().__init__(message)


def _fail(reason_code: str, message: str) -> BootstrapError:
    """Build one stable bootstrap error."""
    return BootstrapError(reason_code, message)


def _require_isolated_python() -> None:
    """Reject launchers that omit Python isolated mode."""
    flags = sys.flags
    if not (
        flags.isolated
        and flags.ignore_environment
        and flags.no_user_site
        and getattr(flags, "safe_path", True)
    ):
        raise _fail(
            "PYTHON_NOT_ISOLATED",
            "Runner bootstrap requires Python isolated mode",
        )


def _absolute_existing_file(value: str, name: str) -> Path:
    """Resolve a required absolute regular file without using cwd."""
    path = Path(value)
    if not path.is_absolute():
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            f"{name} must be an absolute path",
        )
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            f"{name} does not exist",
        ) from exc
    if not resolved.is_file():
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            f"{name} must identify a regular file",
        )
    return resolved


def _absolute_existing_directory(value: str, name: str) -> Path:
    """Resolve a required absolute directory without using cwd."""
    path = Path(value)
    if not path.is_absolute():
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            f"{name} must be an absolute path",
        )
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            f"{name} does not exist",
        ) from exc
    if not resolved.is_dir():
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            f"{name} must identify a directory",
        )
    return resolved


def _parse_arguments(argv: list[str]) -> tuple[Path, Path]:
    """Parse the closed v1 command-line surface."""
    if len(argv) != 5 or argv[1] != "--code-root":
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            "Expected --code-root <path> --manifest <path>",
        )
    if argv[2] == "" or argv[3] != "--manifest" or argv[4] == "":
        raise _fail(
            "INVALID_BOOTSTRAP_ARGUMENT",
            "Expected --code-root <path> --manifest <path>",
        )
    return (
        _absolute_existing_directory(argv[2], "code_root"),
        _absolute_existing_file(argv[4], "manifest_path"),
    )


def _read_manifest(path: Path) -> dict[str, Any]:
    """Read a small closed JSON manifest before importing Channel code."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _fail("MANIFEST_INVALID", "Manifest is not valid JSON") from exc
    if not isinstance(data, dict) or set(data) != _MANIFEST_FIELDS:
        raise _fail("MANIFEST_INVALID", "Manifest fields do not match v1")
    if data["schema_version"] != 1:
        raise _fail("MANIFEST_INVALID", "Manifest schema_version must be 1")
    channel_key = data["channel_key"]
    if (
        not isinstance(channel_key, str)
        or _CHANNEL_KEY_PATTERN.fullmatch(channel_key) is None
    ):
        raise _fail("MANIFEST_INVALID", "Manifest channel_key is invalid")
    digest = data["code_root_sha256"]
    if (
        not isinstance(digest, str)
        or _DIGEST_PATTERN.fullmatch(digest) is None
    ):
        raise _fail(
            "MANIFEST_INVALID",
            "Manifest code_root_sha256 is invalid",
        )
    entrypoint = data["entrypoint"]
    if not isinstance(entrypoint, dict) or set(entrypoint) != (
        _ENTRYPOINT_FIELDS
    ):
        raise _fail("MANIFEST_INVALID", "Manifest entrypoint is invalid")
    for field in _ENTRYPOINT_FIELDS:
        value = entrypoint[field]
        if (
            not isinstance(value, str)
            or not value.isascii()
            or _DOTTED_NAME_PATTERN.fullmatch(value) is None
        ):
            raise _fail(
                "MANIFEST_INVALID",
                f"Manifest entrypoint {field} is invalid",
            )
    return data


def _hash_code_root(code_root: Path) -> str:
    """Hash the trusted code artifact deterministically."""
    digest = hashlib.sha256()
    files = sorted(
        path
        for path in code_root.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    )
    for path in files:
        relative = path.relative_to(code_root).as_posix().encode("utf-8")
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        content = path.read_bytes()
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return digest.hexdigest()


def _validate_manifest(code_root: Path, data: dict[str, Any]) -> None:
    """Ensure the manifest identifies the exact trusted code root."""
    try:
        actual = _hash_code_root(code_root)
    except OSError as exc:
        raise _fail("CODE_ROOT_INVALID", "Unable to hash code_root") from exc
    if actual != data["code_root_sha256"]:
        raise _fail("CODE_ROOT_MISMATCH", "code_root digest does not match")


def _entrypoint_source(code_root: Path, data: dict[str, Any]) -> Path:
    """Resolve the declared module to source inside the trusted root."""
    module = data["entrypoint"]["module"]
    module_path = code_root.joinpath(*module.split("."))
    candidates = (module_path.with_suffix(".py"), module_path / "__init__.py")
    for candidate in candidates:
        if candidate.is_file():
            resolved = candidate.resolve()
            if resolved.is_relative_to(code_root):
                return resolved
    raise _fail(
        "ENTRYPOINT_OUTSIDE_CODE_ROOT",
        "Declared entrypoint module is not inside code_root",
    )


def _clean_environment() -> None:
    """Remove ambient import paths and undeclared inherited variables."""
    allowed = frozenset(
        {
            "COMSPEC",
            "PATH",
            "PATHEXT",
            "SYSTEMDRIVE",
            "SYSTEMROOT",
            "TEMP",
            "TMP",
            "TMPDIR",
            "WINDIR",
        },
    )
    for name in tuple(os.environ):
        canonical_name = name.upper() if os.name == "nt" else name
        if canonical_name not in allowed:
            os.environ.pop(name, None)


def _capture_protocol_output() -> Any:
    """Duplicate initial stdout before ordinary output is redirected."""
    try:
        protocol_fd = os.dup(1)
        os.set_inheritable(protocol_fd, False)
        return os.fdopen(protocol_fd, "wb", buffering=0)
    except OSError as exc:
        raise _fail(
            "PROTOCOL_HANDLE_INVALID",
            "Unable to duplicate protocol stdout",
        ) from exc


def _redirect_ordinary_output() -> None:
    """Route Python stdout and native FD 1 writes to stderr."""
    try:
        os.dup2(2, 1, inheritable=False)
        # The process-wide stdout wrapper intentionally lives until exit.
        redirected_stdout = open(  # pylint: disable=consider-using-with
            1,
            mode="w",
            encoding="utf-8",
            errors="backslashreplace",
            buffering=1,
            closefd=False,
        )
        sys.stdout = redirected_stdout
    except (OSError, ValueError) as exc:
        raise _fail(
            "STDOUT_REDIRECT_FAILED",
            "Unable to redirect ordinary stdout",
        ) from exc


def _add_code_root(code_root: Path) -> None:
    """Replace ambient source paths with the one validated code root."""
    selected = str(code_root)
    sys.dont_write_bytecode = True
    sys.path[:] = [selected] + [
        item
        for item in sys.path
        if item
        and Path(item).resolve()
        != Path(__file__).parent.parent.parent.resolve()
    ]


def _load_entrypoint(data: dict[str, Any], expected_source: Path) -> Any:
    """Import and resolve only the manifest-declared entrypoint."""
    entrypoint = data["entrypoint"]
    try:
        module = importlib.import_module(entrypoint["module"])
        module_file = getattr(module, "__file__", None)
        if (
            module_file is None
            or Path(module_file).resolve() != expected_source
        ):
            raise _fail(
                "ENTRYPOINT_OUTSIDE_CODE_ROOT",
                "Imported entrypoint did not originate from code_root",
            )
        value: Any = module
        for part in entrypoint["qualname"].split("."):
            value = getattr(value, part)
        return value
    except (AttributeError, ImportError) as exc:
        raise _fail(
            "ENTRYPOINT_IMPORT_FAILED",
            "Unable to load declared Channel entrypoint",
        ) from exc


def write_protocol_frame(message: str) -> None:
    """Write one UTF-8 Content-Length frame to private protocol stdout."""
    if _PROTOCOL_HANDLE is None:
        raise _fail(
            "PROTOCOL_HANDLE_INVALID",
            "Protocol output is not initialized",
        )
    body = message.encode("utf-8", "strict")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    _PROTOCOL_HANDLE.write(header + body)


def _report_error(error: BootstrapError) -> None:
    """Report a pre-import failure on stderr without protocol output."""
    payload = {
        "error": error.reason_code,
        "message": str(error),
    }
    sys.stderr.write(f"{json.dumps(payload, separators=(',', ':'))}\n")
    sys.stderr.flush()


def main(argv: list[str] | None = None) -> int:
    """Validate, isolate, redirect output, then start one entrypoint."""
    global _PROTOCOL_HANDLE

    arguments = list(sys.argv if argv is None else argv)
    try:
        _require_isolated_python()
        code_root, manifest_path = _parse_arguments(arguments)
        manifest = _read_manifest(manifest_path)
        _validate_manifest(code_root, manifest)
        entrypoint_source = _entrypoint_source(code_root, manifest)
        _clean_environment()
        _PROTOCOL_HANDLE = _capture_protocol_output()
        _redirect_ordinary_output()
        _add_code_root(code_root)
        entrypoint = _load_entrypoint(manifest, entrypoint_source)
        result = entrypoint(write_protocol_frame)
        if result is not None and not isinstance(result, int):
            raise _fail(
                "ENTRYPOINT_INVALID",
                "Channel entrypoint must return an integer or None",
            )
        return 0 if result is None else result
    except BootstrapError as exc:
        _report_error(exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
