# -*- coding: utf-8 -*-
"""Standalone bootstrap for an isolated Channel Runner process."""

from __future__ import annotations

import asyncio
import ctypes
from ctypes import wintypes
import hashlib
import importlib
import json
import os
from pathlib import Path
import re
import sys
import threading
import types
from typing import Any


_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "code_root_sha256",
        "descriptor_path",
        "descriptor_sha256",
        "allowed_platform_tags",
    },
)
_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_STARTUP_ENVIRONMENT = frozenset(
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
_AMBIENT_IMPORT_ENVIRONMENT = frozenset({"PYTHONHOME", "PYTHONPATH"})
_THREAD_TERMINATE = 0x0001
_ERROR_NOT_FOUND = 1168


class BootstrapError(RuntimeError):
    """Report a stable pre-import Runner bootstrap failure."""

    def __init__(self, reason_code: str, message: str) -> None:
        self.reason_code = reason_code
        super().__init__(message)


class _RunnerProcess:
    """Own the validated driver instance and protocol transport."""

    def __init__(self, descriptor: Any, transport: Any) -> None:
        self._descriptor = descriptor
        self._transport = transport
        self._driver: Any | None = None

    def start(self, expected_source: Path) -> None:
        """Load and construct only the descriptor-declared driver class."""
        driver_class = _load_driver_class(
            self._descriptor.entrypoint,
            expected_source,
        )
        if not isinstance(driver_class, type):
            raise _fail(
                "ENTRYPOINT_INVALID",
                "Runner entrypoint must be a ChannelDriver class",
            )
        self._driver = driver_class()


class _WindowsThreadHandle:
    """Own one cancellable Win32 handle for the writer thread."""

    def __init__(self, kernel32: Any, handle: int) -> None:
        self._kernel32 = kernel32
        self._handle = handle
        self._closed = False

    def cancel(self) -> None:
        """Cancel synchronous I/O currently issued by the writer thread."""
        if self._closed:
            return
        if self._kernel32.CancelSynchronousIo(self._handle):
            return
        error = ctypes.get_last_error()
        if error != _ERROR_NOT_FOUND:
            raise OSError(error, "Unable to cancel protocol pipe write")

    def close(self) -> None:
        """Close the real Win32 thread handle exactly once."""
        if self._closed:
            return
        self._closed = True
        if not self._kernel32.CloseHandle(self._handle):
            error = ctypes.get_last_error()
            raise OSError(error, "Unable to close protocol writer thread")


def _open_windows_thread_handle() -> _WindowsThreadHandle:
    """Open the current writer thread for synchronous I/O cancellation."""
    win_dll = getattr(ctypes, "WinDLL")
    kernel32 = win_dll("kernel32", use_last_error=True)
    kernel32.GetCurrentThreadId.argtypes = []
    kernel32.GetCurrentThreadId.restype = wintypes.DWORD
    kernel32.OpenThread.argtypes = [
        wintypes.DWORD,
        wintypes.BOOL,
        wintypes.DWORD,
    ]
    kernel32.OpenThread.restype = wintypes.HANDLE
    kernel32.CancelSynchronousIo.argtypes = [wintypes.HANDLE]
    kernel32.CancelSynchronousIo.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL
    thread_id = kernel32.GetCurrentThreadId()
    handle = kernel32.OpenThread(_THREAD_TERMINATE, False, thread_id)
    if not handle:
        error = ctypes.get_last_error()
        raise OSError(error, "Unable to open protocol writer thread")
    return _WindowsThreadHandle(kernel32, handle)


class _ThreadPipeWriter:
    """Adapt one synchronous pipe to the asyncio writer interface."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle
        self._condition = threading.Condition()
        self._request: tuple[
            asyncio.AbstractEventLoop,
            asyncio.Future[None],
            bytes,
        ] | None = None
        self._pending_future: asyncio.Future[None] | None = None
        self._thread_handle: Any | None = None
        self._startup_error: OSError | None = None
        self._ready = threading.Event()
        self._exited = threading.Event()
        self._active = False
        self._closed = False
        self._thread = threading.Thread(
            target=self._run,
            name="qwenpaw-protocol-writer",
            daemon=True,
        )
        self._thread.start()
        self._ready.wait()
        if self._startup_error is not None:
            self._exited.wait()
            raise self._startup_error

    @staticmethod
    def _consume_future_error(future: asyncio.Future[None]) -> None:
        """Mark an adapter failure observed even after drain cancellation."""
        if not future.cancelled():
            future.exception()

    @staticmethod
    def _complete(
        future: asyncio.Future[None],
        error: BaseException | None,
    ) -> None:
        """Complete one pipe write on its owning event loop."""
        if future.done():
            return
        if error is None:
            future.set_result(None)
        else:
            future.set_exception(error)

    def _write_all(self, data: bytes) -> None:
        """Write every byte through a possibly partial sync pipe."""
        view = memoryview(data)
        written = 0
        while written < len(view):
            with self._condition:
                if self._closed:
                    raise BrokenPipeError("protocol pipe is closed")
            count = self._handle.write(view[written:])
            if not isinstance(count, int) or count <= 0:
                raise OSError("protocol pipe write made no progress")
            if count > len(view) - written:
                raise OSError("protocol pipe write exceeded input length")
            written += count

    def _run(self) -> None:
        """Process one write at a time outside the asyncio event loop."""
        thread_handle: Any | None = None
        try:
            try:
                thread_handle = _open_windows_thread_handle()
            except OSError as exc:
                self._startup_error = exc
                self._ready.set()
                return
            with self._condition:
                self._thread_handle = thread_handle
                self._ready.set()
            while True:
                with self._condition:
                    while self._request is None and not self._closed:
                        self._condition.wait()
                    if self._request is None:
                        return
                    loop, future, data = self._request
                    self._request = None
                    self._active = True
                error: BaseException | None = None
                try:
                    self._write_all(data)
                except (BrokenPipeError, ConnectionError, OSError) as exc:
                    error = exc
                with self._condition:
                    self._active = False
                try:
                    loop.call_soon_threadsafe(self._complete, future, error)
                except RuntimeError:
                    return
        finally:
            if not self._ready.is_set():
                self._ready.set()
            try:
                self._handle.close()
            finally:
                try:
                    if thread_handle is not None:
                        thread_handle.close()
                finally:
                    self._exited.set()

    def write(self, data: bytes) -> None:
        """Hand one complete frame to the dedicated writer thread."""
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        future.add_done_callback(self._consume_future_error)
        with self._condition:
            if self._closed:
                raise BrokenPipeError("protocol pipe is closed")
            if self._pending_future is not None:
                raise RuntimeError("protocol pipe write is already pending")
            self._pending_future = future
            self._request = (loop, future, bytes(data))
            self._condition.notify()

    async def wait_write_accepted(self) -> None:
        """Wait until the HANDLE accepts the complete pending frame."""
        future = self._pending_future
        if future is None:
            return
        cancelled = False
        try:
            while not future.done():
                try:
                    await asyncio.shield(future)
                except asyncio.CancelledError:
                    cancelled = True
                    self.close()
                if cancelled and not future.done():
                    await self._retry_cancelled_write(future)
            if cancelled:
                try:
                    future.result()
                except Exception:
                    raise asyncio.CancelledError from None
                return
            future.result()
        finally:
            if future.done() and self._pending_future is future:
                self._pending_future = None

    async def _retry_cancelled_write(
        self,
        future: asyncio.Future[None],
    ) -> None:
        """Retry synchronous I/O cancellation until it reaches a result."""
        while not future.done():
            with self._condition:
                thread_handle = self._thread_handle
                active = self._active
            if active and thread_handle is not None:
                thread_handle.cancel()
            try:
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                continue

    async def drain(self) -> None:
        """Wait until the dedicated thread completes the pending write."""
        await self.wait_write_accepted()

    def close(self) -> None:
        """Close the pipe and wake the dedicated writer thread."""
        queued: tuple[
            asyncio.AbstractEventLoop,
            asyncio.Future[None],
            bytes,
        ] | None = None
        with self._condition:
            if self._closed:
                return
            self._closed = True
            queued = self._request
            self._request = None
            if self._active and self._thread_handle is not None:
                self._thread_handle.cancel()
            self._condition.notify()
        if queued is not None:
            loop, future, _ = queued
            loop.call_soon_threadsafe(
                self._complete,
                future,
                BrokenPipeError("protocol pipe is closed"),
            )

    async def wait_closed(self) -> None:
        """Wait asynchronously for the dedicated writer thread to stop."""
        await asyncio.to_thread(self._cancel_until_exited)

    def _cancel_until_exited(self) -> None:
        """Retry cancellation across the synchronous I/O startup race."""
        while not self._exited.wait(timeout=0.01):
            with self._condition:
                thread_handle = self._thread_handle
                active = self._active
            if active and thread_handle is not None:
                thread_handle.cancel()


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
    """Read the closed integrity manifest before importing Channel code."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise _fail("MANIFEST_INVALID", "Manifest is not valid JSON") from exc
    if not isinstance(data, dict) or set(data) != _MANIFEST_FIELDS:
        raise _fail("MANIFEST_INVALID", "Manifest fields do not match v1")
    if data["schema_version"] != 1:
        raise _fail("MANIFEST_INVALID", "Manifest schema_version must be 1")
    for field in ("code_root_sha256", "descriptor_sha256"):
        value = data[field]
        if (
            not isinstance(value, str)
            or _DIGEST_PATTERN.fullmatch(value) is None
        ):
            raise _fail(
                "MANIFEST_INVALID",
                f"Manifest {field} is invalid",
            )
    descriptor_path = data["descriptor_path"]
    if (
        not isinstance(descriptor_path, str)
        or not Path(
            descriptor_path,
        ).is_absolute()
    ):
        raise _fail(
            "MANIFEST_INVALID",
            "Manifest descriptor_path must be absolute",
        )
    platform_tags = data["allowed_platform_tags"]
    if (
        not isinstance(platform_tags, list)
        or not platform_tags
        or any(
            not isinstance(tag, str) or not tag or not tag.isascii()
            for tag in platform_tags
        )
        or len(platform_tags) != len(set(platform_tags))
    ):
        raise _fail(
            "MANIFEST_INVALID",
            "Manifest allowed_platform_tags is invalid",
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


def _sha256_file(path: Path) -> str:
    """Hash one exact file without applying descriptor semantics."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_manifest(code_root: Path, data: dict[str, Any]) -> Path:
    """Validate the code root and exact descriptor integrity."""
    try:
        actual = _hash_code_root(code_root)
    except OSError as exc:
        raise _fail("CODE_ROOT_INVALID", "Unable to hash code_root") from exc
    if actual != data["code_root_sha256"]:
        raise _fail("CODE_ROOT_MISMATCH", "code_root digest does not match")
    try:
        descriptor_path = Path(data["descriptor_path"]).resolve(
            strict=True,
        )
    except OSError as exc:
        raise _fail(
            "DESCRIPTOR_INVALID",
            "Descriptor path does not exist",
        ) from exc
    if not descriptor_path.is_file() or not descriptor_path.is_relative_to(
        code_root,
    ):
        raise _fail(
            "DESCRIPTOR_OUTSIDE_CODE_ROOT",
            "Descriptor is not a regular file inside code_root",
        )
    try:
        descriptor_digest = _sha256_file(descriptor_path)
    except OSError as exc:
        raise _fail(
            "DESCRIPTOR_INVALID",
            "Unable to hash descriptor",
        ) from exc
    if descriptor_digest != data["descriptor_sha256"]:
        raise _fail(
            "DESCRIPTOR_MISMATCH",
            "Descriptor digest does not match",
        )
    return descriptor_path


def _remove_ambient_import_environment() -> None:
    """Remove import path variables before loading the Protocol SDK."""
    for name in tuple(os.environ):
        if name.upper() in _AMBIENT_IMPORT_ENVIRONMENT:
            os.environ.pop(name, None)


def _add_code_root(code_root: Path) -> None:
    """Select the validated source root and bypass the Core package init."""
    protocol_root = code_root / "qwenpaw" / "channel_protocol"
    if not (
        (protocol_root / "descriptor.py").is_file()
        and (protocol_root / "framing.py").is_file()
    ):
        raise _fail(
            "PROTOCOL_SDK_INVALID",
            "code_root does not contain the Channel Protocol SDK",
        )
    selected = str(code_root)
    bootstrap_root = Path(__file__).resolve().parent.parent.parent
    sys.dont_write_bytecode = True
    sys.path[:] = [selected] + [
        item
        for item in sys.path
        if item
        and Path(item).resolve() != code_root
        and Path(item).resolve() != bootstrap_root
    ]
    package = types.ModuleType("qwenpaw")
    package.__package__ = "qwenpaw"
    package.__path__ = [str(code_root / "qwenpaw")]
    sys.modules["qwenpaw"] = package


def _load_protocol_sdk() -> tuple[Any, Any, type[BaseException]]:
    """Load the validated descriptor and framing implementations."""
    try:
        descriptor_module = importlib.import_module(
            "qwenpaw.channel_protocol.descriptor",
        )
        framing_module = importlib.import_module(
            "qwenpaw.channel_protocol.framing",
        )
        errors_module = importlib.import_module(
            "qwenpaw.channel_protocol.errors",
        )
    except ImportError as exc:
        raise _fail(
            "PROTOCOL_SDK_IMPORT_FAILED",
            "Unable to import the Channel Protocol SDK",
        ) from exc
    return (
        descriptor_module.ChannelDescriptor,
        framing_module.FramedTransport,
        errors_module.DescriptorValidationError,
    )


def _load_descriptor(
    path: Path,
    manifest: dict[str, Any],
    descriptor_class: Any,
    validation_error: type[BaseException],
) -> Any:
    """Parse the descriptor through the CH-0-002 validator."""
    try:
        data = path.read_bytes()
        descriptor = descriptor_class.from_json(
            data,
            allowed_platform_tags=manifest["allowed_platform_tags"],
        )
    except (OSError, UnicodeError, validation_error) as exc:
        raise _fail(
            "DESCRIPTOR_INVALID",
            "Descriptor failed v1 validation",
        ) from exc
    if (
        descriptor.process_mode != "runner_process"
        or descriptor.entrypoint.scope != "runner"
    ):
        raise _fail(
            "DESCRIPTOR_NOT_RUNNER",
            "Descriptor does not declare a Runner ChannelDriver",
        )
    return descriptor


def _entrypoint_source(code_root: Path, entrypoint: Any) -> Path:
    """Resolve the descriptor entrypoint source inside the trusted root."""
    module_path = code_root.joinpath(*entrypoint.module.split("."))
    candidates = (module_path.with_suffix(".py"), module_path / "__init__.py")
    for candidate in candidates:
        if candidate.is_file():
            resolved = candidate.resolve()
            if resolved.is_relative_to(code_root):
                return resolved
    raise _fail(
        "ENTRYPOINT_OUTSIDE_CODE_ROOT",
        "Descriptor entrypoint module is not inside code_root",
    )


def _clean_environment(passthrough: tuple[str, ...]) -> None:
    """Keep startup variables and descriptor-declared passthrough only."""
    allowed = set(_STARTUP_ENVIRONMENT)
    allowed.update(passthrough)
    if os.name == "nt":
        allowed = {name.upper() for name in allowed}
    for name in tuple(os.environ):
        canonical_name = name.upper() if os.name == "nt" else name
        if (
            name.upper() in _AMBIENT_IMPORT_ENVIRONMENT
            or canonical_name not in allowed
        ):
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
    """Route inheritable ordinary stdout and native FD 1 to stderr."""
    try:
        os.dup2(2, 1, inheritable=True)
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


def _load_driver_class(entrypoint: Any, expected_source: Path) -> Any:
    """Import and resolve only the descriptor-declared driver class."""
    try:
        module = importlib.import_module(entrypoint.module)
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
        for part in entrypoint.qualname.split("."):
            value = getattr(value, part)
        return value
    except (AttributeError, ImportError) as exc:
        raise _fail(
            "ENTRYPOINT_IMPORT_FAILED",
            "Unable to load declared ChannelDriver entrypoint",
        ) from exc


async def _start_runner(
    descriptor: Any,
    expected_source: Path,
    framed_transport_class: Any,
    protocol_handle: Any,
) -> None:
    """Start one driver with the CH-0-003 protocol transport."""
    transport = await _open_protocol_transport(
        framed_transport_class,
        protocol_handle,
    )
    runner = _RunnerProcess(descriptor, transport)
    try:
        runner.start(expected_source)
    finally:
        await transport.aclose()


async def _open_protocol_transport(
    framed_transport_class: Any,
    protocol_handle: Any,
    *,
    limits: Any | None = None,
) -> Any:
    """Connect the private pipe directly to CH-0-003 framing."""
    if os.name == "nt":
        try:
            writer: Any = _ThreadPipeWriter(protocol_handle)
        except OSError as exc:
            protocol_handle.close()
            raise _fail(
                "PROTOCOL_HANDLE_INVALID",
                "Unable to connect protocol stdout",
            ) from exc
    else:
        loop = asyncio.get_running_loop()

        def protocol_factory() -> asyncio.Protocol:
            return asyncio.streams.FlowControlMixin(loop=loop)

        try:
            write_transport, write_protocol = await loop.connect_write_pipe(
                protocol_factory,
                protocol_handle,
            )
        except (OSError, ValueError) as exc:
            protocol_handle.close()
            raise _fail(
                "PROTOCOL_HANDLE_INVALID",
                "Unable to connect protocol stdout",
            ) from exc
        writer = asyncio.StreamWriter(
            write_transport,
            write_protocol,
            None,
            loop,
        )
    return framed_transport_class(
        asyncio.StreamReader(),
        writer,
        limits=limits,
    )


def _report_error(error: BootstrapError) -> None:
    """Report a pre-import failure on stderr without protocol output."""
    payload = {
        "error": error.reason_code,
        "message": str(error),
    }
    sys.stderr.write(f"{json.dumps(payload, separators=(',', ':'))}\n")
    sys.stderr.flush()


def main(argv: list[str] | None = None) -> int:
    """Validate, isolate, redirect output, then construct one driver."""
    arguments = list(sys.argv if argv is None else argv)
    try:
        _require_isolated_python()
        code_root, manifest_path = _parse_arguments(arguments)
        manifest = _read_manifest(manifest_path)
        descriptor_path = _validate_manifest(code_root, manifest)
        _remove_ambient_import_environment()
        _add_code_root(code_root)
        (
            descriptor_class,
            framed_transport_class,
            validation_error,
        ) = _load_protocol_sdk()
        descriptor = _load_descriptor(
            descriptor_path,
            manifest,
            descriptor_class,
            validation_error,
        )
        entrypoint_source = _entrypoint_source(
            code_root,
            descriptor.entrypoint,
        )
        _clean_environment(descriptor.environment_passthrough_allowlist)
        protocol_handle = _capture_protocol_output()
        _redirect_ordinary_output()
        asyncio.run(
            _start_runner(
                descriptor,
                entrypoint_source,
                framed_transport_class,
                protocol_handle,
            ),
        )
        return 0
    except BootstrapError as exc:
        _report_error(exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
