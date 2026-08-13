# -*- coding: utf-8 -*-
"""Errors raised by Channel protocol primitives."""

from __future__ import annotations

from collections.abc import Sequence


PathPart = str | int


class DescriptorValidationError(ValueError):
    """Report an invalid descriptor or related pure value model."""

    code = "descriptor_invalid"

    def __init__(
        self,
        message: str,
        *,
        path: Sequence[PathPart] = (),
    ) -> None:
        self.path = tuple(path)
        super().__init__(message)


class FrameError(Exception):
    """Base class for strict stdio framing and transport failures."""


class FrameProtocolError(FrameError):
    """Report malformed framing or invalid UTF-8."""


class FrameLimitError(FrameProtocolError):
    """Report a frame or header exceeding its configured bound."""


class FrameEOFError(FrameError):
    """Report clean EOF at a frame boundary."""


class FrameTimeoutError(FrameError):
    """Report a read, queue, or write deadline expiry."""


class FrameClosedError(FrameError):
    """Report an operation attempted on a closed transport."""


class FrameWriteError(FrameError):
    """Report an OS or stream failure while writing a frame."""


def validation_error(
    message: str,
    *,
    path: Sequence[PathPart] = (),
) -> DescriptorValidationError:
    """Build the stable validation exception used by pure models."""
    return DescriptorValidationError(message, path=path)
