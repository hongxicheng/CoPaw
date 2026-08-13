# -*- coding: utf-8 -*-
"""Errors raised by Channel protocol value-model validation."""

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


def validation_error(
    message: str,
    *,
    path: Sequence[PathPart] = (),
) -> DescriptorValidationError:
    """Build the stable validation exception used by pure models."""
    return DescriptorValidationError(message, path=path)
