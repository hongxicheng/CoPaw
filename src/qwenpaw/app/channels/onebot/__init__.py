# -*- coding: utf-8 -*-
"""OneBot channel package with lazy Core-only exports."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .channel import OneBotChannel


def __getattr__(name: str) -> Any:
    """Avoid importing the Core Channel for the Runner entrypoint."""
    if name == "OneBotChannel":
        from .channel import OneBotChannel

        return OneBotChannel
    raise AttributeError(name)


__all__ = ["OneBotChannel"]
