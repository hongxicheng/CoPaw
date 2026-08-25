# -*- coding: utf-8 -*-
"""Voice channel package with lazy Core-only exports."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .channel import VoiceChannel


def __getattr__(name: str) -> Any:
    """Avoid importing the Core Channel for the Runner entrypoint."""
    if name == "VoiceChannel":
        from .channel import VoiceChannel

        return VoiceChannel
    raise AttributeError(name)


__all__ = ["VoiceChannel"]
