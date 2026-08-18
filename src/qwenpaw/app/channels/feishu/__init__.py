# -*- coding: utf-8 -*-
"""Feishu channel package with lazy Core-only exports."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .channel import FeishuChannel


def __getattr__(name: str) -> Any:
    """Avoid importing the Core Channel for the Runner entrypoint."""
    if name == "FeishuChannel":
        from .channel import FeishuChannel

        return FeishuChannel
    raise AttributeError(name)


__all__ = ["FeishuChannel"]
