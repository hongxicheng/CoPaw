# -*- coding: utf-8 -*-
"""Tests for shared channel utilities."""

import base64
from pathlib import Path

import pytest

from qwenpaw.app.channels.utils import (
    MediaDataError,
    materialize_data_url,
    parse_data_url,
)


PNG_DATA_URL = (
    "data:image/png;base64," + base64.b64encode(b"png-data").decode()
)


def test_parse_data_url_decodes_payload() -> None:
    """Base64 data URLs should return bytes and normalized metadata."""
    media = parse_data_url(PNG_DATA_URL)

    assert media is not None
    assert media.data == b"png-data"
    assert media.media_type == "image/png"
    assert media.suffix == ".png"


def test_parse_data_url_rejects_invalid_base64() -> None:
    """Malformed Base64 should fail before any filesystem access."""
    with pytest.raises(MediaDataError):
        parse_data_url("data:image/png;base64,not-valid!")


def test_parse_data_url_enforces_size_limit() -> None:
    """Decoded media must respect the configured size limit."""
    with pytest.raises(MediaDataError):
        parse_data_url(PNG_DATA_URL, max_bytes=2)


def test_materialize_data_url_cleans_up_file(tmp_path) -> None:
    """Generated files should exist only for the send operation."""
    with materialize_data_url(
        PNG_DATA_URL,
        tmp_path,
        filename_hint="screen capture.png",
    ) as path:
        materialized = Path(path)
        assert materialized.read_bytes() == b"png-data"
        assert materialized.suffix == ".png"

    assert not materialized.exists()


def test_materialize_data_url_preserves_local_path(tmp_path) -> None:
    """Non-data references should not be rewritten."""
    local_path = tmp_path / "file.bin"
    local_path.write_bytes(b"data")

    with materialize_data_url(str(local_path), tmp_path) as path:
        assert path == str(local_path)
