# -*- coding: utf-8 -*-
"""Tests for CH-0-004 JSON-RPC and Channel protocol DTOs."""

from __future__ import annotations

import pytest

from qwenpaw.channel_protocol import (
    EndpointParams,
    HostContext,
    HostStateParams,
    LeaseParams,
    ProtocolValidationError,
    RpcErrorObject,
    RpcRequest,
    RpcResponse,
    SendParams,
    VoiceEventKind,
    VoiceSetup,
    VoiceStatusCallback,
    parse_rpc_message,
    validate_content_part,
    voice_event_from_setup,
    voice_event_from_status_callback,
)


def _identity() -> dict[str, object]:
    """Return a valid control identity fixture."""
    return {
        "channel_key": "voice",
        "instance_id": "instance-1",
        "generation": 7,
    }


def test_rpc_envelopes_round_trip_and_reject_unknown_fields() -> None:
    """Requests and responses retain JSON-RPC 2.0 envelope semantics."""
    request = RpcRequest.from_mapping(
        {"jsonrpc": "2.0", "id": "r1", "method": "health", "params": {}},
    )
    assert request.to_mapping()["method"] == "health"
    response = RpcResponse.from_mapping(
        {"jsonrpc": "2.0", "id": "r1", "result": None},
    )
    assert response.to_mapping()["result"] is None
    assert parse_rpc_message(request.to_mapping()) == request
    with pytest.raises(ProtocolValidationError) as exc_info:
        RpcRequest.from_mapping(
            {"jsonrpc": "2.0", "id": "r1", "method": "health", "extra": 1},
        )
    assert exc_info.value.reason_code == "SCHEMA_MISMATCH"


def test_rpc_error_uses_integer_code_and_stable_reason_data() -> None:
    """Stable business reasons remain JSON-RPC-compliant integers plus data."""
    error = RpcErrorObject(
        -32010,
        "invalid state",
        {"reason_code": "INVALID_STATE_TRANSITION"},
    )
    assert error.to_mapping() == {
        "code": -32010,
        "message": "invalid state",
        "data": {"reason_code": "INVALID_STATE_TRANSITION"},
    }


def test_host_context_requires_cross_platform_absolute_media_path() -> None:
    """The media work directory is absolute on POSIX and Windows forms."""
    assert (
        HostContext.from_mapping(
            {"media_work_dir": "/tmp/media"},
        ).media_work_dir
        == "/tmp/media"
    )
    assert (
        HostContext.from_mapping(
            {"media_work_dir": r"C:\\media"},
        ).media_work_dir
        == r"C:\\media"
    )
    with pytest.raises(ProtocolValidationError):
        HostContext.from_mapping({"media_work_dir": "relative/media"})


def test_media_locator_and_send_schema_are_closed() -> None:
    """Outbound media carries locators, never inline binary content."""
    image = validate_content_part(
        {"type": "image", "image_url": "https://example/image"},
    )
    assert image["image_url"].startswith("https://")
    send = SendParams.from_mapping(
        {
            **_identity(),
            "delivery_id": "delivery-1",
            "to_handle": "call-1",
            "content_parts": [image],
        },
    )
    assert send.content_parts[0]["type"] == "image"
    with pytest.raises(ProtocolValidationError):
        validate_content_part(
            {
                "type": "image",
                "image_url": "https://example/image",
                "bytes": "bad",
            },
        )


def test_endpoint_rejects_unauthenticated_external_binding() -> None:
    """A non-loopback endpoint cannot be committed without auth."""
    with pytest.raises(ProtocolValidationError) as exc_info:
        EndpointParams.from_mapping(
            {
                **_identity(),
                "protocol": "http",
                "host": "0.0.0.0",
                "port": 8080,
                "path": "/voice",
                "public_base_url": None,
                "readiness": "ready",
                "bound_externally": True,
                "auth_required": False,
                "quiescing": False,
            },
        )
    assert exc_info.value.reason_code == "AUTH_FAILED"


def test_voice_setup_and_terminal_callback_build_stable_events() -> None:
    """ConversationRelay fields map to stable protocol event DTOs."""
    setup = VoiceSetup.from_mapping(
        {"type": "setup", "callSid": "CA123", "from": "+1", "to": "+2"},
    )
    started = voice_event_from_setup(
        setup,
        event_id="event-1",
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        connection_id="connection-1",
        session_binding="binding-1",
    )
    assert started.event_kind is VoiceEventKind.CALL_STARTED
    callback = VoiceStatusCallback.from_mapping(
        {"CallSid": "CA123", "CallStatus": "completed"},
    )
    closed = voice_event_from_status_callback(
        callback,
        event_id="event-2",
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        connection_id="connection-1",
        session_binding="binding-1",
        sequence=2,
    )
    assert closed.event_kind is VoiceEventKind.CALL_CLOSED
    assert closed.payload == {"status": "completed"}


def test_host_state_key_is_relative_and_lease_has_positive_ttl() -> None:
    """Host state stays instance-scoped and lease TTL cannot be disabled."""
    params = HostStateParams.from_mapping({**_identity(), "key": "checkpoint"})
    assert params.key == "checkpoint"
    with pytest.raises(ProtocolValidationError):
        HostStateParams.from_mapping({**_identity(), "key": "../escape"})
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 1000},
    )
    assert lease.lease_ttl_ms == 1000
