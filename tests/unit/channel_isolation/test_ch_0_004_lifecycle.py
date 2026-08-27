# -*- coding: utf-8 -*-
"""Tests for CH-0-004 Runner lifecycle and fencing."""

# pylint: disable=protected-access

from __future__ import annotations

import asyncio
import traceback

import pytest

from qwenpaw.channel_protocol import (
    EndpointParams,
    HostContext,
    HostStateParams,
    IdentityParams,
    LeaseParams,
    LifecycleController,
    CoreLifecycleAdapter,
    FixtureSecretHandleConsumer,
    HelloParams,
    PrepareParams,
    RpcError,
    RunnerState,
    RpcPeer,
    SendParams,
)


from tests.unit.channel_isolation._ch_0_004_support import (
    Clock,
    _controller,
    _endpoint,
    _hello,
    _hello_expectation,
    _identity,
    _transport_pair,
)


@pytest.mark.asyncio
async def test_endpoint_register_hook_can_reenter_lifecycle() -> None:
    """Endpoint registration invokes Driver work outside the state lock."""
    callback_state: list[str] = []
    controller: LifecycleController

    async def reentrant_hook(
        operation: str,
        _: EndpointParams | None,
    ) -> None:
        if operation == "register":
            health = await controller.health(
                IdentityParams.from_mapping(_identity()),
            )
            callback_state.append(health["state"])

    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        capabilities=("ingress_endpoint",),
        endpoint_handler=reentrant_hook,
        clock_ms=Clock(),
    )
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": ["ingress_endpoint"],
            },
        ),
    )
    await controller.endpoint_register(_endpoint())
    assert callback_state == ["standby"]


async def test_lifecycle_requires_hello_and_commit() -> None:
    """Prepare and activate remain standby until commit succeeds."""
    clock = Clock()
    controller = _controller(clock)
    with pytest.raises(RpcError):
        await controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": HostContext(
                        media_work_dir="/tmp/media",
                    ).to_mapping(),
                    "capabilities": ["media"],
                },
            ),
        )
    assert controller.state is RunnerState.CREATED
    assert controller.accept_hello(_hello())["protocol_version"] == 1
    prepared = await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {"media_work_dir": "/tmp/media"},
                "capabilities": ["ingress_endpoint", "media"],
            },
        ),
    )
    assert prepared["state"] == "standby"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 100},
    )
    activated = await controller.activate(lease)
    assert activated["state"] == "standby"
    with pytest.raises(RpcError):
        await controller.send(
            SendParams.from_mapping(
                {
                    **_identity(),
                    "delivery_id": "d1",
                    "to_handle": "call-1",
                    "content_parts": [{"type": "text", "text": "hello"}],
                },
            ),
        )
    committed = await controller.commit(lease)
    assert committed["state"] == "active"
    sent = await controller.send(
        SendParams.from_mapping(
            {
                **_identity(),
                "delivery_id": "d1",
                "to_handle": "call-1",
                "content_parts": [{"type": "text", "text": "hello"}],
            },
        ),
    )
    assert sent["state"] == "acknowledged"


@pytest.mark.asyncio
async def test_generation_fencing_expiry_and_quiesce() -> None:
    """Stale generations and expired leases cannot continue operating."""
    clock = Clock()
    controller = _controller(clock)
    adapter = CoreLifecycleAdapter(controller, clock_ms=clock)
    controller.accept_hello(_hello())
    await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "opaque", "lease_ttl_ms": 10},
    )
    await controller.activate(lease)
    await controller.commit(lease)
    prepare_token = await adapter.authority.prepare_start(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {},
                "capabilities": [
                    "host_state",
                    "ingress_endpoint",
                    "media",
                ],
            },
        ),
    )
    await adapter.authority.prepare_complete(prepare_token)
    activate_token = await adapter.authority.activate_start(lease)
    await adapter.authority.activate_complete(activate_token, lease)
    commit_token = await adapter.authority.commit_start(lease)
    await adapter.authority.commit_complete(commit_token, lease)
    clock.now = 1011
    with pytest.raises(RpcError) as expired_write:
        await adapter.host_state_put(
            HostStateParams.from_mapping(
                {
                    **_identity(),
                    "key": "stale",
                    "value": {"blocked": True},
                },
            ),
        )
    assert expired_write.value.data["reason_code"] == "LEASE_EXPIRED"
    health = await controller.health(IdentityParams.from_mapping(_identity()))
    assert health["state"] == "failed"
    assert controller.state is RunnerState.FAILED
    with pytest.raises(RpcError) as fenced:
        await controller.stop(IdentityParams.from_mapping(_identity(8)))
    assert fenced.value.data["reason_code"] == "GENERATION_FENCED"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("protocol_version", 2, "PROTOCOL_MISMATCH"),
        ("qwenpaw_version", "0.2", "QWENPAW_VERSION_MISMATCH"),
        ("source_revision", "5" * 64, "SOURCE_REVISION_MISMATCH"),
        (
            "environment_spec_id",
            "ches1_" + "2" * 64,
            "ENVIRONMENT_SPEC_MISMATCH",
        ),
        (
            "environment_id",
            "ches1_" + "1" * 64 + ".install1_" + "3" * 32,
            "ENVIRONMENT_ID_MISMATCH",
        ),
        ("lock_sha256", "4" * 64, "LOCK_MISMATCH"),
        ("python_abi", "cp312-cp312", "PYTHON_ABI_MISMATCH"),
        ("platform_tag", "win_amd64", "PLATFORM_TAG_MISMATCH"),
    ],
)
def test_hello_environment_mismatch_has_stable_reason(
    field: str,
    value: object,
    reason: str,
) -> None:
    """Hello rejects every mismatch against Core-owned expectations."""
    controller = _controller(Clock())
    mapping = _hello().to_mapping()
    mapping[field] = value
    with pytest.raises(RpcError) as mismatch:
        controller.accept_hello(HelloParams.from_mapping(mapping))
    assert mismatch.value.data["reason_code"] == reason


async def test_secret_handle_is_consumed_once_during_prepare() -> None:
    """Fixture handles are generation-scoped, single-use, and not retained."""
    clock = Clock()
    secret_value = "fixture-secret-value"
    consumed_values: list[object] = []
    consumer = FixtureSecretHandleConsumer(
        {("fixture-handle", 7): secret_value},
        consumed_values.append,
    )
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    prepared = await controller.prepare(
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {"secret_handle": "fixture-handle"},
                "capabilities": [],
            },
        ),
    )
    assert prepared["state"] == "standby"
    assert consumed_values == [secret_value]
    assert controller.host_context is not None
    assert "secret_handle" not in controller.host_context.to_mapping()
    assert secret_value not in repr(consumer)
    assert "fixture-handle" not in repr(consumer)

    repeated = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    repeated.accept_hello(_hello())
    with pytest.raises(RpcError) as consumed:
        await repeated.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "fixture-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"
    assert repeated.state is RunnerState.FAILED


@pytest.mark.asyncio
async def test_secret_handle_invalid_and_auth_errors_are_stable() -> None:
    """Handle lookup failures differ from invalid consumed credentials."""
    clock = Clock()
    consumer = FixtureSecretHandleConsumer({}, lambda _: None)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    with pytest.raises(RpcError) as invalid:
        await controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "missing"},
                    "capabilities": [],
                },
            ),
        )
    assert invalid.value.data["reason_code"] == "SECRET_HANDLE_INVALID"

    def reject_credentials(value: object) -> None:
        """Simulate fixture platform authentication failure."""
        raise ValueError(f"invalid fixture credential: {value}")

    auth_consumer = FixtureSecretHandleConsumer(
        {("auth-handle", 7): "invalid-secret"},
        reject_credentials,
    )
    auth_controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=auth_consumer,
        clock_ms=clock,
    )
    auth_controller.accept_hello(_hello())
    with pytest.raises(RpcError) as auth_failed:
        await auth_controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "auth-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert auth_failed.value.data["reason_code"] == "PLATFORM_AUTH_FAILED"
    auth_traceback = "".join(
        traceback.format_exception(auth_failed.value),
    )
    assert "invalid-secret" not in auth_traceback
    assert auth_failed.value.__cause__ is None
    assert auth_failed.value.__context__ is None
    retried_controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=auth_consumer,
        clock_ms=clock,
    )
    retried_controller.accept_hello(_hello())
    with pytest.raises(RpcError) as consumed:
        await retried_controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "auth-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"


@pytest.mark.asyncio
async def test_secret_handle_without_consumer_is_invalid() -> None:
    """An opaque handle cannot be accepted without a prepare consumer."""
    clock = Clock()
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        clock_ms=clock,
    )
    controller.accept_hello(_hello())
    with pytest.raises(RpcError) as invalid:
        await controller.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "no-consumer"},
                    "capabilities": [],
                },
            ),
        )
    assert invalid.value.data["reason_code"] == "SECRET_HANDLE_INVALID"
    assert controller.state is RunnerState.FAILED


@pytest.mark.asyncio
async def test_secret_rpc_error_is_sanitized_before_rpc_response() -> None:
    """Consumer errors cannot copy a secret into an RPC error frame."""
    clock = Clock()
    secret_value = "secret-must-not-cross-rpc"

    def leak_rpc_error(value: object) -> None:
        """Raise a deliberately unsafe consumer error for regression."""
        raise RpcError(
            -32012,
            f"unsafe {value}",
            data={"credential": value},
        )

    consumer = FixtureSecretHandleConsumer(
        {("rpc-error-handle", 7): secret_value},
        leak_rpc_error,
    )
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    with pytest.raises(RpcError) as failed:
        await core.call(
            "channel.prepare",
            {
                **_identity(),
                "host_context": {"secret_handle": "rpc-error-handle"},
                "capabilities": [],
            },
        )
    assert failed.value.data == {"reason_code": "PLATFORM_AUTH_FAILED"}
    assert failed.value.__cause__ is None
    frames = left_transport.sent_messages + right_transport.sent_messages
    assert all(secret_value not in frame for frame in frames)
    assert secret_value not in repr(consumer)
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_cancelled_prepare_fails_and_consumes_secret_handle() -> None:
    """Cancelled prepare clears context but never restores its handle."""
    clock = Clock()
    started = asyncio.Event()
    release = asyncio.Event()

    async def blocked_sink(_: object) -> None:
        """Block credential initialization until the request is cancelled."""
        started.set()
        await release.wait()

    consumer = FixtureSecretHandleConsumer(
        {("cancelled-handle", 7): "cancelled-secret"},
        blocked_sink,
    )
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    prepare = asyncio.create_task(
        core.call(
            "channel.prepare",
            {
                **_identity(),
                "host_context": {"secret_handle": "cancelled-handle"},
                "capabilities": [],
            },
            timeout=1.0,
        ),
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-1", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError) as cancelled:
        await prepare
    assert cancelled.value.data["reason_code"] == "REQUEST_CANCELLED"
    assert controller.state is RunnerState.FAILED
    assert controller.host_context is None
    assert not controller.effective_capabilities
    repeated = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    repeated.accept_hello(_hello())
    with pytest.raises(RpcError) as consumed:
        await repeated.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "cancelled-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_swallowed_secret_cancel_fails_prepare() -> None:
    """Prepare cancellation cannot be converted into a standby success."""
    clock = Clock()
    started = asyncio.Event()

    async def swallowing_sink(_: object) -> None:
        """Suppress cancellation after fixture credential initialization."""
        started.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            pass

    consumer = FixtureSecretHandleConsumer(
        {("swallowed-handle", 7): "swallowed-secret"},
        swallowing_sink,
    )
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    prepare = asyncio.create_task(
        core.call(
            "channel.prepare",
            {
                **_identity(),
                "host_context": {"secret_handle": "swallowed-handle"},
                "capabilities": [],
            },
        ),
    )
    await asyncio.wait_for(started.wait(), timeout=1.0)
    await core.notify(
        "request.cancel",
        {"request_id": "rpc-1", "reason": "user_cancelled"},
    )
    with pytest.raises(RpcError) as cancelled:
        await prepare
    assert cancelled.value.data["reason_code"] == "REQUEST_CANCELLED"
    assert controller.state is RunnerState.FAILED
    assert controller.host_context is None
    assert not controller.effective_capabilities
    repeated = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    repeated.accept_hello(_hello())
    with pytest.raises(RpcError) as consumed:
        await repeated.prepare(
            PrepareParams.from_mapping(
                {
                    **_identity(),
                    "host_context": {"secret_handle": "swallowed-handle"},
                    "capabilities": [],
                },
            ),
        )
    assert consumed.value.data["reason_code"] == "SECRET_HANDLE_CONSUMED"
    await asyncio.gather(core.aclose(), runner.aclose())


@pytest.mark.asyncio
async def test_outbound_and_secret_contract_crosses_rpc_dispatch() -> None:
    """RPC carries stable operations while secret values stay in-process."""
    clock = Clock()
    secret_value = "fixture-secret-never-on-wire"
    consumed: list[object] = []
    consumer = FixtureSecretHandleConsumer(
        {("rpc-fixture-handle", 7): secret_value},
        consumed.append,
    )
    left_transport, right_transport = _transport_pair()
    core = RpcPeer(left_transport)
    runner = RpcPeer(right_transport)
    controller = LifecycleController(
        channel_key="voice",
        instance_id="instance-1",
        generation=7,
        environment_spec_id="ches1_" + "1" * 64,
        environment_id="ches1_" + "1" * 64 + ".install1_" + "2" * 32,
        **_hello_expectation(),
        capabilities=("approval_card", "reaction", "streaming"),
        secret_handle_consumer=consumer,
        clock_ms=clock,
    )
    controller.register_rpc_methods(runner)
    CoreLifecycleAdapter(controller).register_rpc_methods(core)
    await asyncio.gather(core.start(), runner.start())
    await runner.call("runner.hello", _hello().to_mapping())
    prepared = await core.call(
        "channel.prepare",
        PrepareParams.from_mapping(
            {
                **_identity(),
                "host_context": {
                    "secret_handle": "rpc-fixture-handle",
                },
                "capabilities": [
                    "approval_card",
                    "reaction",
                    "streaming",
                ],
            },
        ).to_mapping(),
    )
    assert prepared["state"] == "standby"
    lease = LeaseParams.from_mapping(
        {**_identity(), "lease_token": "rpc-outbound", "lease_ttl_ms": 100},
    )
    await core.call("channel.activate", lease.to_mapping())
    await core.call("channel.commit", lease.to_mapping())
    created = await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "rpc-message-1",
            "to_handle": "chat-1",
            "operation": "message.create",
            "content_parts": [{"type": "text", "text": "Approve?"}],
            "approval": {
                "request_id": "request-1",
                "tool_name": "shell",
                "severity": "high",
            },
        },
    )
    assert created["state"] == "acknowledged"
    await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "rpc-stream-1",
            "to_handle": "chat-1",
            "operation": "stream.start",
            "stream_type": "message",
            "sequence": 0,
            "accumulated_text": "",
        },
    )
    await core.call(
        "channel.send",
        {
            **_identity(),
            "delivery_id": "rpc-stream-end-1",
            "to_handle": "chat-1",
            "operation": "stream.end",
            "target_delivery_id": "rpc-stream-1",
            "stream_type": "message",
            "sequence": 1,
            "accumulated_text": "done",
        },
    )
    reaction = await core.call(
        "channel.reaction",
        {
            **_identity(),
            "delivery_id": "rpc-reaction-1",
            "to_handle": "chat-1",
            "target_delivery_id": "rpc-stream-1",
            "reaction": "completed",
        },
    )
    assert reaction["state"] == "acknowledged"
    assert consumed == [secret_value]
    frames = left_transport.sent_messages + right_transport.sent_messages
    assert any("rpc-fixture-handle" in frame for frame in frames)
    assert all(secret_value not in frame for frame in frames)
    assert controller.host_context is not None
    assert "secret_handle" not in controller.host_context.to_mapping()
    await asyncio.gather(core.aclose(), runner.aclose())
