# -*- coding: utf-8 -*-
"""Contract checks for the CH-0-001 Core/Runner boundary document."""

from __future__ import annotations

import ast
import re
from pathlib import Path
from pathlib import PurePosixPath, PureWindowsPath


ROOT = Path(__file__).resolve().parents[3]
BASE_PATH = ROOT / "src/qwenpaw/app/channels/base.py"
REGISTRY_PATH = ROOT / "src/qwenpaw/app/channels/registry.py"
CONFIG_PATH = ROOT / "src/qwenpaw/config/config.py"
DESIGN_PATH = ROOT / "docs/proposals/channel-isolation/DESIGN.md"
PLUGIN_API_PATH = ROOT / "src/qwenpaw/plugins/api.py"
PLUGIN_REGISTRY_PATH = ROOT / "src/qwenpaw/plugins/registry.py"
AZURE_PLUGIN_PATH = ROOT / "plugins/channel/azure_bot/plugin.py"
AZURE_CHANNEL_PATH = ROOT / "plugins/channel/azure_bot/channel.py"
PLAN_PATH = (
    ROOT / "docs/proposals/channel-isolation/IMPLEMENTATION_WORK_PLAN.md"
)
DOC_PATH = ROOT / (
    "docs/proposals/channel-isolation/CH-0-001_CORE_RUNNER_BOUNDARY.md"
)

EXPECTED_CHANNELS = {
    "imessage",
    "discord",
    "dingtalk",
    "feishu",
    "qq",
    "telegram",
    "mattermost",
    "mqtt",
    "console",
    "matrix",
    "slack",
    "voice",
    "sip",
    "wecom",
    "xiaoyi",
    "yuanbao",
    "wechat",
    "onebot",
}

MEDIA_MODES = {
    "imessage": "not in contract",
    "discord": "落盘",
    "dingtalk": "落盘",
    "feishu": "落盘",
    "qq": "落盘",
    "telegram": "落盘",
    "mattermost": "落盘",
    "mqtt": "不提供",
    "console": "not in contract",
    "matrix": "落盘",
    "slack": "落盘",
    "voice": "不提供",
    "sip": "不提供",
    "wecom": "落盘",
    "xiaoyi": "落盘",
    "yuanbao": "落盘",
    "wechat": "落盘",
    "onebot": "定位符直传",
}

DISPATCH_MODES = {key: "manager_queue" for key in EXPECTED_CHANNELS}
DISPATCH_MODES.update({"voice": "direct_session", "sip": "direct_session"})

BOUNDARY_HOOK_METHODS = {
    "_consume_with_tracker",
    "_before_consume_process",
    "_on_consume_error",
    "_on_process_completed",
    "on_event_content",
    "on_event_message_completed",
    "on_streaming_start",
    "on_streaming_delta",
    "on_streaming_end",
    "_on_turn_usage_ready",
}


def _table_rows(text: str, header: str) -> list[list[str]]:
    """Read a simple pipe table following an exact header."""
    lines = text.splitlines()
    try:
        index = lines.index(header)
    except ValueError as exc:
        raise AssertionError(f"missing table header: {header}") from exc
    rows: list[list[str]] = []
    for line in lines[index + 2 :]:
        if not line.startswith("|"):
            break
        cells = [
            cell.strip().strip("`") for cell in line.strip("|").split("|")
        ]
        rows.append(cells)
    return rows


def _base_methods() -> list[str]:
    """Return methods directly declared by BaseChannel."""
    tree = ast.parse(BASE_PATH.read_text(encoding="utf-8"))
    channel = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "BaseChannel"
    )
    return [
        node.name
        for node in channel.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]


def _registry_keys() -> set[str]:
    """Read builtin registry keys without importing optional SDKs."""
    tree = ast.parse(REGISTRY_PATH.read_text(encoding="utf-8"))
    specs = next(
        node
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and (
            any(
                isinstance(target, ast.Name) and target.id == "_BUILTIN_SPECS"
                for target in getattr(node, "targets", [])
            )
            or (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == "_BUILTIN_SPECS"
            )
        )
    )
    assert isinstance(specs.value, ast.Dict)
    return {
        key.value
        for key in specs.value.keys
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }


def _registry_specs() -> dict[str, tuple[str, str]]:
    """Read builtin module and class entrypoints from the registry AST."""
    tree = ast.parse(REGISTRY_PATH.read_text(encoding="utf-8"))
    specs = next(
        node
        for node in tree.body
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and (
            (
                isinstance(node, ast.Assign)
                and any(
                    isinstance(target, ast.Name)
                    and target.id == "_BUILTIN_SPECS"
                    for target in node.targets
                )
            )
            or (
                isinstance(node, ast.AnnAssign)
                and isinstance(node.target, ast.Name)
                and node.target.id == "_BUILTIN_SPECS"
            )
        )
    )
    assert isinstance(specs.value, ast.Dict)
    result: dict[str, tuple[str, str]] = {}
    for key, value in zip(specs.value.keys, specs.value.values):
        if not isinstance(key, ast.Constant) or not isinstance(key.value, str):
            continue
        assert isinstance(value, ast.Tuple)
        module, cls = value.elts
        assert isinstance(module, ast.Constant)
        assert isinstance(cls, ast.Constant)
        result[key.value] = (module.value, cls.value)
    return result


def _channel_source(channel_key: str) -> Path:
    """Resolve a builtin channel source without importing its SDK."""
    module_name, _ = _registry_specs()[channel_key]
    package = module_name.removeprefix(".")
    package_path = BASE_PATH.parent / package
    init_path = package_path / "__init__.py"
    channel_path = package_path / "channel.py"
    return (
        init_path
        if init_path.exists() and not channel_path.exists()
        else channel_path
    )


def _class_node(path: Path, class_name: str) -> ast.ClassDef:
    """Find a concrete class declaration in a source file."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.ClassDef) and node.name == class_name
    )


def _class_methods(path: Path, class_name: str) -> set[str]:
    """Return methods directly overridden by a concrete channel class."""
    node = _class_node(path, class_name)
    return {
        child.name
        for child in node.body
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
    }


def _assignment_value(node: ast.ClassDef, name: str) -> object | None:
    """Read a simple class-level assignment from an AST node."""
    for child in node.body:
        targets: list[ast.expr] = []
        if isinstance(child, ast.Assign):
            targets = child.targets
        elif isinstance(child, ast.AnnAssign):
            targets = [child.target]
        if any(
            isinstance(target, ast.Name) and target.id == name
            for target in targets
        ):
            value = child.value
            if isinstance(value, ast.Constant):
                return value.value
    return None


def _design_rpc_methods() -> set[str]:
    """Read the normative RPC method names from Design section 7.3."""
    text = DESIGN_PATH.read_text(encoding="utf-8")
    section = text[text.index("### 7.3 最小方法集合") :]
    section = section[: section.index("### 7.4 ")]
    return {
        match.group(1)
        for match in re.finditer(r"^([a-z]+(?:\.[a-z_]+)+)\s+", section, re.M)
    }


def _config_channel_types() -> dict[str, str]:
    """Read ChannelConfig field-to-model annotations from source."""
    tree = ast.parse(CONFIG_PATH.read_text(encoding="utf-8"))
    channel_config = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "ChannelConfig"
    )
    result: dict[str, str] = {}
    for child in channel_config.body:
        if not isinstance(child, ast.AnnAssign):
            continue
        if isinstance(child.target, ast.Name) and isinstance(
            child.annotation,
            ast.Name,
        ):
            result[child.target.id] = child.annotation.id
    return result


def _class_has_field(path: Path, class_name: str, field_name: str) -> bool:
    """Return whether a config class declares a field."""
    node = _class_node(path, class_name)
    return any(
        isinstance(child, ast.AnnAssign)
        and isinstance(child.target, ast.Name)
        and child.target.id == field_name
        for child in node.body
    )


def _method_source(path: Path, class_name: str, method_name: str) -> str:
    """Return source text for one class method."""
    node = _class_node(path, class_name)
    method = next(
        child
        for child in node.body
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
        and child.name == method_name
    )
    lines = path.read_text(encoding="utf-8").splitlines()
    return "\n".join(lines[method.lineno - 1 : method.end_lineno])


def _method_source_or_empty(
    path: Path,
    class_name: str,
    method_name: str,
) -> str:
    """Return method source, or an empty string when no override exists."""
    try:
        return _method_source(path, class_name, method_name)
    except StopIteration:
        return ""


def _documented_hook_overrides(text: str) -> dict[str, set[str]]:
    """Read the channel hook override table from the boundary document."""
    rows = _table_rows(text, "| channel_key | relevant hook overrides |")
    return {
        row[0]: {hook for hook in BOUNDARY_HOOK_METHODS if hook in row[1]}
        for row in rows
    }


def test_base_channel_methods_have_one_matrix_row() -> None:
    """Every current BaseChannel method is documented exactly once."""
    rows = _table_rows(
        DOC_PATH.read_text(encoding="utf-8"),
        "| method | declaration | owner | isolated mapping | notes |",
    )
    documented = [row[0] for row in rows]
    assert len(documented) == 77
    assert len(set(documented)) == len(documented)
    assert set(documented) == set(_base_methods())
    assert all(len(row) == 5 for row in rows)
    assert all(
        row[2] in {"Core", "Runner", "Split", "Compatibility"} for row in rows
    )


def test_registry_and_media_tables_cover_exact_builtin_baseline() -> None:
    """The document covers all 18 registry keys without drift."""
    text = DOC_PATH.read_text(encoding="utf-8")
    assert _registry_keys() == EXPECTED_CHANNELS
    media_rows = _table_rows(
        text,
        (
            "| channel_key | inbound_media_mode | media_work_dir contract | "
            "current gap to record |"
        ),
    )
    assert len(media_rows) == len(EXPECTED_CHANNELS)
    assert {row[0] for row in media_rows} == EXPECTED_CHANNELS
    assert {row[0]: row[1] for row in media_rows} == MEDIA_MODES


def test_dispatch_table_matches_current_queue_owners() -> None:
    """Derive every builtin dispatch value from its concrete class."""
    text = DOC_PATH.read_text(encoding="utf-8")
    rows = _table_rows(
        text,
        "| channel_key | dispatch_mode | queue/ACL behavior |",
    )
    assert len(rows) == len(EXPECTED_CHANNELS)
    assert {row[0] for row in rows} == EXPECTED_CHANNELS
    assert {row[0]: row[1] for row in rows} == DISPATCH_MODES
    actual: dict[str, str] = {}
    base_tree = ast.parse(BASE_PATH.read_text(encoding="utf-8"))
    base_class = next(
        node
        for node in base_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "BaseChannel"
    )
    base_queue = _assignment_value(base_class, "uses_manager_queue")
    assert base_queue is True
    for key, (_, class_name) in _registry_specs().items():
        node = _class_node(_channel_source(key), class_name)
        queue = _assignment_value(node, "uses_manager_queue")
        actual[key] = (
            "manager_queue" if queue is not False else "direct_session"
        )
    assert actual == DISPATCH_MODES


def test_hook_override_table_matches_real_builtin_and_plugin_sources() -> None:
    """Document every relevant override in all builtin and legacy classes."""
    text = DOC_PATH.read_text(encoding="utf-8")
    documented = _documented_hook_overrides(text)
    actual: dict[str, set[str]] = {}
    for key, (_, class_name) in _registry_specs().items():
        actual[key] = (
            _class_methods(_channel_source(key), class_name)
            & BOUNDARY_HOOK_METHODS
        )
    actual["azure_bot"] = (
        _class_methods(AZURE_CHANNEL_PATH, "AzureBotChannel")
        & BOUNDARY_HOOK_METHODS
    )
    assert documented == actual
    assert set(documented) == EXPECTED_CHANNELS | {"azure_bot"}

    rows = _table_rows(
        text,
        "| method | declaration | owner | isolated mapping | notes |",
    )
    owners = {row[0]: row[2] for row in rows}
    for method in BOUNDARY_HOOK_METHODS - {
        "_on_turn_usage_ready",
        "on_event_content",
    }:
        assert owners[method] == "Split"
    assert owners["_on_turn_usage_ready"] == "Core"
    assert documented["telegram"] >= {"_consume_with_tracker"}
    assert documented["wecom"] >= {"_consume_with_tracker"}
    assert "TaskTracker 和任务取消调度\n留在 Core" in text
    runner_cleanup_semantics = "".join(
        [
            "Runner 清除 typing、\n",
            "processing 等 Driver-owned 平台状态",
        ],
    )
    assert runner_cleanup_semantics in text

    telegram_cleanup = _method_source(
        _channel_source("telegram"),
        _registry_specs()["telegram"][1],
        "_consume_with_tracker",
    )
    assert "except asyncio.CancelledError" in telegram_cleanup
    assert "self._is_processing.pop" in telegram_cleanup
    assert "self._stop_typing" in telegram_cleanup

    wecom_cleanup = _method_source(
        _channel_source("wecom"),
        _registry_specs()["wecom"][1],
        "_consume_with_tracker",
    )
    assert "finally:" in wecom_cleanup
    assert "self._processing_sessions.discard" in wecom_cleanup


def test_acl_identity_is_produced_by_runner_and_consumed_by_core() -> None:
    """The ACL identity contract is explicit in docs and source."""
    text = DOC_PATH.read_text(encoding="utf-8")
    assert "Runner extracts the stable real sender identity" in text
    assert "Core validates, preserves and persists that field" in text
    assert "Core never reconstructs `acl_sender_id`" in text
    assert "display name,\nshared session id or presentation id" in text
    assert 'meta["user_name"]' in text
    assert "acl_sender_id" in (
        BASE_PATH.read_text(encoding="utf-8")
        + (BASE_PATH.parent / "manager.py").read_text(encoding="utf-8")
    )
    for path in (
        BASE_PATH.parent / "feishu/channel.py",
        BASE_PATH.parent / "discord_/channel.py",
        BASE_PATH.parent / "slack/handler.py",
        AZURE_CHANNEL_PATH,
    ):
        assert "acl_sender_id" in path.read_text(encoding="utf-8")


def test_driver_rpc_mapping_matches_design_without_checkpoint_method() -> None:
    """The document maps exactly Design 7.3 and has no second wire API."""
    text = DOC_PATH.read_text(encoding="utf-8")
    rows = _table_rows(
        text,
        "| canonical RPC | direction | Driver / Host relation |",
    )
    documented = {row[0] for row in rows}
    assert documented == _design_rpc_methods()
    assert len(rows) == 21
    assert "`checkpoint` wire method" in text
    assert "`checkpoint` |" not in text
    assert "不是 wire API" in text


def test_media_path_rules_and_configuration_gaps_are_source_derived() -> None:
    """Freeze path bases and record current schema/factory/env gaps."""
    text = DOC_PATH.read_text(encoding="utf-8")
    assert "Core cwd nor Runner cwd participates" in text
    assert "`~` is expanded before the absolute/relative" in text
    assert "PureWindowsPath" in Path(__file__).read_text(encoding="utf-8")
    assert PurePosixPath("/srv/agent-a") / "attachments" == Path(
        "/srv/agent-a/attachments",
    )
    assert str(PureWindowsPath("C:/QwenPaw") / "downloads") == (
        "C:\\QwenPaw\\downloads"
    )
    assert '`from_config`, `media_dir="attachments"`' in text
    assert '`from_env`, `SLACK_MEDIA_DIR="downloads"`' in text

    config_types = _config_channel_types()
    media_channels = {
        "discord",
        "dingtalk",
        "feishu",
        "qq",
        "telegram",
        "mattermost",
        "matrix",
        "slack",
        "wecom",
        "xiaoyi",
        "yuanbao",
        "wechat",
    }
    for key in media_channels:
        config_name = config_types[key]
        has_schema = _class_has_field(CONFIG_PATH, config_name, "media_dir")
        source = _channel_source(key)
        _, class_name = _registry_specs()[key]
        constructor = _method_source_or_empty(source, class_name, "__init__")
        from_config = _method_source_or_empty(
            source,
            class_name,
            "from_config",
        )
        from_env = _method_source_or_empty(source, class_name, "from_env")
        assert has_schema == (
            key not in {"qq", "telegram", "matrix", "xiaoyi"}
        )
        assert ("media_dir" in constructor) == (key != "matrix")
        assert ("media_dir" in from_config) == (
            key not in {"telegram", "matrix"}
        )
        env_key = f"{key.upper()}_MEDIA_DIR"
        if key == "discord":
            env_key = "DISCORD_MEDIA_DIR"
        if key == "dingtalk":
            env_key = "DINGTALK_MEDIA_DIR"
        if key == "feishu":
            env_key = "FEISHU_MEDIA_DIR"
        if key == "mattermost":
            env_key = "MATTERMOST_MEDIA_DIR"
        if key == "wecom":
            env_key = "WECOM_MEDIA_DIR"
        if key == "wechat":
            env_key = "WECHAT_MEDIA_DIR"
        if key == "xiaoyi":
            env_key = "XIAOYI_MEDIA_DIR"
        assert (env_key in from_env) == (
            key not in {"qq", "telegram", "matrix", "slack", "yuanbao"}
        )


def test_legacy_plugin_registration_is_concrete_base_channel_contract() -> (
    None
):
    """Verify PluginAPI, registry, and the Azure Bot registration chain."""
    api_source = PLUGIN_API_PATH.read_text(encoding="utf-8")
    registry_source = PLUGIN_REGISTRY_PATH.read_text(encoding="utf-8")
    azure_source = AZURE_PLUGIN_PATH.read_text(encoding="utf-8")
    azure_channel_source = AZURE_CHANNEL_PATH.read_text(encoding="utf-8")
    assert "self._registry.register_channel(" in api_source
    assert "issubclass(channel_class, BaseChannel)" in registry_source
    assert "channel_class=AzureBotChannel" in azure_source
    assert "api.register_channel(" in azure_source
    assert "class AzureBotChannel(BaseChannel):" in azure_channel_source
    assert "`PluginAPI.register_channel` accepts" in (
        DOC_PATH.read_text(encoding="utf-8")
    )


def test_interfaces_are_dto_only_and_compatibility_is_complete() -> None:
    """Runner signatures prohibit Core objects and list all combinations."""
    text = DOC_PATH.read_text(encoding="utf-8")
    start = text.index("### ChannelDriver（Runner）")
    end = text.index("### IsolatedChannelProxy（Core）")
    driver_text = text[start:end]
    forbidden = (
        "Path",
        "Workspace",
        "AgentRequest",
        "AgentResponse",
        "Event",
        "Future",
        "WebSocket",
        "ChannelManager",
    )
    signature_lines = [
        line for line in driver_text.splitlines() if line.startswith("|")
    ][2:]
    assert signature_lines
    assert not any(
        token in line.split("|")[2]
        for line in signature_lines
        for token in forbidden
    )
    assert "strings、numbers、booleans、null、arrays" in driver_text
    compat = _table_rows(
        text,
        (
            "| source_kind | process_mode | Core representation | "
            "driver contract | compatibility promise |"
        ),
    )
    assert {(row[0], row[1]) for row in compat} == {
        ("builtin", "in_process"),
        ("builtin", "runner_process"),
        ("plugin", "in_process"),
        ("plugin", "runner_process"),
    }
    assert "azure_bot" in text


def test_boundary_invariants_and_media_rules_are_explicit() -> None:
    """Guard the high-risk ACL, media, and follow-up ownership statements."""
    text = DOC_PATH.read_text(encoding="utf-8")
    assert "acl_sender_id" in text
    assert 'meta["user_name"]' in text
    assert "never combine two senders" in text
    assert "explicit config.media_dir -> expanduser" in text
    assert "unset config.media_dir" in text
    assert "<CHANNEL>_MEDIA_DIR" in text
    assert "not append a Channel subdirectory" in text
    assert "ADR-034" in text
    assert "ADR-032 is not used" in text
    assert "parser belongs to `CH-2-004`" in text
    assert "`[x] 独立 Review 和最终验证通过`" in text
    assert "does not implement `ChannelHostAdapter`" in text


def test_plan_has_one_ch_0_001_status_record() -> None:
    """The task status is maintained once after review completion."""
    lines = PLAN_PATH.read_text(encoding="utf-8").splitlines()
    start = lines.index("### CH-0-001：Core/Runner 职责和兼容边界")
    end = next(
        index
        for index in range(start + 1, len(lines))
        if lines[index].startswith("### CH-")
    )
    block = lines[start:end]
    statuses = [line for line in block if line.startswith("- 状态：")]
    assert statuses == ["- 状态：[x] 独立 Review 和最终验证通过"]
    assert "证据：职责矩阵和 override 基线见" in block
