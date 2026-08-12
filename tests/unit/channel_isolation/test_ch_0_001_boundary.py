# -*- coding: utf-8 -*-
"""Contract checks for the CH-0-001 Core/Runner boundary document."""

from __future__ import annotations

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
BASE_PATH = ROOT / "src/qwenpaw/app/channels/base.py"
REGISTRY_PATH = ROOT / "src/qwenpaw/app/channels/registry.py"
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
    """Voice and SIP are the only direct-session builtins."""
    text = DOC_PATH.read_text(encoding="utf-8")
    rows = _table_rows(
        text,
        "| channel_key | dispatch_mode | queue/ACL behavior |",
    )
    assert len(rows) == len(EXPECTED_CHANNELS)
    assert {row[0] for row in rows} == EXPECTED_CHANNELS
    assert {row[0]: row[1] for row in rows} == DISPATCH_MODES
    source = BASE_PATH.parent
    direct = {
        package
        for package in ("voice", "sip")
        if any(
            "uses_manager_queue = False" in path.read_text(encoding="utf-8")
            for path in (source / package).rglob("*.py")
        )
    }
    assert direct == {"voice", "sip"}


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
    assert "consists only of strings" in driver_text
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
    assert "never combines two senders" in text
    assert (
        'config.media_dir -> workspace_dir / "media" -> WORKING_DIR / "media"'
        in text
    )
    assert "<CHANNEL>_MEDIA_DIR" in text
    assert "not append a Channel subdirectory" in text
    assert "ADR-034" in text
    assert "ADR-032 is not used" in text
    assert "parser belongs to `CH-2-004`" in text
    assert "`[-] 等待独立 Review`" in text
    assert "does not implement `ChannelHostAdapter`" in text


def test_plan_has_one_ch_0_001_status_record() -> None:
    """The task status is maintained once and remains review-pending."""
    lines = PLAN_PATH.read_text(encoding="utf-8").splitlines()
    start = lines.index("### CH-0-001：Core/Runner 职责和兼容边界")
    end = next(
        index
        for index in range(start + 1, len(lines))
        if lines[index].startswith("### CH-")
    )
    block = lines[start:end]
    statuses = [line for line in block if line.startswith("- 状态：")]
    assert statuses == ["- 状态：[-] 等待独立 Review"]
