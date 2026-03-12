# -*- coding: utf-8 -*-
"""WeCom channel utilities."""
from __future__ import annotations

import re
from typing import List


def format_markdown_tables(text: str) -> str:
    """Format GFM markdown tables for WeCom compatibility.

    WeCom requires table columns to be properly aligned.
    This function normalizes table formatting.

    Args:
        text: Input markdown text possibly containing tables.

    Returns:
        Text with formatted tables.
    """
    lines = text.split("\n")
    result: List[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect table start (line with |)
        if "|" in line and not line.strip().startswith("```"):
            # Collect table lines
            table_lines: List[str] = []
            while i < len(lines) and "|" in lines[i]:
                table_lines.append(lines[i])
                i += 1
            # Format and add table
            if table_lines:
                result.extend(_format_table(table_lines))
            continue
        result.append(line)
        i += 1
    return "\n".join(result)


def _format_table(lines: List[str]) -> List[str]:
    """Format a single markdown table."""
    if not lines:
        return lines

    # Parse cells
    rows: List[List[str]] = []
    for line in lines:
        cells = [c.strip() for c in line.split("|")]
        # Remove empty first/last cells from leading/trailing |
        if cells and not cells[0]:
            cells = cells[1:]
        if cells and not cells[-1]:
            cells = cells[:-1]
        if cells:
            rows.append(cells)

    if len(rows) < 2:
        return lines  # Not a valid table

    # Check if second row is separator (contains only -, :, |, spaces)
    sep_pattern = re.compile(r"^[\s\-:|]+$")
    has_separator = sep_pattern.match(lines[1]) is not None

    if not has_separator:
        return lines  # Not a valid GFM table

    # Calculate column widths
    col_count = max(len(r) for r in rows)
    widths: List[int] = [0] * col_count
    for row in rows:
        for j, cell in enumerate(row):
            widths[j] = max(widths[j], len(cell))

    # Format rows with proper padding
    formatted: List[str] = []
    for idx, row in enumerate(rows):
        padded = [cell.ljust(widths[j]) for j, cell in enumerate(row)]
        formatted.append("| " + " | ".join(padded) + " |")
        # Add separator after header if not present
        if idx == 0 and not has_separator:
            sep = "|" + "|".join("-" * (w + 2) for w in widths) + "|"
            formatted.append(sep)

    return formatted
