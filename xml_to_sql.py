#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Parse Oracle-style XML SQL mapping files and export each statement to its own .sql file.

Rules:
- Look for tags: <sql>, <select>, <insert>, <update>, <delete>
- Each tag must have an "id" attribute; use it as the output filename: {id}.sql
- Save the *inner contents* of the tag (everything between start/end tags) to that .sql file.
- Unescape XML entities like &lt; &gt; &amp; so the SQL is readable.

Usage:
  python export_sql_from_xml.py /path/to/oracle_xml_folder /path/to/output_folder
"""

from __future__ import annotations

import os
import re
import sys
import html
from pathlib import Path
from typing import Iterable, Tuple
import xml.etree.ElementTree as ET


TARGET_TAGS = {"sql", "select", "insert", "update", "delete"}


def strip_namespace(tag: str) -> str:
    """Convert '{namespace}tag' -> 'tag'"""
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def sanitize_filename(name: str) -> str:
    """
    Make a safe filename across OSes.
    - Remove/replace characters that are problematic on Windows/macOS/Linux.
    - Trim whitespace/dots.
    """
    name = name.strip()
    # Replace path separators and other illegal filename chars
    name = re.sub(r'[\\/:*?"<>|]', "_", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name)
    # Avoid trailing dots/spaces (Windows)
    name = name.strip(" .")
    if not name:
        name = "unnamed"
    return name


def unique_path(base_path: Path) -> Path:
    """
    If base_path exists, create base_path stem + _{n}.sql
    """
    if not base_path.exists():
        return base_path
    stem = base_path.stem
    suffix = base_path.suffix
    parent = base_path.parent
    n = 2
    while True:
        cand = parent / f"{stem}_{n}{suffix}"
        if not cand.exists():
            return cand
        n += 1


def inner_xml_to_text(elem: ET.Element, encoding: str = "unicode") -> str:
    """
    Get the inner content of an element, preserving child markup if present.
    Then unescape XML/HTML entities so &lt;= becomes <= etc.
    """
    parts = []

    # Leading text inside the element
    if elem.text:
        parts.append(elem.text)

    # Any children + their tails
    for child in list(elem):
        parts.append(ET.tostring(child, encoding=encoding, method="xml"))
        if child.tail:
            parts.append(child.tail)

    raw = "".join(parts)

    # Unescape entities (&lt;, &gt;, &amp;, etc.)
    # Many mapping files escape < and > inside SQL.
    unescaped = html.unescape(raw)

    # Normalize newlines a bit (optional)
    unescaped = unescaped.replace("\r\n", "\n").replace("\r", "\n")

    # Trim only one leading/trailing blank line noise, but keep SQL formatting
    # (You can remove these two lines if you want exact whitespace)
    unescaped = unescaped.strip("\n") + "\n"

    return unescaped


def iter_sql_nodes(tree_root: ET.Element) -> Iterable[Tuple[str, ET.Element]]:
    """
    Yield (tagname, element) for target tags regardless of namespaces.
    """
    for elem in tree_root.iter():
        tag = strip_namespace(elem.tag)
        if tag in TARGET_TAGS:
            yield tag, elem


def export_from_xml_file(xml_path: Path, out_dir: Path) -> int:
    """
    Export all eligible nodes from a single XML file.
    Returns number of exported SQL files.
    """
    try:
        tree = ET.parse(xml_path)
    except ET.ParseError as e:
        print(f"[WARN] Skipping (XML parse error): {xml_path} ({e})")
        return 0

    root = tree.getroot()
    count = 0

    for tag, elem in iter_sql_nodes(root):
        sql_id = elem.get("id")
        if not sql_id:
            # If you want to support other attribute names (e.g., "name"), add here
            continue

        filename = sanitize_filename(sql_id) + ".sql"
        out_path = unique_path(out_dir / filename)

        content = inner_xml_to_text(elem)

        # Optionally, add a tiny header comment showing origin
        header = f"-- source: {xml_path.name}  tag: <{tag} id=\"{sql_id}\">\n"
        final = header + content

        out_path.write_text(final, encoding="utf-8")
        count += 1

    return count


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        print("Usage: python export_sql_from_xml.py <oracle_xml_folder> [output_folder]")
        return 2

    src_dir = Path(argv[1]).expanduser().resolve()
    out_dir = Path(argv[2]).expanduser().resolve() if len(argv) >= 3 else (src_dir / "_exported_sql")

    if not src_dir.exists() or not src_dir.is_dir():
        print(f"[ERROR] Not a folder: {src_dir}")
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)

    xml_files = sorted(src_dir.glob("*.xml"))
    if not xml_files:
        print(f"[WARN] No .xml files found in {src_dir}")
        return 0

    total = 0
    for xml_path in xml_files:
        exported = export_from_xml_file(xml_path, out_dir)
        print(f"[OK] {xml_path.name}: exported {exported}")
        total += exported

    print(f"\nDone. Total exported: {total}")
    print(f"Output folder: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))