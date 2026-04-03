from __future__ import annotations

import xml.etree.ElementTree as ET
from collections.abc import Iterable


def local_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def iter_local_matches(root: ET.Element, tag_names: Iterable[str]) -> Iterable[ET.Element]:
    wanted = set(tag_names)
    for element in root.iter():
        if local_name(element.tag) in wanted:
            yield element


def find_first_text(root: ET.Element, *tag_names: str) -> str | None:
    for element in iter_local_matches(root, tag_names):
        text = (element.text or "").strip()
        if text:
            return text
    return None


def findall_text(root: ET.Element, *tag_names: str) -> list[str]:
    values: list[str] = []
    for element in iter_local_matches(root, tag_names):
        text = (element.text or "").strip()
        if text:
            values.append(text)
    return values


def find_child_text(parent: ET.Element, *tag_names: str) -> str | None:
    for child in parent:
        if local_name(child.tag) in tag_names:
            text = (child.text or "").strip()
            if text:
                return text
    return None


def as_int(value: str | None) -> int | None:
    if not value:
        return None
    digits = value.replace(",", "").strip()
    if digits in {"", "-", "--"}:
        return None
    try:
        return int(float(digits))
    except ValueError:
        return None


def as_float(value: str | None) -> float | None:
    if not value:
        return None
    digits = value.replace(",", "").strip()
    if digits in {"", "-", "--"}:
        return None
    try:
        return float(digits)
    except ValueError:
        return None
