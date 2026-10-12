"""Pure transformation helpers mirrored by the Spark pipeline."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ProductHierarchy:
    product_family: str
    product_tier: str
    product_format: str
    product_orientation: str


def split_product_hierarchy(raw: str | None) -> ProductHierarchy:
    if raw is None:
        raise ValueError("HIERARCHY_REQUIRED")
    parts = [part.strip() for part in raw.split("|")]
    if len(parts) != 4:
        raise ValueError("HIERARCHY_SEGMENT_COUNT")
    if any(not part for part in parts):
        raise ValueError("HIERARCHY_EMPTY_SEGMENT")
    return ProductHierarchy(*parts)
