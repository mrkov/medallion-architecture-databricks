import pytest

from medallion_poc.transforms import ProductHierarchy, split_product_hierarchy


def test_splits_and_trims_four_part_hierarchy():
    assert split_product_hierarchy(" PHOTOBOOK | PREMIUM | A4 | LANDSCAPE ") == ProductHierarchy(
        product_family="PHOTOBOOK",
        product_tier="PREMIUM",
        product_format="A4",
        product_orientation="LANDSCAPE",
    )


@pytest.mark.parametrize(
    ("raw", "code"),
    [
        (None, "HIERARCHY_REQUIRED"),
        ("PHOTOBOOK|PREMIUM|A4", "HIERARCHY_SEGMENT_COUNT"),
        ("PHOTOBOOK|PREMIUM|A4|LANDSCAPE|EXTRA", "HIERARCHY_SEGMENT_COUNT"),
        ("PHOTOBOOK||A4|LANDSCAPE", "HIERARCHY_EMPTY_SEGMENT"),
    ],
)
def test_rejects_malformed_hierarchy(raw, code):
    with pytest.raises(ValueError, match=code):
        split_product_hierarchy(raw)

