import pytest

from services import units


def test_parse_pound_lingo_to_grams():
    parsed = units.parse_quantity_unit("50#")
    assert parsed["canonical_unit"] == "g"
    assert parsed["canonical_value"] == pytest.approx(22679.6185, rel=1e-6)
    assert parsed["display_original"] == "50#"


def test_parse_case_lingo_to_each():
    parsed = units.parse_quantity_unit("2 cs")
    assert parsed["canonical_unit"] == "each"
    assert parsed["canonical_value"] == pytest.approx(2.0)
    assert parsed["display_original"] == "2 cs"


def test_parse_fl_oz_to_ml():
    parsed = units.parse_quantity_unit("4 fl oz")
    assert parsed["canonical_unit"] == "ml"
    assert parsed["canonical_value"] == pytest.approx(118.29411825, rel=1e-6)


def test_reject_missing_unit():
    with pytest.raises(units.UnitNormalizationError):
        units.parse_quantity_unit("50")
