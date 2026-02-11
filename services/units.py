import re
from typing import Dict, Optional

from services import lexicon

MASS_FACTORS_TO_G: Dict[str, float] = {
    "mg": 0.001,
    "g": 1.0,
    "kg": 1000.0,
    "oz": 28.349523125,
    "lb": 453.59237,
}

VOLUME_FACTORS_TO_ML: Dict[str, float] = {
    "ml": 1.0,
    "l": 1000.0,
    "fl oz": 29.5735295625,
    "quart": 946.352946,
    "pint": 473.176473,
    "gallon": 3785.411784,
}

COUNT_UNITS = {"case", "each"}

UNIT_ALIASES: Dict[str, str] = {
    "#": "lb",
    "lb": "lb",
    "lbs": "lb",
    "pound": "lb",
    "pounds": "lb",
    "mg": "mg",
    "g": "g",
    "kg": "kg",
    "oz": "oz",
    "gram": "g",
    "grams": "g",
    "kilogram": "kg",
    "kilograms": "kg",
    "milligram": "mg",
    "milligrams": "mg",
    "ml": "ml",
    "l": "l",
    "liter": "l",
    "liters": "l",
    "litre": "l",
    "litres": "l",
    "fl oz": "fl oz",
    "quart": "quart",
    "qt": "quart",
    "qts": "quart",
    "pint": "pint",
    "pt": "pint",
    "pts": "pint",
    "gallon": "gallon",
    "gal": "gallon",
    "gals": "gallon",
    "milliliter": "ml",
    "milliliters": "ml",
    "millilitre": "ml",
    "millilitres": "ml",
    "fl oz": "fl oz",
    "floz": "fl oz",
    "oz fl": "fl oz",
    "fluid ounce": "fl oz",
    "fluid ounces": "fl oz",
    "case": "case",
    "cases": "case",
    "cs": "case",
    "ea": "each",
    "each": "each",
    "pcs": "each",
    "pc": "each",
}

QUANTITY_RE = re.compile(
    r"^\s*(?P<qty>-?\d+(?:\.\d+)?)\s*(?P<unit>fl\s*oz|oz\s*fl|#|lbs?|lb|kg|g|mg|ml|l|qt|quart|pt|pint|gal|gallon|cs|case|cases|ea|each|pcs?|floz)?\s*$",
    re.IGNORECASE,
)


class UnitNormalizationError(ValueError):
    pass


def _coerce_float(value: object) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        raise UnitNormalizationError("Quantity must be numeric.")
    if out <= 0:
        raise UnitNormalizationError("Quantity must be greater than zero.")
    return out


def normalize_unit_token(unit: str, restaurant_tag: Optional[str] = None) -> str:
    raw = " ".join(str(unit or "").strip().lower().split())
    if not raw:
        raise UnitNormalizationError("Unit is required.")

    alias_resolved = lexicon.resolve_alias(raw, restaurant_tag=restaurant_tag)
    candidate = " ".join(str(alias_resolved).strip().lower().split())

    if candidate in UNIT_ALIASES:
        return UNIT_ALIASES[candidate]

    # fallback for exact singular/plural patterns not in alias map
    if candidate.endswith("s") and candidate[:-1] in UNIT_ALIASES:
        return UNIT_ALIASES[candidate[:-1]]

    raise UnitNormalizationError(f"Unsupported unit '{unit}'.")


def normalize_quantity(
    quantity: object,
    unit: str,
    *,
    display_original: Optional[str] = None,
    restaurant_tag: Optional[str] = None,
) -> Dict[str, object]:
    qty = _coerce_float(quantity)
    normalized_unit = normalize_unit_token(unit, restaurant_tag=restaurant_tag)

    canonical_value: float
    canonical_unit: str

    if normalized_unit in MASS_FACTORS_TO_G:
        canonical_value = qty * MASS_FACTORS_TO_G[normalized_unit]
        canonical_unit = "g"
    elif normalized_unit in VOLUME_FACTORS_TO_ML:
        canonical_value = qty * VOLUME_FACTORS_TO_ML[normalized_unit]
        canonical_unit = "ml"
    elif normalized_unit in COUNT_UNITS:
        canonical_value = qty
        canonical_unit = "each"
    else:
        raise UnitNormalizationError(f"Unsupported unit '{unit}'.")

    original = (display_original or f"{qty:g} {unit}").strip()
    pretty = f"{canonical_value:,.3f}".rstrip("0").rstrip(".")
    display_pretty = f"{pretty} {canonical_unit} ({original})"

    return {
        "canonical_value": round(float(canonical_value), 6),
        "canonical_unit": canonical_unit,
        "display_original": original,
        "display_pretty": display_pretty,
        "normalized_unit": normalized_unit,
        "input_quantity": qty,
    }


def parse_quantity_unit(text: str, restaurant_tag: Optional[str] = None) -> Dict[str, object]:
    raw = " ".join(str(text or "").split()).strip()
    if not raw:
        raise UnitNormalizationError("Quantity text is empty.")

    alias_text = lexicon.replace_aliases_in_text(raw, restaurant_tag=restaurant_tag)
    match = QUANTITY_RE.match(alias_text)
    if not match:
        raise UnitNormalizationError("Could not parse quantity and unit.")

    qty = match.group("qty")
    unit = match.group("unit")
    if unit is None:
        raise UnitNormalizationError("Unit is required.")

    return normalize_quantity(
        qty,
        unit,
        display_original=raw,
        restaurant_tag=restaurant_tag,
    )
