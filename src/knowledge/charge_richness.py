"""
Shared charge-value scoring helpers
===================================
Used by both the cross-file merge step (main.py) and the cross-table merge
step inside ExcelParser._auto_detect to pick the *richer* of two conflicting
charge-field extractions instead of blindly overwriting (last-wins).

Kept here (rather than duplicated) so a single richness definition governs
every merge point in the pipeline — e.g. so a `{type: distance_weight_matrix,
matrix: [...]}` always outranks a flat `{v, f}` scalar, regardless of which
layer is doing the merging.
"""


def charge_num(x):
    """Coerce numeric-looking values robustly: real numbers, numeric strings
    ("20", "20.5"), and percent strings ("20%") all count as real data —
    parsers occasionally hand back strings instead of floats. Returns 0.0
    for anything non-numeric, NaN, or +/-Infinity.
    """
    if isinstance(x, bool):
        return 0.0
    if isinstance(x, (int, float)):
        f = float(x)
    elif isinstance(x, str):
        try:
            f = float(x.strip().rstrip("%").replace(",", ""))
        except ValueError:
            return 0.0
    else:
        return 0.0
    if f != f or f in (float("inf"), float("-inf")):  # NaN / Inf guard
        return 0.0
    return f


def charge_richness(value) -> int:
    """Score how much information a charge-field candidate carries.

    Higher = richer/more specific. Used to pick the better of two
    conflicting extractions instead of blindly overwriting (last-file-wins
    or dict-wins-over-scalar), mirroring the pair-count comparison already
    used for zone_matrix.
    """
    _num = charge_num

    if value is None:
        return 0
    if isinstance(value, bool):
        return 0
    if isinstance(value, (int, float, str)):
        return 1 if _num(value) else 0
    if isinstance(value, dict):
        if not value:
            return 0
        # Complex shapes (rate tables/slabs) carry many entries — richest
        for complex_key in ("distance_weight_matrix", "weight_band", "matrix",
                            "bands", "rules", "slabs"):
            inner = value.get(complex_key)
            if isinstance(inner, (list, dict)) and inner:
                return 10 + len(inner)
        # {v, f}/{value, fixed, variable} style — richer when more sub-fields populated
        nonzero = sum(1 for k in ("v", "f", "value", "variable", "fixed")
                      if _num(value.get(k)))
        if nonzero:
            return 1 + nonzero
        # Unknown dict shape with content (e.g. {"basis": "per_shipment"}) still
        # carries *some* information — rank it above nothing, below numeric data.
        return 1 if any(_num(v) or (isinstance(v, str) and v.strip()) for v in value.values()) else 0
    if isinstance(value, list):
        return 10 + len(value) if value else 0
    return 0
