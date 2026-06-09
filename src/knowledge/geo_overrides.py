"""
Geo override cleanup
====================
Single shared definition for cleaning up vendor-sourced city/state hints that
are about to be promoted into a UTSF zone's `pinOverrides` array (used when a
pincode the vendor claims to serve is absent from our master `pincodes.json`
reference snapshot — see ZoneMapper.build_serviceability).

Mirrors the "one definition governs every merge point" pattern established by
knowledge/charge_richness.py: normalisation policy lives here ONCE, so parsers
stay faithful/raw (they hand over whatever the source said) and every promotion
site (currently just build_serviceability, potentially more later) applies the
exact same cleanup — no drift between layers, no per-call duplication of an
alias table.
"""

from typing import Optional, Tuple

# Small alias table for known spelling drift between vendor docs and our
# canonical state names. Extend as new variants are observed — this is the
# ONE place that needs updating.
_STATE_ALIASES = {
    "tamilnadu": "Tamil Nadu",
}

# Placeholder values that mean "no real geo data" — must be excluded rather
# than carried forward as if they were genuine vendor-supplied facts.
_PLACEHOLDERS = {"unknown", "n/a", "na", "-", "", "none", "null", "tbd"}


def _clean_name(raw) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s.lower() in _PLACEHOLDERS:
        return None
    return s


def normalize_state_name(raw) -> Optional[str]:
    """Trim/title-case a state name and resolve known spelling aliases.
    Returns None for blanks/placeholders."""
    s = _clean_name(raw)
    if s is None:
        return None
    titled = s.title()
    return _STATE_ALIASES.get(titled.lower().replace(" ", ""), titled)


def normalize_city_name(raw) -> Optional[str]:
    """Trim/title-case a city name. Returns None for blanks/placeholders."""
    s = _clean_name(raw)
    if s is None:
        return None
    return s.title()


def clean_geo_hint(city, state) -> Optional[Tuple[str, str]]:
    """
    Single gate a vendor-sourced (city, state) pair must pass before it can be
    recorded in pinOverrides. Both must survive normalisation — a hint missing
    either half (e.g. the "Unknown"/"Unknown" placeholder rows seen in real
    vendor sheets) is not usable geo data and must be dropped, not guessed at.

    Returns (city, state) cleaned, or None if the hint isn't usable.
    """
    c = normalize_city_name(city)
    s = normalize_state_name(state)
    if c is None or s is None:
        return None
    return (c, s)
