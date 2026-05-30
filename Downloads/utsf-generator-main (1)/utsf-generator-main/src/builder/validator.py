"""
FC4 Validator
=============
Validates FC4 UTSF files for completeness, consistency, and pricing sanity.
"""

from typing import Dict, List, Tuple
from fc4_schema import ALL_ZONES, REGIONS, validate_fc4, MODE_NORMALIZE


def full_validate(utsf: Dict) -> Tuple[bool, List[str], List[str]]:
    """
    Full validation of FC4 UTSF.
    Returns: (is_valid, errors, warnings)
    """
    errors = validate_fc4(utsf)
    warnings = []

    _check_pricing_sanity(utsf, errors, warnings)
    _check_serviceability_consistency(utsf, errors, warnings)
    _check_zone_matrix_completeness(utsf, warnings)
    _check_company_completeness(utsf, warnings)

    is_valid = len(errors) == 0
    return is_valid, errors, warnings


def _check_pricing_sanity(utsf: Dict, errors: List, warnings: List):
    pricing = utsf.get("pricing", {})
    pr = pricing.get("priceRate", {})
    zm = pricing.get("zoneRates", {})

    # Zone rates should be positive
    for orig, dests in zm.items():
        for dest, rate in dests.items():
            if rate <= 0:
                errors.append(f"zoneRates[{orig}][{dest}]: rate {rate} is non-positive")
            elif rate > 500:
                warnings.append(f"zoneRates[{orig}][{dest}]: rate {rate} seems very high (>500/kg)")
            elif rate < 1:
                warnings.append(f"zoneRates[{orig}][{dest}]: rate {rate} seems very low (<1/kg)")

    # Min charges sanity
    min_charges = pr.get("minCharges", 0)
    if min_charges > 5000:
        warnings.append(f"priceRate.minCharges={min_charges} is unusually high")

    # Fuel % sanity (v2.1: scalar)
    fuel = pr.get("fuel", 0)
    if isinstance(fuel, (int, float)) and fuel > 50:
        warnings.append(f"fuel={fuel}% seems high (>50%)")

    # ODA sanity (v2.1: priceRate.odaCharges)
    oda = pr.get("odaCharges", {})
    if isinstance(oda, dict) and oda:
        oda_type = oda.get("type")
        if oda_type == "per_kg_minimum":
            if not oda.get("v") and not oda.get("f"):
                errors.append("odaCharges is per_kg_minimum but has no v or f values")
        elif oda_type == "weight_band":
            if not oda.get("bands"):
                errors.append("odaCharges is weight_band but has no bands")
        elif oda_type == "distance_weight_matrix":
            if not oda.get("matrix"):
                errors.append("odaCharges is distance_weight_matrix but has no matrix")


def _check_serviceability_consistency(utsf: Dict, errors: List, warnings: List):
    svc = utsf.get("serviceability", {})
    if not svc:
        warnings.append("serviceability is empty — no zones defined")
        return

    active_zones = [z for z, d in svc.items() if d.get("mode") != "NOT_SERVED"]
    if len(active_zones) == 0:
        warnings.append("All zones are NOT_SERVED — transporter serves no pincodes")

    for zone, data in svc.items():
        mode = MODE_NORMALIZE.get(data.get("mode", "NOT_SERVED"), "NOT_SERVED")
        if mode == "FULL_MINUS_EXCEPT":
            if not data.get("exceptRanges") and not data.get("exceptSingles"):
                # FULL_MINUS_EXCEPT with no exceptions = FULL_ZONE
                warnings.append(
                    f"{zone}: mode=FULL_MINUS_EXCEPT but no excepted pincodes — should be FULL_ZONE"
                )
        elif mode == "ONLY_SERVED":
            if not data.get("servedRanges") and not data.get("servedSingles"):
                warnings.append(
                    f"{zone}: mode=ONLY_SERVED but no served pincodes — effectively NOT_SERVED"
                )


def _check_zone_matrix_completeness(utsf: Dict, warnings: List):
    pricing = utsf.get("pricing", {})
    zm = pricing.get("zoneRates", {})
    svc = utsf.get("serviceability", {})

    active_zones = [z for z, d in svc.items() if d.get("mode") != "NOT_SERVED"]

    # All active origin zones should have rates
    for zone in active_zones:
        if zone not in zm:
            warnings.append(f"Zone {zone} is served but has no origin rates in zoneRates")
        else:
            for dest_zone in active_zones:
                if dest_zone not in zm.get(zone, {}):
                    warnings.append(
                        f"zoneRates missing [{zone}][{dest_zone}] — both zones are active"
                    )


def _check_company_completeness(utsf: Dict, warnings: List):
    m = utsf.get("meta", utsf.get("company", {}))
    if not m.get("companyName") and not m.get("name"):
        warnings.append("meta.companyName is missing")
    if not m.get("gstNo"):
        warnings.append("meta.gstNo not set — required for invoicing")
    if not m.get("contactPhone") and not m.get("contactEmail"):
        warnings.append("No contact info (contactPhone or contactEmail) for transporter")


def print_validation_report(utsf: Dict):
    """Pretty-print a validation report to console."""
    meta = utsf.get("meta", utsf.get("company", {}))
    name = meta.get("companyName") or meta.get("name") or "Unknown"
    valid, errors, warnings = full_validate(utsf)

    print(f"\n{'='*60}")
    print(f"Validation Report: {name}")
    print(f"{'='*60}")
    ok  = "[OK]"
    bad = "[!!]"
    warn = "[?]"
    print(f"Status: {ok + ' VALID' if valid else bad + ' INVALID'}")
    print(f"Data Quality: {utsf.get('dataQuality', 0):.1f}/100")
    stats = utsf.get("stats", {})
    print(f"Coverage: {stats.get('totalServedPincodes', 0):,} pincodes, "
          f"{stats.get('zonesServed', 0)} zones")

    if errors:
        print(f"\nErrors ({len(errors)}):")
        for e in errors:
            print(f"  {bad} {e}")

    if warnings:
        print(f"\nWarnings ({len(warnings)}):")
        for w in warnings:
            print(f"  {warn} {w}")

    if not errors and not warnings:
        print("\n[OK] No issues found")

    print(f"{'='*60}\n")
    return valid
