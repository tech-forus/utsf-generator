"""
UTSF Encoder (v2.1)
===================
Converts extracted transporter data into the locked UTSF v2.1 format.
Output is fully compatible with utsfService.js and utsfDecoder.ts.

LOCKED FORMAT — do not change field names.

Input (raw_data dict from parsers):
  company_details   {name, gstNo, phone, email, ...}
  charges           {fuel, docketCharges, rovCharges, odaCharges, ...}
  zone_matrix       {originZone: {destZone: ratePerKg}}
  served_pincodes   [int, ...]   (flat list)  OR
  zone_pincodes     {zone: [int, ...]}         (preferred)
  oda_pincodes      [int, ...]
  serviceability    pre-built dict (skips pincode processing)

Output (UTSF v2.1):
  version, generatedAt, sourceFormat, meta, pricing, serviceability, oda, stats
"""

import json
import copy
import os
from datetime import datetime
from typing import Dict, List, Optional, Any

from fc4_schema import (
    UTSF_EMPTY_TEMPLATE, ALL_ZONES, MODE_NORMALIZE,
    compress_to_ranges, calculate_data_quality, validate_utsf,
    empty_zone_entry, empty_priceRate,
    MODE_FULL_ZONE, MODE_FULL_MINUS_EXCEPT, MODE_ONLY_SERVED, MODE_NOT_SERVED,
)
from builder.zone_mapper import ZoneMapper


# ─── Value helpers ────────────────────────────────────────────────────────────

def _scalar(val, default: float = 0.0) -> float:
    """Convert a value (scalar, dict, or None) to float."""
    if val is None:
        return default
    if isinstance(val, dict):
        # Try common keys in priority order
        for key in ("value", "v", "variable", "f", "fixed", "charge"):
            v = val.get(key)
            if v is not None:
                try:
                    return float(v)
                except (ValueError, TypeError):
                    pass
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _vf(val, v_default: float = 0.0, f_default: float = 0.0) -> Dict:
    """
    Extract {v, f} from a charge value.
    Handles: dict with v/variable/f/fixed keys, scalar (small=%, large=fixed).
    Returns canonical {v: float, f: float}.
    """
    if isinstance(val, dict):
        v = float(val.get("v") if val.get("v") is not None else val.get("variable") or v_default)
        f = float(val.get("f") if val.get("f") is not None else val.get("fixed") or f_default)
        return {"v": v, "f": f}
    if isinstance(val, (int, float)):
        n = float(val)
        if n <= 100:
            return {"v": n, "f": f_default}
        else:
            return {"v": v_default, "f": n}
    return {"v": v_default, "f": f_default}


# ─── Encoder class ────────────────────────────────────────────────────────────

class FC4Encoder:
    """
    Encodes raw transporter data into UTSF v2.1 format.
    Class name kept for backward compatibility.
    """

    def __init__(self, pincodes_path: str, zones_data_path: str = None):
        self.zone_mapper = ZoneMapper(pincodes_path, zones_data_path)

    def encode(
        self,
        raw_data: Dict,
        source_files: List[str] = None,
        transporter_id: str = None
    ) -> Dict:
        """
        Main entry point. Converts raw_data -> UTSF v2.1 dict.
        """
        print(f"[Encoder] Starting UTSF v2.1 encode (id={transporter_id})")

        utsf = copy.deepcopy(UTSF_EMPTY_TEMPLATE)
        now = datetime.utcnow().isoformat() + "Z"

        utsf["generatedAt"] = now
        utsf["sourceFiles"] = source_files or []
        utsf["pricing"]["priceRate"] = empty_priceRate()

        # ── Meta ──────────────────────────────────────────────────────────────
        print(f"[Encoder] Encoding meta...")
        self._encode_meta(utsf, raw_data, transporter_id, now)
        m = utsf["meta"]
        print(f"[Encoder]   companyName={m.get('companyName')}  "
              f"gstNo={m.get('gstNo')}  mode={m.get('transportMode')}")

        # ── Pricing ───────────────────────────────────────────────────────────
        print(f"[Encoder] Encoding pricing...")
        self._encode_pricing(utsf, raw_data)
        pr = utsf["pricing"]["priceRate"]
        zr = utsf["pricing"]["zoneRates"]
        print(f"[Encoder]   zoneRates: {len(zr)} origin zones")
        print(f"[Encoder]   fuel={pr.get('fuel',0)}%  "
              f"docketCharges={pr.get('docketCharges',0)}  "
              f"minCharges={pr.get('minCharges',0)}  "
              f"kFactor={pr.get('kFactor',5000)}")

        # ── Serviceability ────────────────────────────────────────────────────
        print(f"[Encoder] Encoding serviceability...")
        self._encode_serviceability(utsf, raw_data)
        active = [z for z, d in utsf["serviceability"].items()
                  if d.get("mode") != MODE_NOT_SERVED]
        print(f"[Encoder]   {len(active)} active zones: {sorted(active)}")

        # ── ODA block ─────────────────────────────────────────────────────────
        utsf["oda"] = self.zone_mapper.build_oda_block(utsf["serviceability"])
        total_oda = sum(d.get("odaCount", 0) for d in utsf["oda"].values())
        if total_oda:
            print(f"[Encoder]   ODA block: {total_oda:,} pincodes in {len(utsf['oda'])} zones")

        # ── Stats ─────────────────────────────────────────────────────────────
        print(f"[Encoder] Computing stats...")
        stats = self.zone_mapper.compute_stats(utsf["serviceability"])
        # Map FC4 stat keys → v2.1 stat keys
        utsf["stats"]["totalPincodes"]     = stats.get("totalServedPincodes", 0)
        utsf["stats"]["totalOdaPincodes"]  = stats.get("totalOdaPincodes", 0)
        utsf["stats"]["zonesServed"]       = stats.get("zonesServed", 0)
        utsf["stats"]["activeZones"]       = stats.get("activeZones", [])
        utsf["stats"]["avgCoveragePercent"]= stats.get("avgCoveragePercent", 0.0)
        utsf["stats"]["coverageByRegion"]  = stats.get("coverageByRegion", {})
        print(f"[Encoder]   totalPincodes={utsf['stats']['totalPincodes']:,}  "
              f"zonesServed={utsf['stats']['zonesServed']}  "
              f"totalOdaPincodes={utsf['stats']['totalOdaPincodes']:,}")

        # ── Data quality ──────────────────────────────────────────────────────
        quality = calculate_data_quality(utsf)
        utsf["dataQuality"] = quality
        utsf["stats"]["dataQuality"]["overall"] = quality
        missing = self._find_missing_fields(utsf)
        utsf["stats"]["dataQuality"]["missingFields"] = missing
        print(f"[Encoder] Data quality: {quality:.1f}/100")
        if missing:
            print(f"[Encoder]   Missing fields: {missing}")

        # ── Validate ──────────────────────────────────────────────────────────
        errors = validate_utsf(utsf)
        if errors:
            print(f"[Encoder] Validation issues ({len(errors)}):")
            for e in errors:
                print(f"[Encoder]   {e}")
        else:
            print(f"[Encoder] Validation: OK")

        print(f"[Encoder] Encode complete.")
        return utsf

    # ─── Meta ─────────────────────────────────────────────────────────────────

    def _encode_meta(self, utsf: Dict, raw: Dict, tid: str, now: str):
        cd = raw.get("company_details") or raw.get("company") or {}
        m = utsf["meta"]

        m["id"] = tid or cd.get("id") or cd.get("_id") or cd.get("vendorCode")
        m["companyName"] = (
            cd.get("companyName") or cd.get("name") or cd.get("company_name")
        )
        m["shortName"] = cd.get("shortName") or cd.get("short_name")
        m["vendorCode"] = cd.get("vendorCode") or cd.get("vendor_code") or cd.get("code")
        m["customerID"] = cd.get("customerID") or cd.get("customer_id")
        m["transportMode"] = (
            cd.get("transportMode") or cd.get("transport_mode") or
            cd.get("serviceType") or cd.get("service_type") or "LTL"
        )
        m["transporterType"] = cd.get("transporterType") or (
            "temporary" if m["customerID"] else "regular"
        )
        m["gstNo"] = cd.get("gstNo") or cd.get("gst_no") or cd.get("gst")
        m["panNo"] = cd.get("panNo") or cd.get("pan_no") or cd.get("pan")
        m["website"] = cd.get("website")
        m["address"] = cd.get("address") or cd.get("contact", {}).get("address")
        m["city"] = cd.get("city") or cd.get("contact", {}).get("city")
        m["state"] = cd.get("state") or cd.get("contact", {}).get("state")
        m["pincode"] = str(cd.get("contact_pincode") or cd.get("pincode") or
                           cd.get("contact", {}).get("pincode") or "")
        m["contactPhone"] = (
            cd.get("phone") or cd.get("contact_phone") or
            cd.get("contact", {}).get("phone")
        )
        m["contactEmail"] = (
            cd.get("email") or cd.get("contact_email") or
            cd.get("contact", {}).get("email")
        )
        m["rating"] = float(cd.get("rating") or 4.0)
        m["isVerified"] = bool(cd.get("isVerified") or cd.get("verified"))
        m["chargesVerified"] = bool(cd.get("chargesVerified"))
        m["approvalStatus"] = (
            cd.get("approvalStatus") or cd.get("approval_status") or "pending"
        )
        m["createdAt"] = cd.get("createdAt") or now
        m["updatedAt"] = now

    # ─── Pricing ──────────────────────────────────────────────────────────────

    def _encode_pricing(self, utsf: Dict, raw: Dict):
        """
        Fill pricing.priceRate and pricing.zoneRates from raw data.

        Handles multiple input shapes:
          raw["charges"] = {fuel: 20, docketCharges: 50, rovCharges: {v:0.1, f:100}, ...}
          raw["charges"] = {priceRate: {...}}   (nested v2 shape)
          raw["zone_matrix"] / raw["zoneMatrix"] / raw["zoneRates"]
        """
        charges = raw.get("charges") or raw.get("pricing") or {}
        # Unwrap if charges is nested (v2 priceRate shape)
        if "priceRate" in charges:
            charges = charges["priceRate"]

        print(f"[Encoder._encode_pricing] charge keys: {list(charges.keys())}")

        p = utsf["pricing"]
        p["effectiveFrom"] = raw.get("effectiveFrom") or charges.get("effectiveFrom")
        p["effectiveTo"]   = raw.get("effectiveTo")   or charges.get("effectiveTo")
        p["rateVersion"]   = charges.get("rateVersion", "1.0")

        pr = p["priceRate"]

        # ── Scalars ────────────────────────────────────────────────────────────
        pr["minWeight"]    = _scalar(charges.get("minWeight"),    0.5)
        pr["minCharges"]   = _scalar(charges.get("minCharges"),   0.0)
        pr["docketCharges"]= _scalar(charges.get("docketCharges"), 0.0)
        pr["greenTax"]     = _scalar(charges.get("greenTax"),     0.0)
        pr["daccCharges"]  = _scalar(charges.get("daccCharges"),  0.0)
        pr["miscCharges"]  = _scalar(
            charges.get("miscCharges") or charges.get("ewayCharges"), 0.0
        )
        pr["dodCharges"]   = _scalar(charges.get("dodCharges") or charges.get("dod"), 0.0)

        # Divisor / kFactor
        div_raw = (charges.get("volumetricDivisor") or charges.get("divisor") or
                   charges.get("kFactor") or charges.get("cfactor") or 5000)
        div_val = _scalar(div_raw, 5000)
        pr["divisor"] = div_val
        pr["kFactor"] = div_val

        # ── Fuel ───────────────────────────────────────────────────────────────
        fuel_raw = (charges.get("fuel") or charges.get("fuelSurcharge") or
                    charges.get("fuel_percent") or 0)
        if isinstance(fuel_raw, dict):
            pr["fuel"] = float(fuel_raw.get("value") or fuel_raw.get("v") or 0)
        else:
            pr["fuel"] = _scalar(fuel_raw, 0.0)
        print(f"[Encoder._encode_pricing]   fuel={pr['fuel']}%  "
              f"docketCharges={pr['docketCharges']}  minCharges={pr['minCharges']}")

        # ── Variable+fixed charges ─────────────────────────────────────────────
        # rovCharges
        pr["rovCharges"] = self._encode_vf_charge(
            charges.get("rovCharges") or charges.get("rov"), "rovCharges"
        )

        # insuranceCharges
        pr["insuranceCharges"] = self._encode_vf_charge(
            charges.get("insuranceCharges") or charges.get("insurance"), "insuranceCharges"
        )

        # odaCharges — complex: handles all types
        pr["odaCharges"] = self._encode_oda_charges(
            charges.get("odaCharges") or charges.get("oda")
        )

        # handlingCharges — additive: fixed + weight * v%
        h = charges.get("handlingCharges") or charges.get("handling")
        if isinstance(h, dict) and h:
            hv = _vf(h)
            th = _scalar(h.get("thresholdWeight") or h.get("threshholdweight"), 0.0)
            pr["handlingCharges"] = {"v": hv["v"], "f": hv["f"]}
            if th > 0:
                pr["handlingCharges"]["thresholdWeight"] = th
        else:
            pr["handlingCharges"] = _vf(h)

        # fmCharges, appointmentCharges, codCharges, prepaidCharges, topayCharges
        for key, src_keys in [
            ("fmCharges",          ["fmCharges", "fm"]),
            ("appointmentCharges", ["appointmentCharges", "appointment"]),
            ("codCharges",         ["codCharges", "cod"]),
            ("prepaidCharges",     ["prepaidCharges", "prepaid"]),
            ("topayCharges",       ["topayCharges", "topay"]),
        ]:
            val = None
            for sk in src_keys:
                val = charges.get(sk)
                if val is not None:
                    break
            pr[key] = self._encode_vf_charge(val, key)

        # invoiceValueCharges
        inv = charges.get("invoiceValueCharges") or charges.get("invoiceValue")
        if isinstance(inv, dict) and (inv.get("enabled") or inv.get("percentage") or
                                      inv.get("slabs") or inv.get("v")):
            pr["invoiceValueCharges"] = {
                "enabled":       True,
                "percentage":    float(inv.get("percentage") or inv.get("v") or 0),
                "minimumAmount": float(inv.get("minimumAmount") or inv.get("f") or 0),
            }
            if inv.get("slabs"):
                pr["invoiceValueCharges"]["type"]  = "slabs"
                pr["invoiceValueCharges"]["slabs"] = inv["slabs"]
        else:
            pr["invoiceValueCharges"] = None

        # ── Log charge summary ─────────────────────────────────────────────────
        present = [k for k in ("fuel","rovCharges","insuranceCharges","odaCharges",
                                "handlingCharges","fmCharges","codCharges",
                                "docketCharges","minCharges")
                   if self._charge_has_value(pr.get(k))]
        missing = [k for k in ("fuel","rovCharges","odaCharges","docketCharges","minCharges")
                   if not self._charge_has_value(pr.get(k))]
        print(f"[Encoder._encode_pricing]   present: {present}")
        print(f"[Encoder._encode_pricing]   missing: {missing}")

        # ── Zone rates ─────────────────────────────────────────────────────────
        zm = (
            raw.get("zone_matrix") or raw.get("zoneMatrix") or
            raw.get("zoneRates") or
            charges.get("zoneRates") or charges.get("zoneMatrix") or {}
        )
        normalized_zr: Dict = {}
        for orig, dests in zm.items():
            o = orig.upper()
            if not isinstance(dests, dict):
                continue
            normalized_zr[o] = {
                d.upper(): float(v)
                for d, v in dests.items()
                if isinstance(v, (int, float))
            }
        p["zoneRates"] = normalized_zr

        if normalized_zr:
            sample = list(normalized_zr.items())[:2]
            for orig, dests in sample:
                print(f"[Encoder._encode_pricing]   zoneRates[{orig}]: "
                      f"{len(dests)} dest zones  sample={dict(list(dests.items())[:4])}")
            # Sanity check: detect "collapsed" matrices where cross-zone rates
            # are suspiciously equal to same-zone rates (sign of column alignment bug)
            self._validate_zone_matrix(normalized_zr)
        else:
            print(f"[Encoder._encode_pricing]   zoneRates: (empty)")

    def _validate_zone_matrix(self, zr: Dict):
        """
        Detect common zone matrix parsing errors:
        - Collapsed cross-zone rates (all origins have same rate to a destination)
        - Rate = 0 for all cross-zone pairs (missing data)
        - Cross-zone rate == same-zone rate (column misalignment sign)

        Logs warnings; does NOT modify the matrix.
        """
        all_zones = sorted(zr.keys())
        if len(all_zones) < 2:
            return

        collapsed_dests = []
        for dest in set(d for dests in zr.values() for d in dests):
            rates = [zr[o].get(dest) for o in all_zones if dest in zr.get(o, {})]
            rates = [r for r in rates if r is not None and r > 0]
            if len(rates) >= 3 and len(set(rates)) == 1:
                collapsed_dests.append((dest, rates[0]))

        if collapsed_dests:
            print(f"[Encoder] WARNING: Zone matrix may have collapsed rates "
                  f"(all origins have same rate to destination):")
            for dest, rate in collapsed_dests[:5]:
                print(f"[Encoder]   → {dest}: all origins = {rate} (possible column misalignment)")

        # Check if any origin has cross-zone == same-zone rates
        misaligned = []
        for orig in all_zones:
            same_zone_rate = zr[orig].get(orig)
            if same_zone_rate is None:
                continue
            cross_zone_matches = [
                dest for dest, rate in zr[orig].items()
                if dest != orig and rate == same_zone_rate
            ]
            if len(cross_zone_matches) >= 2:
                misaligned.append((orig, same_zone_rate, cross_zone_matches[:3]))

        if misaligned:
            print(f"[Encoder] WARNING: Multiple cross-zone rates equal same-zone rate "
                  f"(possible origin_col misalignment):")
            for orig, rate, matches in misaligned[:3]:
                print(f"[Encoder]   {orig}→{orig}={rate} same as {orig}→{matches}")

    def _encode_vf_charge(self, val, label: str = "") -> Dict:
        """
        Encode any charge value → {v: float, f: float}.
        Logs if non-zero.
        """
        result = _vf(val)
        if result["v"] != 0 or result["f"] != 0:
            print(f"[Encoder._encode_pricing]   {label}: v={result['v']}  f={result['f']}")
        return result

    def _encode_oda_charges(self, oda) -> Any:
        """
        Encode odaCharges to locked v2.1 format.

        Supported input types:
          None / 0                      → {v:0, f:0}
          scalar float                  → {v:x} if ≤100 else {f:x}
          {v, f}                        → legacy additive (f + weight * v%)
          {type:"per_kg_minimum", v, f} → max(v/100*weight, f)
          {type:"per_shipment", f}      → flat per ODA shipment
          {type:"weight_band", bands}   → step by weight
          {type:"distance_weight_matrix", matrix} → distance×weight table
        """
        if not oda:
            return {"v": 0.0, "f": 0.0}

        if isinstance(oda, (int, float)):
            n = float(oda)
            result = {"v": n, "f": 0.0} if n <= 100 else {"v": 0.0, "f": n}
            print(f"[Encoder._encode_pricing]   odaCharges: scalar → {result}")
            return result

        if not isinstance(oda, dict):
            return {"v": 0.0, "f": 0.0}

        oda_type = oda.get("type", "")

        # Distance × weight matrix (V Express style)
        if oda_type == "distance_weight_matrix" or oda.get("matrix"):
            matrix = oda.get("matrix") or []
            if matrix:
                # Normalize distance key names: minDist/maxDist → minKm/maxKm
                normalized_matrix = []
                for entry in matrix:
                    ne = {"bands": entry.get("bands", [])}
                    ne["minKm"] = entry.get("minKm") if entry.get("minKm") is not None else entry.get("minDist", 0)
                    if entry.get("maxKm") is not None:
                        ne["maxKm"] = entry["maxKm"]
                    elif entry.get("maxDist") is not None:
                        ne["maxKm"] = entry["maxDist"]
                    normalized_matrix.append(ne)
                result = {"type": "distance_weight_matrix", "matrix": normalized_matrix}
                # Derive minimum from smallest non-zero charge for display
                all_charges = [b.get("charge", 0) for e in normalized_matrix
                                for b in e.get("bands", []) if b.get("charge", 0) > 0]
                if all_charges:
                    result["minimum"] = min(all_charges)
                print(f"[Encoder._encode_pricing]   odaCharges: distance_weight_matrix "
                      f"({len(normalized_matrix)} dist bands)")
                return result

        # Weight band (DP World style)
        if oda_type == "weight_band" or oda_type == "weight_slab" or oda.get("bands"):
            bands = oda.get("bands") or []
            if bands:
                result = {"type": "weight_band", "bands": bands}
                first_min = bands[0].get("minimum") or bands[0].get("charge")
                if first_min:
                    result["minimum"] = float(first_min)
                print(f"[Encoder._encode_pricing]   odaCharges: weight_band "
                      f"({len(bands)} bands)")
                return result

        # Per shipment flat
        if oda_type == "per_shipment":
            f = _scalar(oda.get("f") or oda.get("fixed") or oda.get("charge"), 0.0)
            result = {"type": "per_shipment", "v": 0.0, "f": f}
            print(f"[Encoder._encode_pricing]   odaCharges: per_shipment f={f}")
            return result

        # per_kg_minimum: max(v%/100 * weight, f)
        if oda_type == "per_kg_minimum":
            v = _scalar(oda.get("v") or oda.get("variable") or oda.get("perKg"), 0.0)
            f = _scalar(oda.get("f") or oda.get("fixed") or oda.get("minimum"), 0.0)
            result = {"type": "per_kg_minimum", "v": v, "f": f}
            print(f"[Encoder._encode_pricing]   odaCharges: per_kg_minimum v={v} f={f}")
            return result

        # Default: treat as simple {v, f} (legacy additive)
        vf = _vf(oda)
        if vf["v"] != 0 or vf["f"] != 0:
            print(f"[Encoder._encode_pricing]   odaCharges: {vf}")
        return vf

    @staticmethod
    def _charge_has_value(charge) -> bool:
        """Return True if a charge has any non-zero value."""
        if charge is None:
            return False
        if isinstance(charge, (int, float)):
            return float(charge) != 0
        if isinstance(charge, dict):
            return bool(
                charge.get("type") or charge.get("bands") or charge.get("matrix") or
                charge.get("v", 0) != 0 or charge.get("f", 0) != 0
            )
        return False

    # ─── Serviceability ───────────────────────────────────────────────────────

    def _encode_serviceability(self, utsf: Dict, raw: Dict):
        """
        Build v2.1 serviceability from raw data.
        Supports:
          1. pre-built serviceability dict (v2 or FC4) → normalize
          2. zone_pincodes: {zone: [pincodes]}
          3. served_pincodes: flat list
        """
        existing_svc = raw.get("serviceability")
        if existing_svc and isinstance(existing_svc, dict) and existing_svc:
            print(f"[Encoder._encode_serviceability] Pre-built serviceability "
                  f"({len(existing_svc)} zones) — normalising field names")
            utsf["serviceability"] = self._normalize_serviceability(existing_svc)
            return

        oda_pincodes = list(map(int, raw.get("oda_pincodes", [])))
        print(f"[Encoder._encode_serviceability] ODA pincodes: {len(oda_pincodes):,}")

        # zone_pincodes (most precise)
        zone_pincodes = raw.get("zone_pincodes") or raw.get("zonePincodes")
        if zone_pincodes and isinstance(zone_pincodes, dict):
            total_zp = sum(len(v) for v in zone_pincodes.values())
            print(f"[Encoder._encode_serviceability] zone_pincodes path: "
                  f"{total_zp:,} pincodes across {len(zone_pincodes)} zones")
            overrides = self.zone_mapper.detect_transporter_zone_overrides(zone_pincodes)
            all_served = [int(p) for pins in zone_pincodes.values() for p in pins]
            utsf["serviceability"] = self.zone_mapper.build_serviceability(
                all_served, oda_pincodes, overrides
            )
            return

        # Flat served_pincodes
        served = raw.get("served_pincodes") or raw.get("servedPincodes") or []
        if served:
            served = list(map(int, served))
            print(f"[Encoder._encode_serviceability] flat served_pincodes: "
                  f"{len(served):,}")
            utsf["serviceability"] = self.zone_mapper.build_serviceability(
                served, oda_pincodes
            )
            return

        print("[Encoder._encode_serviceability] WARNING: no serviceability data")

    def _normalize_serviceability(self, svc: Dict) -> Dict:
        """
        Normalise incoming serviceability dict to v2.1 field names.
        Handles both v2 (exceptRanges) and FC4 (excludedRanges) inputs.
        """
        result: Dict = {}
        for zone, data in svc.items():
            if zone not in ALL_ZONES:
                continue
            entry = empty_zone_entry()
            raw_mode = data.get("mode", MODE_NOT_SERVED)
            mode = MODE_NORMALIZE.get(raw_mode, MODE_NOT_SERVED)
            entry["mode"] = mode

            if mode == MODE_FULL_MINUS_EXCEPT:
                # Accept both v2 and FC4 field names
                entry["exceptRanges"]  = (data.get("exceptRanges") or
                                          data.get("excludedRanges") or [])
                entry["exceptSingles"] = (data.get("exceptSingles") or
                                          data.get("excludedSingles") or [])
            elif mode == MODE_ONLY_SERVED:
                entry["servedRanges"]  = (data.get("servedRanges") or
                                          data.get("includedRanges") or [])
                entry["servedSingles"] = (data.get("servedSingles") or
                                          data.get("includedSingles") or [])

            # Cross-zone (FC4 feature preserved)
            entry["crossZoneRanges"]  = data.get("crossZoneRanges", [])
            entry["crossZoneSingles"] = data.get("crossZoneSingles", [])

            # ODA inline
            entry["odaRanges"]  = data.get("odaRanges", [])
            entry["odaSingles"] = data.get("odaSingles", [])
            entry["odaCount"]   = data.get("odaCount", 0)

            # Stats
            entry["totalInZone"]     = data.get("totalInZone", data.get("totalCount", 0))
            entry["servedCount"]     = data.get("servedCount", 0)
            entry["coveragePercent"] = data.get("coveragePercent", 0.0)

            result[zone] = entry
        return result

    # ─── Helpers ──────────────────────────────────────────────────────────────

    def _find_missing_fields(self, utsf: Dict) -> List[str]:
        missing = []
        m = utsf.get("meta", {})
        if not m.get("companyName"):    missing.append("meta.companyName")
        if not m.get("gstNo"):          missing.append("meta.gstNo")
        if not m.get("contactPhone"):   missing.append("meta.contactPhone")
        p = utsf.get("pricing", {})
        if not p.get("zoneRates"):      missing.append("pricing.zoneRates")
        if not utsf.get("serviceability"): missing.append("serviceability")
        return missing

    def save(self, utsf: Dict, output_path: str):
        """Save UTSF to JSON file."""
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(utsf, f, indent=2, ensure_ascii=False)
        size_kb = os.path.getsize(output_path) / 1024
        print(f"[Encoder] Saved: {output_path} ({size_kb:.1f} KB)")
        return output_path


# ─── Migration helper ─────────────────────────────────────────────────────────

def migrate_v2_to_fc4(
    v2_utsf: Dict,
    pincodes_path: str,
    zones_path: str = None
) -> Dict:
    """
    Re-encode any v2.x UTSF as v2.1.
    Preserves all data, normalizes structure.
    """
    encoder = FC4Encoder(pincodes_path, zones_path)
    meta = v2_utsf.get("meta", {})
    pricing = v2_utsf.get("pricing", {})
    pr = pricing.get("priceRate", {})

    raw: Dict = {
        "company_details": {
            "id":            meta.get("id"),
            "companyName":   meta.get("companyName"),
            "vendorCode":    meta.get("vendorCode"),
            "customerID":    meta.get("customerID"),
            "transportMode": meta.get("transportMode", "LTL"),
            "gstNo":         meta.get("gstNo"),
            "address":       meta.get("address"),
            "city":          meta.get("city"),
            "state":         meta.get("state"),
            "pincode":       meta.get("pincode"),
            "rating":        meta.get("rating", 4.0),
            "isVerified":    meta.get("isVerified", False),
            "approvalStatus": meta.get("approvalStatus", "pending"),
            "status":        "active",
            "createdAt":     meta.get("createdAt"),
            "updatedAt":     meta.get("updatedAt"),
        },
        "charges":       pr,
        "zone_matrix":   pricing.get("zoneRates", {}),
        "serviceability": v2_utsf.get("serviceability", {}),
    }

    # Merge ODA from separate v2 oda block into serviceability
    v2_oda = v2_utsf.get("oda", {})
    for zone, oda_data in v2_oda.items():
        if zone in raw["serviceability"]:
            raw["serviceability"][zone]["odaRanges"]  = oda_data.get("odaRanges", [])
            raw["serviceability"][zone]["odaSingles"] = oda_data.get("odaSingles", [])
            raw["serviceability"][zone]["odaCount"]   = oda_data.get("odaCount", 0)

    return encoder.encode(raw, source_files=["migration_v2"])
