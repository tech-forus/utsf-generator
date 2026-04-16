"""
ML Dictionary Engine — Auto-Learning Correction System
=======================================================
Works like phone autocorrect: tracks every correction, builds confidence over
time, and automatically promotes high-frequency matches into the learned dict
so you never have to correct the same thing twice.

Flow:
  1. User sees a fuzzy-matched label in the audit panel.
  2. They confirm it (correct) or correct it (wrong).
  3. This engine records the outcome in learning_data.json.
  4. After AUTO_PROMOTE_THRESHOLD confirmations of the same (raw→canonical)
     pair, it auto-writes the entry into learned_dict.py permanently.
  5. Stats API lets the viewer show learning progress.
"""

import os
import json
import time
import re
from typing import Dict, List, Optional, Tuple

# Source location (used as fallback in dev mode)
_HERE = os.path.dirname(os.path.abspath(__file__))


def _get_writable_dir() -> str:
    """
    Return the writable directory for persistent files.

    In frozen EXE mode: UTSF_ROOT is set by launcher.py to the folder that
    contains the EXE (writable, persists between runs).  We store mutable data
    in UTSF_ROOT/knowledge/ so it survives between sessions.

    In dev mode (normal Python): use the src/knowledge/ directory next to this
    file, same as before.
    """
    root = os.environ.get("UTSF_ROOT")
    if root and os.path.isdir(root):
        d = os.path.join(root, "knowledge")
        os.makedirs(d, exist_ok=True)
        return d
    return _HERE


# Paths are now computed lazily so that UTSF_ROOT is already set when called
def _learning_data_path() -> str:
    return os.path.join(_get_writable_dir(), "learning_data.json")


def _learned_dict_path() -> str:
    return os.path.join(_get_writable_dir(), "learned_dict.py")

# How many confirmations before auto-promoting to the permanent learned dict
AUTO_PROMOTE_THRESHOLD = 3

# Decay factor — a correction vote reduces confidence by this fraction
CORRECTION_DECAY = 0.5


# ─── Persistence helpers ──────────────────────────────────────────────────────

def _load_data() -> Dict:
    """Load the learning frequency store from disk."""
    path = _learning_data_path()
    if not os.path.exists(path):
        return {"entries": {}, "stats": {"total_confirmations": 0, "total_corrections": 0,
                                          "auto_promoted": 0}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"entries": {}, "stats": {"total_confirmations": 0, "total_corrections": 0,
                                          "auto_promoted": 0}}


def _save_data(data: Dict):
    """Atomically save the learning store."""
    path = _learning_data_path()
    tmp  = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ─── Key helpers ──────────────────────────────────────────────────────────────

def _entry_key(learn_type: str, raw: str, canonical) -> str:
    """Deterministic dict key for a (type, raw, canonical) triple."""
    canon_str = json.dumps(canonical, sort_keys=True) if not isinstance(canonical, str) else canonical
    return f"{learn_type}||{raw.lower().strip()}||{canon_str}"


# ─── Core API ─────────────────────────────────────────────────────────────────

def record_confirmation(learn_type: str, raw: str, canonical) -> Dict:
    """
    Record that the user confirmed a match (raw → canonical is CORRECT).
    Returns a dict with keys: confirmed_count, auto_promoted, threshold.
    """
    data = _load_data()
    key  = _entry_key(learn_type, raw, canonical)
    now  = time.time()

    entry = data["entries"].get(key, {
        "type":        learn_type,
        "raw":         raw,
        "canonical":   canonical,
        "confirmations": 0,
        "corrections":   0,
        "confidence":    0.5,
        "auto_promoted": False,
        "first_seen":    now,
        "last_seen":     now,
    })

    entry["confirmations"] += 1
    # Confidence grows with each confirmation (asymptotes toward 1.0)
    entry["confidence"] = min(0.99, entry["confidence"] + (1.0 - entry["confidence"]) * 0.35)
    entry["last_seen"] = now
    data["entries"][key] = entry
    data["stats"]["total_confirmations"] = data["stats"].get("total_confirmations", 0) + 1

    auto_promoted = False
    if (entry["confirmations"] >= AUTO_PROMOTE_THRESHOLD
            and not entry["auto_promoted"]
            and entry["confidence"] >= 0.8):
        _write_to_learned_dict(learn_type, raw, canonical)
        entry["auto_promoted"] = True
        data["stats"]["auto_promoted"] = data["stats"].get("auto_promoted", 0) + 1
        auto_promoted = True

    _save_data(data)
    return {
        "confirmed_count": entry["confirmations"],
        "confidence":      round(entry["confidence"], 3),
        "auto_promoted":   auto_promoted,
        "threshold":       AUTO_PROMOTE_THRESHOLD,
    }


def record_correction(learn_type: str, raw: str, wrong_canonical, correct_canonical) -> Dict:
    """
    Record that the user corrected a bad match (raw → wrong was WRONG,
    raw → correct is RIGHT).
    Decays confidence on the wrong pair; immediately writes the correct one.
    """
    data = _load_data()
    now  = time.time()

    # Decay the wrong entry
    bad_key   = _entry_key(learn_type, raw, wrong_canonical)
    bad_entry = data["entries"].get(bad_key, {})
    if bad_entry:
        bad_entry["corrections"] = bad_entry.get("corrections", 0) + 1
        bad_entry["confidence"]  = max(0.0, bad_entry.get("confidence", 0.5) * CORRECTION_DECAY)
        bad_entry["last_seen"]   = now
        data["entries"][bad_key] = bad_entry

    # Immediately write the correct mapping (user explicitly taught us)
    _write_to_learned_dict(learn_type, raw, correct_canonical)

    # Record the correct entry with a strong head start
    good_key   = _entry_key(learn_type, raw, correct_canonical)
    good_entry = data["entries"].get(good_key, {
        "type":          learn_type,
        "raw":           raw,
        "canonical":     correct_canonical,
        "confirmations": 0,
        "corrections":   0,
        "confidence":    0.5,
        "auto_promoted": False,
        "first_seen":    now,
        "last_seen":     now,
    })
    good_entry["confirmations"] += 1
    good_entry["confidence"]     = min(0.99, good_entry["confidence"] + 0.4)
    good_entry["auto_promoted"]  = True   # was explicitly taught
    good_entry["last_seen"]      = now
    data["entries"][good_key] = good_entry
    data["stats"]["total_corrections"] = data["stats"].get("total_corrections", 0) + 1

    _save_data(data)
    return {"written": True, "correct_canonical": correct_canonical}


def get_suggestion(learn_type: str, raw: str) -> Optional[Tuple]:
    """
    Look up the best known canonical for a raw label.
    Returns (canonical, confidence) if confidence ≥ 0.7, else None.
    """
    data    = _load_data()
    raw_key = raw.lower().strip()
    best    = None

    for key, entry in data["entries"].items():
        if entry.get("type") != learn_type:
            continue
        if entry.get("raw", "").lower().strip() != raw_key:
            continue
        conf = entry.get("confidence", 0.0)
        if conf < 0.7:
            continue
        if best is None or conf > best[1]:
            best = (entry["canonical"], conf)

    return best


def get_stats() -> Dict:
    """Return learning statistics for the API."""
    data    = _load_data()
    entries = data.get("entries", {})
    stats   = data.get("stats", {})

    # Aggregate per type
    by_type: Dict[str, Dict] = {}
    for entry in entries.values():
        t = entry.get("type", "unknown")
        if t not in by_type:
            by_type[t] = {"total": 0, "promoted": 0, "avg_confidence": 0.0, "_conf_sum": 0.0}
        by_type[t]["total"]        += 1
        by_type[t]["_conf_sum"]    += entry.get("confidence", 0.0)
        if entry.get("auto_promoted"):
            by_type[t]["promoted"] += 1

    for t in by_type:
        n = by_type[t]["total"]
        by_type[t]["avg_confidence"] = round(by_type[t]["_conf_sum"] / n, 3) if n else 0
        del by_type[t]["_conf_sum"]

    # Top high-confidence entries (promoted)
    top = sorted(
        [e for e in entries.values() if e.get("auto_promoted")],
        key=lambda e: e.get("confirmations", 0),
        reverse=True
    )[:20]

    return {
        "total_entries":       len(entries),
        "total_confirmations": stats.get("total_confirmations", 0),
        "total_corrections":   stats.get("total_corrections",  0),
        "auto_promoted":       stats.get("auto_promoted",       0),
        "promote_threshold":   AUTO_PROMOTE_THRESHOLD,
        "by_type":             by_type,
        "top_learned":         [
            {"type": e["type"], "raw": e["raw"],
             "canonical": e["canonical"],
             "confirmations": e["confirmations"],
             "confidence": round(e["confidence"], 3)}
            for e in top
        ],
    }


# ─── Write into learned_dict.py ───────────────────────────────────────────────

def _write_to_learned_dict(learn_type: str, raw: str, canonical):
    """
    Atomically insert/update an entry in learned_dict.py (writable copy).

    In frozen EXE mode this writes to UTSF_ROOT/knowledge/learned_dict.py
    (a user-writable file next to the EXE), NOT the read-only bundled copy
    inside sys._MEIPASS.  The file is created from scratch if it doesn't exist.
    """
    path = _learned_dict_path()

    # Bootstrap an empty learned_dict.py in the writable dir if needed
    if not os.path.exists(path):
        try:
            with open(path, "w", encoding="utf-8") as f:
                f.write(
                    '"""User-learned corrections (auto-managed). Do not edit by hand.)"""\n\n'
                    'LEARNED_CHARGES = {}\n\n'
                    'LEARNED_ZONES = {}\n'
                )
        except OSError:
            return  # Can't write — silently skip

    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    if learn_type == "charge":
        key        = raw.lower().strip()
        entry_line = f'    {key!r}: {canonical!r},'
        target     = "LEARNED_CHARGES"
    elif learn_type == "zone":
        key   = raw.upper().strip()
        zones = canonical if isinstance(canonical, list) else [canonical]
        entry_line = f'    {key!r}: {zones!r},'
        target = "LEARNED_ZONES"
    else:
        return  # Unknown type — don't touch the file

    # Already present?  (simple substring check on the key portion)
    if f"    {key!r}:" in content:
        # Update in-place: replace existing line for this key
        content = re.sub(
            rf"(    {re.escape(repr(key))}:)[^\n]*",
            entry_line,
            content
        )
    else:
        # Insert before closing } of the target dict
        m = re.search(
            rf'({re.escape(target)}\s*=\s*\{{)(.*?)(\}}\s*\n)',
            content, re.DOTALL
        )
        if m:
            content = content[:m.start(3)] + "\n" + entry_line + "\n" + content[m.start(3):]
        else:
            content += f"\n# auto-learned\n{target}.setdefault({key!r}, {canonical!r})\n"

    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)
    print(f"[MLDict] Auto-wrote {learn_type}: {raw!r} → {canonical!r}")
