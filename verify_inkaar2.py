"""
Full verification of Insurance Inkar 2:
  - Source Excel vs UTSF: served pincodes, ODA pincodes, zone counts
  - Zone mismatches (vendor-claimed vs master)
  - UTSF required fields
  - MongoDB stored document
"""
import json, sys
import pandas as pd
from collections import defaultdict

SRC = r'transporters\InsuranceInkaar2\company_details\insurance_inkaar_assured_serviceability.xlsx'
UTSF_PATH = r'output\insuranceinkaar2.utsf.json'
MASTER_PATH = r'data\pincodes.json'

DIVIDER = '=' * 70

# ── Load source Excel ─────────────────────────────────────────────────────────
df = pd.read_excel(SRC, sheet_name='Pincode Records', header=0)
df.columns = ['pincode','state','city','zone','pickup_status','delivery_status','oda']
df = df.dropna(subset=['pincode'])
df['pincode'] = df['pincode'].astype(str).str.strip().str.zfill(6)
df['oda_flag'] = df['oda'].astype(str).str.strip().str.upper().isin(['YES','Y','1','TRUE'])
df['zone'] = df['zone'].astype(str).str.strip()

src_all   = df.copy()
src_served_only = df[~df['oda_flag']].copy()   # non-ODA rows
src_oda         = df[df['oda_flag']].copy()    # ODA rows

src_all_pins         = set(src_all['pincode'])
src_served_only_pins = set(src_served_only['pincode'])
src_oda_pins         = set(src_oda['pincode'])
src_zone_map         = dict(zip(df['pincode'], df['zone']))

# ── Load master pincodes.json ─────────────────────────────────────────────────
with open(MASTER_PATH) as f:
    master_raw = json.load(f)
master = {}
for e in master_raw:
    pin = str(e.get('pincode', '')).zfill(6)
    master[pin] = e

# ── Load UTSF ─────────────────────────────────────────────────────────────────
with open(UTSF_PATH) as f:
    utsf = json.load(f)

svc_block = utsf.get('serviceability', {})
oda_block = utsf.get('oda', {})

# UTSF served = ALL zone-mapped pincodes (includes ODA by design — they appear
# in serviceability AND in oda block simultaneously)
utsf_served_pins = set()
utsf_served_by_zone = {}
for zone, data in svc_block.items():
    if data.get('mode') == 'NOT_SERVED':
        continue
    pins = set()
    for r in data.get('servedRanges', []):
        for p in range(r['s'], r['e'] + 1):
            pins.add(str(p).zfill(6))
    for p in data.get('servedSingles', []):
        pins.add(str(p).zfill(6))
    for ov in data.get('pinOverrides', []):
        pins.add(str(ov['pincode']).zfill(6))
    utsf_served_by_zone[zone] = pins
    utsf_served_pins |= pins

utsf_oda_pins = set()
utsf_oda_by_zone = {}
for zone, data in oda_block.items():
    pins = set()
    for r in data.get('odaRanges', []):
        for p in range(r['s'], r['e'] + 1):
            pins.add(str(p).zfill(6))
    for p in data.get('odaSingles', []):
        pins.add(str(p).zfill(6))
    utsf_oda_by_zone[zone] = pins
    utsf_oda_pins |= pins

# ── 1. Source overview ────────────────────────────────────────────────────────
print(DIVIDER)
print('1. SOURCE DATA OVERVIEW')
print(DIVIDER)
print(f'  Total rows in Excel        : {len(df)}')
print(f'  Non-ODA (served)           : {len(src_served_only_pins)}')
print(f'  ODA                        : {len(src_oda_pins)}')
print(f'  Total unique pincodes      : {len(src_all_pins)}')

# ── 2. UTSF overview ─────────────────────────────────────────────────────────
print()
print(DIVIDER)
print('2. UTSF DATA OVERVIEW')
print(DIVIDER)
print(f'  Served (incl. ODA in svc)  : {len(utsf_served_pins)}')
print(f'  ODA block                  : {len(utsf_oda_pins)}')
utsf_served_non_oda = utsf_served_pins - utsf_oda_pins
print(f'  Served-only (svc minus ODA): {len(utsf_served_non_oda)}')

# ── 3. Pincode comparison (source total vs UTSF served) ──────────────────────
print()
print(DIVIDER)
print('3. PINCODE COMPARISON: Source ALL vs UTSF Served')
print('   (UTSF served includes ODA pincodes — correct by design)')
print(DIVIDER)

missing_from_utsf = src_all_pins - utsf_served_pins     # in source, not in UTSF served
extra_in_utsf     = utsf_served_pins - src_all_pins      # in UTSF, not in source at all

print(f'  Source total               : {len(src_all_pins)}')
print(f'  UTSF served                : {len(utsf_served_pins)}')
print(f'  Missing from UTSF          : {len(missing_from_utsf)}')
print(f'  Extra in UTSF (unexpected) : {len(extra_in_utsf)}')

# Classify missing: in master vs not
missing_in_master     = {p for p in missing_from_utsf if p in master}
missing_not_in_master = missing_from_utsf - missing_in_master

print(f'    Of missing: {len(missing_in_master)} ARE in master (lookup bug)')
print(f'    Of missing: {len(missing_not_in_master)} NOT in master (expected drop)')

if missing_in_master:
    print(f'\n  BUG: pincodes in master but dropped from UTSF (first 20):')
    for p in sorted(missing_in_master)[:20]:
        m = master[p]
        src_z = src_zone_map.get(p, '?')
        print(f'    {p}  source_zone={src_z}  master_zone={m.get("zone","?")}  city={m.get("city","?")}')

if extra_in_utsf:
    print(f'\n  UNEXPECTED extra pincodes in UTSF (first 10):')
    for p in sorted(extra_in_utsf)[:10]:
        m = master.get(p)
        mz = m.get('zone','?') if m else 'NOT_IN_MASTER'
        print(f'    {p}  master_zone={mz}')

# ── 4. ODA pincode comparison ─────────────────────────────────────────────────
print()
print(DIVIDER)
print('4. ODA PINCODE COMPARISON')
print(DIVIDER)
oda_missing = src_oda_pins - utsf_oda_pins
oda_extra   = utsf_oda_pins - src_oda_pins

print(f'  Source ODA                 : {len(src_oda_pins)}')
print(f'  UTSF ODA                   : {len(utsf_oda_pins)}')
print(f'  Missing from UTSF ODA      : {len(oda_missing)}')
print(f'  Extra in UTSF ODA          : {len(oda_extra)}')

if oda_missing:
    oda_miss_in_master = {p for p in oda_missing if p in master}
    oda_miss_not_in_master = oda_missing - oda_miss_in_master
    print(f'    Of missing ODA: {len(oda_miss_in_master)} in master (lookup bug)')
    print(f'    Of missing ODA: {len(oda_miss_not_in_master)} not in master (expected)')
    if oda_miss_in_master:
        print(f'\n  BUG: ODA pincodes in master but dropped from UTSF ODA (first 20):')
        for p in sorted(oda_miss_in_master)[:20]:
            m = master[p]
            src_z = src_zone_map.get(p,'?')
            print(f'    {p}  source_zone={src_z}  master_zone={m.get("zone","?")}')

# ── 5. Zone-by-zone breakdown ─────────────────────────────────────────────────
print()
print(DIVIDER)
print('5. ZONE-BY-ZONE BREAKDOWN')
print('   Source: served-only | ODA | total')
print('   UTSF:   served(incl.ODA in svc) | ODA block | served-only(svc-ODA)')
print(DIVIDER)
print(f'  {"Zone":5} {"Src_srv":8} {"Src_ODA":8} {"Src_tot":8} '
      f'{"U_svc":7} {"U_ODA":7} {"U_srvonly":10} {"Delta_tot":10} {"Status"}')

all_zones = sorted(set(
    list(utsf_served_by_zone.keys()) + list(utsf_oda_by_zone.keys()) +
    list(src_served_only['zone'].unique()) + list(src_oda['zone'].unique())
))

for z in all_zones:
    s_srv = len(src_served_only[src_served_only['zone']==z])
    s_oda = len(src_oda[src_oda['zone']==z])
    s_tot = s_srv + s_oda
    u_svc = len(utsf_served_by_zone.get(z, set()))
    u_oda = len(utsf_oda_by_zone.get(z, set()))
    u_srv_only = len(utsf_served_by_zone.get(z, set()) - utsf_oda_by_zone.get(z, set()))
    delta = u_svc - s_tot
    status = 'OK' if delta == 0 else f'DIFF({delta:+d})'
    print(f'  {z:5} {s_srv:8} {s_oda:8} {s_tot:8} '
          f'{u_svc:7} {u_oda:7} {u_srv_only:10} {delta:10}  {status}')

# ── 6. Zone mismatch: vendor-claimed vs master ────────────────────────────────
print()
print(DIVIDER)
print('6. ZONE MISMATCH: Vendor-claimed zone vs master pincodes.json zone')
print(DIVIDER)
mismatch_count = 0
not_in_master_count = 0
mismatch_by_pair = defaultdict(list)

for _, row in df.iterrows():
    pin = row['pincode']
    src_z = row['zone']
    m = master.get(pin)
    if m is None:
        not_in_master_count += 1
        continue
    master_z = m.get('zone', '')
    if src_z != master_z:
        mismatch_count += 1
        mismatch_by_pair[(src_z, master_z)].append(pin)

print(f'  Pincodes not in master         : {not_in_master_count}')
print(f'  Zone mismatches (in master)    : {mismatch_count}')
if mismatch_by_pair:
    print('\n  Mismatch breakdown (vendor_zone -> master_zone: count + samples):')
    for (vz, mz), pins in sorted(mismatch_by_pair.items(), key=lambda x: -len(x[1])):
        print(f'    {vz} -> {mz}: {len(pins)} pincodes  e.g. {pins[:3]}')
else:
    print('  All vendor zone labels match master. No zone conflicts.')

# ── 7. Required UTSF fields ───────────────────────────────────────────────────
print()
print(DIVIDER)
print('7. REQUIRED UTSF FIELDS')
print(DIVIDER)

meta    = utsf.get('meta', {})
pricing = utsf.get('pricing', {})
pr      = pricing.get('priceRate', {})   # charges live here
stats   = utsf.get('stats', {})

def chk(label, val):
    ok = val not in (None, '', 0, {}, [])
    tag = 'OK     ' if ok else 'MISSING'
    display = str(val)[:60] if val is not None else 'null'
    print(f'  {tag}  {label:35} = {display}')

print('  -- meta --')
chk('meta.id',           meta.get('id'))
chk('meta.companyName',  meta.get('companyName'))
chk('meta.vendorCode',   meta.get('vendorCode'))
chk('meta.gstNo',        meta.get('gstNo'))
chk('meta.city',         meta.get('city'))
chk('meta.transportMode', meta.get('transportMode'))
chk('meta.address',      meta.get('address'))
chk('meta.contactPhone', meta.get('contactPhone'))
chk('meta.contactEmail', meta.get('contactEmail'))

print('  -- pricing charges (in priceRate) --')
chk('priceRate.fuel',          pr.get('fuel'))
chk('priceRate.docketCharges', pr.get('docketCharges'))
chk('priceRate.minCharges',    pr.get('minCharges'))
chk('priceRate.minWeight',     pr.get('minWeight'))
chk('priceRate.odaCharges',    pr.get('odaCharges'))
chk('priceRate.rovCharges',    pr.get('rovCharges'))
chk('priceRate.insuranceCharges', pr.get('insuranceCharges'))
chk('priceRate.greenTax',      pr.get('greenTax'))
chk('priceRate.daccCharges',   pr.get('daccCharges'))
chk('priceRate.topayCharges',  pr.get('topayCharges'))
chk('priceRate.divisor',       pr.get('divisor'))
chk('priceRate.codCharges',    pr.get('codCharges'))

print('  -- rate matrix --')
chk('pricing.zoneRates (origins)', len(pricing.get('zoneRates', {})) or None)

print('  -- serviceability --')
chk('serviceability (zones)', len(svc_block) or None)
chk('oda (zones)',            len(oda_block) or None)
chk('stats.totalPincodes',   stats.get('totalPincodes'))
chk('stats.zonesServed',     stats.get('zonesServed'))

# ── 8. MongoDB check ──────────────────────────────────────────────────────────
print()
print(DIVIDER)
print('8. MONGODB CHECK')
print(DIVIDER)
try:
    from pymongo import MongoClient
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=3000)
    client.server_info()
    db = client['freight_compare']

    # try both possible collection names
    for col_name in ['vendors', 'transporters', 'utsf']:
        col = db[col_name]
        count = col.count_documents({})
        print(f'  Collection "{col_name}": {count} documents')

    # search for Insurance Inkar
    for col_name in ['vendors', 'transporters']:
        col = db[col_name]
        doc = col.find_one({'$or': [
            {'meta.id': 'insuranceinkaar2'},
            {'meta.companyName': {'$regex': 'insurance', '$options': 'i'}},
            {'id': 'insuranceinkaar2'},
        ]})
        if doc:
            print(f'\n  Found in "{col_name}":')
            print(f'    _id            : {doc.get("_id")}')
            m = doc.get('meta', doc)
            print(f'    companyName    : {m.get("companyName", doc.get("companyName"))}')
            print(f'    gstNo          : {m.get("gstNo", doc.get("gstNo"))}')
            print(f'    vendorCode     : {m.get("vendorCode", doc.get("vendorCode"))}')
            svc = doc.get('serviceability', {})
            print(f'    serviceability : {len(svc)} zones')
            oda = doc.get('oda', {})
            print(f'    oda            : {len(oda)} zones')
            pr_db = doc.get('pricing', {}).get('priceRate', doc.get('priceRate', {}))
            print(f'    fuel           : {pr_db.get("fuel")}')
            print(f'    odaCharges     : {pr_db.get("odaCharges")}')
            print(f'    docketCharges  : {pr_db.get("docketCharges")}')
            zr = doc.get('pricing', {}).get('zoneRates', doc.get('zoneRates', {}))
            print(f'    zoneRates      : {len(zr)} origins')
            print(f'    approvalStatus : {m.get("approvalStatus", doc.get("approvalStatus"))}')
        else:
            print(f'  Not found in "{col_name}"')

except Exception as e:
    print(f'  MongoDB error: {e}')
    print('  (Is freight-compare-backend running? Try checking port 27017)')
