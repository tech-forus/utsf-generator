import json
import pandas as pd
from collections import defaultdict

SRC = r'C:\Users\tech\Downloads\generated_test_sets\generated_test_sets\Folder_3_Paisa_Vasool_Premium\Set_14_InsuranceInkaar\insurance_inkaar_assured_serviceability.xlsx'
UTSF_PATH = r'output\insuranceinkaar2.utsf.json'
MASTER_PATH = r'data\pincodes.json'

# ── Load source Excel ─────────────────────────────────────────────────────────
df = pd.read_excel(SRC, sheet_name='Pincode Records', header=0)
df.columns = ['pincode','state','city','zone','pickup_status','delivery_status','oda']
df = df.dropna(subset=['pincode'])
df['pincode'] = df['pincode'].astype(str).str.strip().str.zfill(6)
df['oda_flag'] = df['oda'].astype(str).str.strip().str.upper().isin(['YES','Y','1','TRUE'])
df['zone'] = df['zone'].astype(str).str.strip()

src_served = df[~df['oda_flag']].copy()
src_oda    = df[df['oda_flag']].copy()

src_served_pins = set(src_served['pincode'])
src_oda_pins    = set(src_oda['pincode'])
src_zone_map    = dict(zip(df['pincode'], df['zone']))   # vendor's claimed zone

print('=== SOURCE DATA ===')
print(f'Total rows      : {len(df)}')
print(f'Non-ODA (served): {len(src_served_pins)}')
print(f'ODA             : {len(src_oda_pins)}')
print()

# ── Load master pincodes.json ─────────────────────────────────────────────────
with open(MASTER_PATH) as f:
    master_raw = json.load(f)
master = {}   # pincode -> {zone, city, state}
for entry in master_raw:
    pin = str(entry.get('pincode', '')).zfill(6)
    master[pin] = entry

# ── Load UTSF ─────────────────────────────────────────────────────────────────
with open(UTSF_PATH) as f:
    utsf = json.load(f)

svc   = utsf.get('serviceability', {})
oda_b = utsf.get('oda', {})

# Decode served pincodes from UTSF ranges+singles
utsf_served_pins = set()
utsf_served_by_zone = {}
for zone, data in svc.items():
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

# Decode ODA pincodes from UTSF
utsf_oda_pins = set()
utsf_oda_by_zone = {}
for zone, data in oda_b.items():
    pins = set()
    for r in data.get('odaRanges', []):
        for p in range(r['s'], r['e'] + 1):
            pins.add(str(p).zfill(6))
    for p in data.get('odaSingles', []):
        pins.add(str(p).zfill(6))
    utsf_oda_by_zone[zone] = pins
    utsf_oda_pins |= pins

print('=== UTSF DATA ===')
print(f'Served pincodes : {len(utsf_served_pins)}')
print(f'ODA pincodes    : {len(utsf_oda_pins)}')
print()

# ── Served comparison ─────────────────────────────────────────────────────────
print('=== SERVED PINCODE COMPARISON ===')
in_src_not_utsf = src_served_pins - utsf_served_pins
in_utsf_not_src = utsf_served_pins - src_served_pins

print(f'Source served   : {len(src_served_pins)}')
print(f'UTSF served     : {len(utsf_served_pins)}')
print(f'In source, missing from UTSF: {len(in_src_not_utsf)}')
print(f'In UTSF, not in source       : {len(in_utsf_not_src)}')

if in_src_not_utsf:
    print('\nSAMPLE pincodes dropped (first 20):')
    for p in sorted(in_src_not_utsf)[:20]:
        m = master.get(p)
        src_z = src_zone_map.get(p, '?')
        mz = m.get('zone','?') if m else 'NOT_IN_MASTER'
        print(f'  {p}  source_zone={src_z}  master_zone={mz}')

if in_utsf_not_src:
    print('\nSAMPLE pincodes extra in UTSF (first 10):')
    for p in sorted(in_utsf_not_src)[:10]:
        m = master.get(p)
        mz = m.get('zone','?') if m else 'NOT_IN_MASTER'
        print(f'  {p}  master_zone={mz}')

# ── ODA comparison ────────────────────────────────────────────────────────────
print()
print('=== ODA PINCODE COMPARISON ===')
oda_in_src_not_utsf = src_oda_pins - utsf_oda_pins
oda_in_utsf_not_src = utsf_oda_pins - src_oda_pins

print(f'Source ODA  : {len(src_oda_pins)}')
print(f'UTSF ODA    : {len(utsf_oda_pins)}')
print(f'Missing from UTSF ODA: {len(oda_in_src_not_utsf)}')
print(f'Extra in UTSF ODA    : {len(oda_in_utsf_not_src)}')

if oda_in_src_not_utsf:
    print('\nSample ODA dropped (first 20):')
    for p in sorted(oda_in_src_not_utsf)[:20]:
        m = master.get(p)
        src_z = src_zone_map.get(p, '?')
        mz = m.get('zone','?') if m else 'NOT_IN_MASTER'
        print(f'  {p}  source_zone={src_z}  master_zone={mz}')

# ── Zone mismatch analysis ────────────────────────────────────────────────────
print()
print('=== ZONE MISMATCH ANALYSIS ===')
print('(vendor-claimed zone vs master pincodes.json zone)')
mismatch_count = 0
not_in_master = 0
mismatch_by_pair = defaultdict(list)

for _, row in df.iterrows():
    pin = row['pincode']
    src_z = row['zone']
    m = master.get(pin)
    if m is None:
        not_in_master += 1
        continue
    master_z = m.get('zone','')
    if src_z != master_z:
        mismatch_count += 1
        mismatch_by_pair[(src_z, master_z)].append(pin)

print(f'Pincodes not in master         : {not_in_master}')
print(f'Zone mismatches (in master)    : {mismatch_count}')
if mismatch_by_pair:
    print('\nMismatch breakdown (vendor_zone -> master_zone: count):')
    for (vz, mz), pins in sorted(mismatch_by_pair.items(), key=lambda x: -len(x[1])):
        print(f'  {vz} -> {mz}: {len(pins)} pincodes  (sample: {pins[:3]})')

# ── Zone-by-zone served count: source vs UTSF ────────────────────────────────
print()
print('=== ZONE-BY-ZONE SERVED COUNT: SOURCE vs UTSF ===')
print(f'{"Zone":6} {"Src(served)":12} {"UTSF(served)":13} {"Src(ODA)":9} {"UTSF(ODA)":10} {"Status"}')
all_zones = sorted(set(list(utsf_served_by_zone.keys()) + list(utsf_oda_by_zone.keys()) +
                        list(src_served.groupby("zone").groups.keys()) +
                        list(src_oda.groupby("zone").groups.keys())))
for z in all_zones:
    s_srv = len(src_served[src_served['zone']==z])
    u_srv = len(utsf_served_by_zone.get(z, set()))
    s_oda = len(src_oda[src_oda['zone']==z])
    u_oda = len(utsf_oda_by_zone.get(z, set()))
    status = 'OK' if s_srv==u_srv and s_oda==u_oda else 'DIFF'
    print(f'  {z:4} {s_srv:12} {u_srv:13} {s_oda:9} {u_oda:10}  {status}')

# ── Required UTSF fields check ────────────────────────────────────────────────
print()
print('=== UTSF REQUIRED FIELDS CHECK ===')
REQUIRED = {
    'meta.id':           lambda u: bool(u.get('meta',{}).get('id')),
    'meta.companyName':  lambda u: bool(u.get('meta',{}).get('companyName')),
    'meta.gstNo':        lambda u: u.get('meta',{}).get('gstNo') not in (None,''),
    'meta.city':         lambda u: bool(u.get('meta',{}).get('city')),
    'meta.mode':         lambda u: bool(u.get('meta',{}).get('mode')),
    'pricing.fuel':      lambda u: u.get('pricing',{}).get('fuel') not in (None,''),
    'pricing.zoneRates': lambda u: bool(u.get('pricing',{}).get('zoneRates')),
    'pricing.docketCharges': lambda u: u.get('pricing',{}).get('docketCharges') not in (None,''),
    'pricing.minCharges':    lambda u: u.get('pricing',{}).get('minCharges') not in (None,''),
    'pricing.odaCharges':    lambda u: u.get('pricing',{}).get('odaCharges') not in (None,''),
    'serviceability':    lambda u: bool(u.get('serviceability')),
    'oda':               lambda u: bool(u.get('oda')),
    'stats.totalPincodes': lambda u: bool(u.get('stats',{}).get('totalPincodes')),
}
for field, check in REQUIRED.items():
    ok = check(utsf)
    val = ''
    if '.' in field:
        s, k = field.split('.', 1)
        val = utsf.get(s, {}).get(k, 'MISSING')
    print(f'  {"OK" if ok else "MISSING":7} {field:30} = {str(val)[:60]}')

print()
print('=== META FIELDS ===')
meta = utsf.get('meta', {})
for k, v in meta.items():
    print(f'  {k}: {v}')

print()
print('=== PRICING FIELDS ===')
pricing = utsf.get('pricing', {})
for k, v in pricing.items():
    if k == 'zoneRates':
        zones = list(v.keys())
        pairs = sum(len(r) for r in v.values())
        print(f'  zoneRates: {len(zones)} origins, {pairs} pairs')
    else:
        print(f'  {k}: {v}')
