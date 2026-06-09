import json

MASTER_PATH = r'data\pincodes.json'
with open(MASTER_PATH) as f:
    master_raw = json.load(f)
master = {}
for e in master_raw:
    pin = str(e.get('pincode','')).zfill(6)
    master[pin] = e

# Full list of 207 missing pincodes
import pandas as pd

SRC = r'C:\Users\tech\Downloads\generated_test_sets\generated_test_sets\Folder_3_Paisa_Vasool_Premium\Set_14_InsuranceInkaar\insurance_inkaar_assured_serviceability.xlsx'
df = pd.read_excel(SRC, sheet_name='Pincode Records', header=0)
df.columns = ['pincode','state','city','zone','pickup_status','delivery_status','oda']
df = df.dropna(subset=['pincode'])
df['pincode'] = df['pincode'].astype(str).str.strip().str.zfill(6)

src_all_pins = set(df['pincode'])

UTSF_PATH = r'output\insuranceinkaar2.utsf.json'
with open(UTSF_PATH) as f:
    utsf = json.load(f)

svc = utsf.get('serviceability', {})
utsf_served_pins = set()
for zone, data in svc.items():
    if data.get('mode') == 'NOT_SERVED':
        continue
    for r in data.get('servedRanges', []):
        for p in range(r['s'], r['e'] + 1):
            utsf_served_pins.add(str(p).zfill(6))
    for p in data.get('servedSingles', []):
        utsf_served_pins.add(str(p).zfill(6))

missing = sorted(src_all_pins - utsf_served_pins)
print(f'Total missing from UTSF: {len(missing)}')
print()

# Summarize by prefix
from collections import Counter
prefix_counts = Counter(p[:3] for p in missing)
print('Missing pincodes by prefix:')
for prefix, cnt in sorted(prefix_counts.items()):
    print(f'  {prefix}xxx : {cnt}')

print()
print('Detail of ALL missing (showing master zone):')
zone_groups = {}
for p in missing:
    m = master.get(p, {})
    mz = m.get('zone', 'NOT_IN_MASTER')
    zone_groups.setdefault(mz, []).append(p)

for mz, pins in sorted(zone_groups.items()):
    print(f'\n  master_zone={mz} : {len(pins)} pincodes')
    for p in pins[:5]:
        m = master.get(p, {})
        src_row = df[df['pincode']==p].iloc[0] if len(df[df['pincode']==p]) > 0 else None
        src_z = src_row['zone'] if src_row is not None else '?'
        oda_f = src_row['oda'] if src_row is not None else '?'
        print(f'    {p}  src_zone={src_z}  oda={oda_f}  master_city={m.get("city","?")}  master_state={m.get("state","?")}')
    if len(pins) > 5:
        print(f'    ... and {len(pins)-5} more')
