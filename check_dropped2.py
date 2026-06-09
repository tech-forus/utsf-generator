"""
Diagnose WHY 207 pincodes are in source but missing from UTSF served.
"""
import json, pandas as pd

SRC = r'C:\Users\tech\Downloads\generated_test_sets\generated_test_sets\Folder_3_Paisa_Vasool_Premium\Set_14_InsuranceInkaar\insurance_inkaar_assured_serviceability.xlsx'
UTSF_PATH = r'output\insuranceinkaar2.utsf.json'
MASTER_PATH = r'data\pincodes.json'

df = pd.read_excel(SRC, sheet_name='Pincode Records', header=0)
df.columns = ['pincode','state','city','zone','pickup_status','delivery_status','oda']
df = df.dropna(subset=['pincode'])
df['pincode'] = df['pincode'].astype(str).str.strip().str.zfill(6)
df['oda_flag'] = df['oda'].astype(str).str.strip().str.upper().isin(['YES','Y','1','TRUE'])

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

missing = sorted(set(df['pincode']) - utsf_served_pins)
print(f'Missing pincodes: {len(missing)}')

# Check for duplicates in source
dup_check = df[df['pincode'].isin(missing)]
print()
print('Occurrences of missing pincodes in source Excel:')
print(dup_check[['pincode','zone','oda_flag']].groupby('pincode').agg(
    count=('pincode','count'),
    oda_values=('oda_flag', lambda x: list(x)),
    zones=('zone', lambda x: list(x))
).head(30).to_string())

print()
# Are these duplicates (appear as both served AND ODA)?
print('Summary — missing pincodes by duplicate type:')
for p in missing[:30]:
    rows = df[df['pincode']==p]
    oda_vals = list(rows['oda_flag'])
    zones = list(rows['zone'])
    print(f'  {p}: {len(rows)} rows, oda={oda_vals}, zone={zones}')

print()
# Check: do duplicates appear once as ODA and once as served?
both_served_and_oda = 0
only_served = 0
only_oda = 0
other = 0
for p in missing:
    rows = df[df['pincode']==p]
    is_oda = list(rows['oda_flag'])
    if len(rows) == 1:
        if is_oda[0]:
            only_oda += 1
        else:
            only_served += 1
    elif True in is_oda and False in is_oda:
        both_served_and_oda += 1
    else:
        other += 1

print(f'Missing pincodes breakdown:')
print(f'  Appear once as non-ODA only    : {only_served}')
print(f'  Appear once as ODA only        : {only_oda}')
print(f'  Appear as BOTH served AND ODA  : {both_served_and_oda}')
print(f'  Other (all-ODA duplicates etc) : {other}')

# Check zone_mapper int keys
print()
with open(MASTER_PATH) as f:
    master_raw = json.load(f)
master_int = {int(e['pincode']): e for e in master_raw if str(e.get('pincode','')).isdigit()}
print('Master lookup with INT keys (correct):')
for p in missing[:5]:
    m = master_int.get(int(p))
    print(f'  {p} -> {m}')
