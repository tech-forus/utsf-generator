import json, pandas as pd

SRC = r'C:\Users\tech\Downloads\generated_test_sets\generated_test_sets\Folder_3_Paisa_Vasool_Premium\Set_14_InsuranceInkaar\insurance_inkaar_assured_serviceability.xlsx'
df = pd.read_excel(SRC, sheet_name='Pincode Records', header=0)
df.columns = ['pincode','state','city','zone','pickup_status','delivery_status','oda']
df = df.dropna(subset=['pincode'])
df['pincode'] = df['pincode'].astype(str).str.strip().str.zfill(6)

with open('output/insuranceinkaar2.utsf.json') as f:
    utsf = json.load(f)
with open('data/pincodes.json') as f2:
    master = {str(e['pincode']).zfill(6): e for e in json.load(f2)}

svc = utsf.get('serviceability', {})
utsf_all = set()
utsf_by_zone = {}
for zone, data in svc.items():
    if data.get('mode') == 'NOT_SERVED': continue
    pins = set()
    for r in data.get('servedRanges', []):
        for p in range(r['s'], r['e']+1):
            pins.add(str(p).zfill(6))
            utsf_all.add(str(p).zfill(6))
    for p in data.get('servedSingles', []):
        pins.add(str(p).zfill(6))
        utsf_all.add(str(p).zfill(6))
    utsf_by_zone[zone] = pins

src_s3 = set(df[df['zone']=='S3']['pincode'])
missing = src_s3 - utsf_all

print(f'S3 source: {len(src_s3)}, UTSF all: {len(utsf_all)}')
print(f'Missing from UTSF entirely: {len(missing)}')

for p in missing:
    m_row = df[df['pincode']==p].iloc[0]
    m_entry = master.get(p)
    print(f'  Pincode: {p}')
    print(f'  Source: zone={m_row["zone"]}, oda={m_row["oda"]}, city={m_row["city"]}, state={m_row["state"]}')
    print(f'  Master: {m_entry}')
