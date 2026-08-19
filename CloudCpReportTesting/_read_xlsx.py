import zipfile, xml.etree.ElementTree as ET, csv, sys

path = r'c:\Cloud_cp Testing\cloudcp-phase-wise-testing\docs\testcaselist.xlsx'
z = zipfile.ZipFile(path)

ns = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}

# shared strings
sst = []
with z.open('xl/sharedStrings.xml') as f:
    tree = ET.parse(f)
    for si in tree.getroot().findall('m:si', ns):
        texts = si.findall('.//m:t', ns)
        sst.append(''.join(t.text or '' for t in texts))

def col_to_idx(cellref):
    letters = ''.join(c for c in cellref if c.isalpha())
    idx = 0
    for c in letters:
        idx = idx * 26 + (ord(c.upper()) - ord('A') + 1)
    return idx - 1

with z.open('xl/worksheets/sheet1.xml') as f:
    tree = ET.parse(f)
root = tree.getroot()
sheetData = root.find('m:sheetData', ns)

rows = []
maxcol = 0
for row in sheetData.findall('m:row', ns):
    rowvals = {}
    for c in row.findall('m:c', ns):
        ref = c.get('r')
        idx = col_to_idx(ref)
        maxcol = max(maxcol, idx)
        t = c.get('t')
        v = c.find('m:v', ns)
        val = v.text if v is not None else ''
        if t == 's' and val != '':
            val = sst[int(val)]
        elif t == 'inlineStr':
            isnode = c.find('m:is', ns)
            if isnode is not None:
                val = ''.join(tn.text or '' for tn in isnode.findall('.//m:t', ns))
        rowvals[idx] = val
    rows.append(rowvals)

with open(r'c:\Cloud_cp Testing\cloudcp-phase-wise-testing\_testcaselist_dump.csv', 'w', newline='', encoding='utf-8') as out:
    w = csv.writer(out)
    for rowvals in rows:
        line = [rowvals.get(i, '') for i in range(maxcol + 1)]
        w.writerow(line)

print("rows:", len(rows), "cols:", maxcol + 1)
