import csv
from datetime import datetime
from pathlib import Path

def parse_date(date_str):
    if not date_str:
        return None
    try:
        parts = date_str.split('/')
        if len(parts) == 3:
            m, d, y = parts
            if len(y) == 2:
                y = '20' + y if int(y) < 50 else '19' + y
            return datetime(int(y), int(m), int(d))
    except ValueError:
        pass
    return None

def main():
    input_file = Path('backend/data/raw/lookups/2025-april-psc.csv')
    output_file = Path('backend/data/raw/lookups/Simplified_PSC_Lookup.csv')
    
    now = datetime.now()
    
    pscs = {} # psc_code -> row_dict
    
    with open(input_file, mode='r', encoding='utf-8-sig') as infile:
        reader = csv.DictReader(infile)
        for row in reader:
            psc_code = row.get('PSC CODE', '').strip()
            if not psc_code:
                continue
                
            end_date_str = row.get('END DATE', '').strip()
            end_date = parse_date(end_date_str)
            
            if end_date and end_date < now:
                # Expired
                continue
                
            # If not expired, and we already have one, we can choose to keep the existing one 
            # or replace it if this one has a more recent start date.
            # Usually the active one doesn't have an end date.
            # We'll just keep the first non-expired one we see, which is usually the current active one.
            if psc_code not in pscs:
                pscs[psc_code] = {
                    'psc_code': psc_code,
                    'psc_name': row.get('PRODUCT AND SERVICE CODE NAME', ''),
                    'psc_includes': row.get('PRODUCT AND SERVICE CODE INCLUDES', ''),
                    'psc_level_1_category': row.get('Level 1 Category', '')
                }

    # Write output
    with open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
        fieldnames = ['psc_code', 'psc_name', 'psc_includes', 'psc_level_1_category']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        # Write out in sorted order of psc_code for consistency
        for psc_code in sorted(pscs.keys()):
            writer.writerow(pscs[psc_code])
            
    print(f"Generated {output_file} successfully.")
    print(f"Total entries: {len(pscs)}")

if __name__ == '__main__':
    main()
