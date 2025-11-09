#!/usr/bin/env python3
"""
extract_nbm_csv.py - Extract NBM data from wgrib2 CSV output
"""
import sys
import json
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree
from datetime import datetime

def extract_variable(grib_file, match_pattern, output_csv):
    """Extract a single variable to CSV using wgrib2"""
    import subprocess
    cmd = f"wgrib2 {grib_file} -match '{match_pattern}' -csv {output_csv}"
    subprocess.run(cmd, shell=True, check=True)

def read_wgrib2_csv(csv_file):
    """Read wgrib2 CSV output into a structured format"""
    df = pd.read_csv(csv_file, header=None, names=[
        'ref_time', 'valid_time', 'var', 'level', 'lon', 'lat', 'value'
    ])
    return df

def c2f(kelvin):
    return (kelvin - 273.15) * 1.8 + 32

def ms2mph(ms):
    return ms * 2.23694

def extract_nbm_data(grib_file, zips_data, temp_dir='./temp'):
    """Extract NBM data using wgrib2 CSV output"""
    import os
    import subprocess
    
    os.makedirs(temp_dir, exist_ok=True)
    
    # Variable definitions
    variables = {
        'temp': (':APTMP:2 m above ground:', 'temp.csv', lambda x: round(c2f(x), 1)),
        'rh': (':RH:2 m above ground:', 'rh.csv', lambda x: round(x, 1)),
        'wdir': (':WDIR:10 m above ground:', 'wdir.csv', lambda x: round(x, 1)),
        'wind': (':WIND:10 m above ground:', 'wind.csv', lambda x: round(ms2mph(x), 1)),
        'gust': (':GUST:10 m above ground:', 'gust.csv', lambda x: round(ms2mph(x), 1)),
        'vis': (':VIS:surface:', 'vis.csv', lambda x: round(x * 0.000621371, 2)),
        'cape': (':CAPE:surface:', 'cape.csv', lambda x: round(x, 1))
    }
    
    print(f"Extracting {len(variables)} variables from {grib_file}", file=sys.stderr)
    
    # Extract each variable
    var_data = {}
    valid_time = None
    
    for var_name, (pattern, csv_name, transform) in variables.items():
        csv_path = os.path.join(temp_dir, csv_name)
        print(f"Extracting {var_name}...", file=sys.stderr)
        
        try:
            cmd = f"wgrib2 {grib_file} -match '{pattern}' -csv {csv_path}"
            subprocess.run(cmd, shell=True, check=True, capture_output=True)
            
            df = read_wgrib2_csv(csv_path)
            
            if len(df) == 0:
                print(f"WARNING: No data for {var_name}", file=sys.stderr)
                continue
            
            # Get valid time from first variable
            if valid_time is None:
                valid_time = df['valid_time'].iloc[0]
            
            var_data[var_name] = {
                'df': df,
                'transform': transform
            }
            
            print(f"  Loaded {len(df)} points", file=sys.stderr)
            
        except Exception as e:
            print(f"ERROR extracting {var_name}: {e}", file=sys.stderr)
            continue
    
    if not var_data:
        print("ERROR: No variables extracted", file=sys.stderr)
        return []
    
    # Use first variable's grid for KDTree
    first_var = list(var_data.values())[0]['df']
    grid_lats = first_var['lat'].values
    grid_lons = first_var['lon'].values
    grid_coords = np.column_stack([grid_lats, grid_lons])
    
    print(f"Building KDTree with {len(grid_coords)} grid points...", file=sys.stderr)
    tree = cKDTree(grid_coords)
    
    # Prepare ZIP coordinates
    zip_lats = np.array([z['lat'] for z in zips_data])
    zip_lons = np.array([z['lon'] for z in zips_data])
    
    # Handle longitude wrap (if grid uses 0-360)
    if np.max(grid_lons) > 180:
        zip_lons = np.where(zip_lons < 0, zip_lons + 360, zip_lons)
    
    zip_coords = np.column_stack([zip_lats, zip_lons])
    
    distances, indices = tree.query(zip_coords, k=1)
    valid_mask = distances < 0.03
    n_matched = np.sum(valid_mask)
    
    print(f"Matched {n_matched}/{len(zips_data)} ZIPs", file=sys.stderr)
    
    # Build results
    results = []
    
    for i in range(len(zips_data)):
        if not valid_mask[i]:
            continue
        
        idx = indices[i]
        zip_data = zips_data[i]
        
        record = {
            'zip': zip_data['zip'],
            'valid_time': valid_time
        }
        
        # Extract values for each variable
        for var_name, var_info in var_data.items():
            df = var_info['df']
            transform = var_info['transform']
            
            try:
                value = df.iloc[idx]['value']
                if not np.isnan(value) and value != 9999:
                    if var_name == 'temp':
                        record['temp_f'] = transform(value)
                    elif var_name == 'rh':
                        record['rh_pct'] = transform(value)
                    elif var_name == 'wdir':
                        record['wdir_deg'] = transform(value)
                    elif var_name == 'wind':
                        record['wind_mph'] = transform(value)
                    elif var_name == 'gust':
                        record['gust_mph'] = transform(value)
                    elif var_name == 'vis':
                        record['vis_mi'] = transform(value)
                    elif var_name == 'cape':
                        record['cape'] = transform(value)
            except:
                pass
        
        results.append(record)
    
    print(f"Extracted {len(results)} records", file=sys.stderr)
    
    return results

def main():
    if len(sys.argv) != 4:
        print("Usage: python3 extract_nbm_csv.py <grib2> <zips.json> <output.json>")
        sys.exit(1)
    
    grib_file, zips_json, output_json = sys.argv[1], sys.argv[2], sys.argv[3]
    
    with open(zips_json) as f:
        zips_data = json.load(f)
    
    results = extract_nbm_data(grib_file, zips_data)
    
    with open(output_json, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"Wrote {len(results)} records to {output_json}", file=sys.stderr)

if __name__ == '__main__':
    main()