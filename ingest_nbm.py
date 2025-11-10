#!/usr/bin/env python3
"""
ingest_nbm_streaming.py - Ultra-low memory NBM ingestion
Streams data in mini-batches of 100 records to minimize memory usage
"""

import os
import sys
import gc
import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
import redis as redis_lib
import pandas as pd
import numpy as np
from scipy.spatial import cKDTree


class StreamingNBMIngester:
    def __init__(self):
        self.job_id = f"nbm_{int(time.time())}"
        self.tmp_dir = Path(os.environ.get('TMP_DIR', '/tmp/nbm'))
        self.tmp_dir.mkdir(exist_ok=True, parents=True)
        
        # Database configuration
        self.db_url = os.environ.get('DATABASE_URL')
        if not self.db_url:
            raise ValueError("DATABASE_URL environment variable required")
        
        # Redis configuration (optional)
        self.redis_url = os.environ.get('REDIS_URL')
        self.redis_client = None
        if self.redis_url:
            try:
                self.redis_client = redis_lib.from_url(self.redis_url)
                self.redis_client.ping()
                self.log("✓ Connected to Redis")
            except Exception as e:
                self.log(f"⚠ Redis not available: {e}")
        
        # Configuration
        self.hot_zips = int(os.environ.get('HOT_ZIPS', '5000'))
        self.batch_size = 100  # Insert every 100 records
        
        # Variable definitions
        self.variables = {
            'temp': {
                'pattern': ':APTMP:2 m above ground:',
                'field': 'temp_f',
                'transform': lambda x: round(self.c2f(x), 1)
            },
            'rh': {
                'pattern': ':RH:2 m above ground:',
                'field': 'rh_pct',
                'transform': lambda x: round(x, 1)
            },
            'wdir': {
                'pattern': ':WDIR:10 m above ground:',
                'field': 'wdir_deg',
                'transform': lambda x: round(x, 1)
            },
            'wind': {
                'pattern': ':WIND:10 m above ground:',
                'field': 'wind_mph',
                'transform': lambda x: round(self.ms2mph(x), 1)
            },
            'gust': {
                'pattern': ':GUST:10 m above ground:',
                'field': 'gust_mph',
                'transform': lambda x: round(self.ms2mph(x), 1)
            },
            'vis': {
                'pattern': ':VIS:surface:',
                'field': 'vis_mi',
                'transform': lambda x: round(x * 0.000621371, 2)
            },
            'cape': {
                'pattern': ':CAPE:surface:',
                'field': 'cape',
                'transform': lambda x: round(x, 1)
            }
        }
        
        self.log(f"Starting NBM ingestion job {self.job_id}")
    
    def log(self, message):
        """Log with timestamp"""
        timestamp = datetime.utcnow().isoformat() + 'Z'
        print(f"[{self.job_id}] {timestamp} {message}", flush=True)
    
    @staticmethod
    def c2f(kelvin):
        """Convert Kelvin to Fahrenheit"""
        return (kelvin - 273.15) * 1.8 + 32
    
    @staticmethod
    def ms2mph(ms):
        """Convert m/s to mph"""
        return ms * 2.23694
    
    def get_latest_nbm_cycle(self):
        """Determine the latest available NBM cycle"""
        now = datetime.utcnow()
        cycles = [0, 6, 12, 18]
        run_hour = cycles[-1]
        
        for h in cycles:
            if now.hour >= h + 3:
                run_hour = h
            else:
                break
        
        run_date = now.date()
        if now.hour < run_hour + 3:
            run_date = run_date - timedelta(days=1)
            run_hour = cycles[-1]
        
        return {
            'run_hour': f"{run_hour:02d}",
            'run_date': run_date.strftime('%Y%m%d'),
            'run_datetime': datetime.combine(run_date, datetime.min.time()).replace(hour=run_hour)
        }
    
    def get_last_processed_run(self):
        """Get the last processed NBM run from database"""
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            
            cur.execute("""
                SELECT run_date, run_hour 
                FROM nbm_ingestion_log 
                ORDER BY run_date DESC, run_hour DESC 
                LIMIT 1
            """)
            
            result = cur.fetchone()
            cur.close()
            conn.close()
            
            if result:
                return {
                    'run_date': result[0],
                    'run_hour': result[1]
                }
            
            return None
            
        except Exception as e:
            self.log(f"Could not fetch last run: {e}")
            return None
    
    def log_ingestion_run(self, run_date, run_hour, forecast_hours, record_count):
        """Log this ingestion run to the database"""
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS nbm_ingestion_log (
                    id SERIAL PRIMARY KEY,
                    run_date VARCHAR(8) NOT NULL,
                    run_hour VARCHAR(2) NOT NULL,
                    ingested_at TIMESTAMPTZ DEFAULT NOW(),
                    forecast_hours INTEGER[],
                    record_count INTEGER,
                    UNIQUE(run_date, run_hour)
                )
            """)
            
            cur.execute("""
                INSERT INTO nbm_ingestion_log 
                    (run_date, run_hour, forecast_hours, record_count)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (run_date, run_hour) 
                DO UPDATE SET 
                    ingested_at = NOW(),
                    forecast_hours = EXCLUDED.forecast_hours,
                    record_count = EXCLUDED.record_count
            """, (run_date, run_hour, forecast_hours, record_count))
            
            conn.commit()
            cur.close()
            conn.close()
            
        except Exception as e:
            self.log(f"Warning: Could not log run: {e}")
    
    def determine_forecast_hours_to_process(self, current_run):
        """Determine which forecast hours to process"""
        last_run = self.get_last_processed_run()
        
        if not last_run or \
           last_run['run_date'] != current_run['run_date'] or \
           last_run['run_hour'] != current_run['run_hour']:
            
            self.log("📥 NEW MODEL RUN - downloading full forecast")
            return {
                'reason': 'new_run',
                'hours': [1, 2, 3, 4, 5, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36]
            }
        else:
            self.log("🔄 Same run - updating near-term only")
            return {
                'reason': 'update',
                'hours': [1, 2, 3, 4, 5, 6]
            }
    
    def load_zip_codes(self):
        """Load ZIP code data from database"""
        self.log("Loading ZIP codes from database...")
        
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        
        cur.execute("""
            SELECT code, city, "stateId",
                   ST_X(centroid::geometry) as lon,
                   ST_Y(centroid::geometry) as lat
            FROM zipcodes
            ORDER BY code
        """)
        
        zips = []
        for row in cur.fetchall():
            zips.append({
                'zip': row[0],
                'city': row[1],
                'state': row[2],
                'lon': row[3],
                'lat': row[4]
            })
        
        cur.close()
        conn.close()
        
        self.log(f"✓ Loaded {len(zips)} ZIP codes")
        return zips
    
    def download_grib(self, url, output_path):
        """Download GRIB file from URL"""
        import urllib.request
        
        with urllib.request.urlopen(url, timeout=120) as response:
            data = response.read()
            with open(output_path, 'wb') as f:
                f.write(data)
        
        size_mb = len(data) / (1024 * 1024)
        return size_mb
    
    def insert_batch(self, batch, conn, cur):
        """Insert a batch of records"""
        if not batch:
            return
        
        values = [
            (
                r['zip'],
                r['valid_time'],
                r.get('temp_f'),
                r.get('rh_pct'),
                r.get('wind_mph'),
                r.get('wdir_deg'),
                r.get('gust_mph'),
                r.get('vis_mi'),
                r.get('cape')
            )
            for r in batch
        ]
        
        query = """
            INSERT INTO forecasts (
                zip, valid_time, temp_f, rh_pct, wind_mph, 
                wdir_deg, gust_mph, vis_mi, cape
            )
            VALUES %s
            ON CONFLICT (zip, valid_time) 
            DO UPDATE SET
                temp_f = EXCLUDED.temp_f,
                rh_pct = EXCLUDED.rh_pct,
                wind_mph = EXCLUDED.wind_mph,
                wdir_deg = EXCLUDED.wdir_deg,
                gust_mph = EXCLUDED.gust_mph,
                vis_mi = EXCLUDED.vis_mi,
                cape = EXCLUDED.cape
        """
        
        execute_values(cur, query, values)
        conn.commit()
    
    def extract_variable_csv(self, grib_path, pattern, csv_path):
        """Extract a single variable to CSV using wgrib2"""
        cmd = f"wgrib2 '{grib_path}' -match '{pattern}' -csv '{csv_path}'"
        
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"wgrib2 failed: {result.stderr}")
        
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return None
        
        return csv_path
    
    def read_wgrib2_csv(self, csv_file):
        """Read wgrib2 CSV output"""
        df = pd.read_csv(csv_file, header=None, names=[
            'ref_time', 'valid_time', 'var', 'level', 'lon', 'lat', 'value'
        ])
        return df
    
    def extract_nbm_data_streaming(self, grib_path, zips, fhr_str):
        """
        ULTRA STREAMING extraction - minimal memory footprint
        Uses dict lookups instead of DataFrames
        """
        
        self.log(f"    Extracting variables from GRIB...")
        
        # Step 1: Extract all variables to CSV (files on disk)
        var_csv_paths = {}
        
        for var_name, var_config in self.variables.items():
            csv_path = self.tmp_dir / f"{self.job_id}_{var_name}.csv"
            
            try:
                result_path = self.extract_variable_csv(
                    grib_path,
                    var_config['pattern'],
                    csv_path
                )
                
                if result_path and csv_path.exists():
                    var_csv_paths[var_name] = {
                        'path': csv_path,
                        'config': var_config
                    }
                
            except Exception as e:
                csv_path.unlink(missing_ok=True)
                continue
        
        if not var_csv_paths:
            self.log(f"    ⚠ No variables extracted")
            return 0
        
        self.log(f"    Found {len(var_csv_paths)} variables")
        
        # Step 2: Read FIRST variable ONLY to build spatial index
        first_var_name = list(var_csv_paths.keys())[0]
        first_csv = var_csv_paths[first_var_name]['path']
        
        self.log(f"    Building spatial index...")
        
        grid_coords = []
        valid_time = None
        
        # Read first CSV line by line - only keep coords
        with open(first_csv, 'r') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) >= 7:
                    if valid_time is None:
                        valid_time = parts[1]
                    lon = float(parts[4])
                    lat = float(parts[5])
                    grid_coords.append([lat, lon])
        
        grid_coords = np.array(grid_coords)
        tree = cKDTree(grid_coords)
        
        self.log(f"    Grid has {len(grid_coords)} points")
        
        # Prepare ZIP coordinates
        zip_lats = np.array([z['lat'] for z in zips])
        zip_lons = np.array([z['lon'] for z in zips])
        
        if np.max(grid_coords[:, 1]) > 180:
            zip_lons = np.where(zip_lons < 0, zip_lons + 360, zip_lons)
        
        zip_coords = np.column_stack([zip_lats, zip_lons])
        
        # Query tree
        distances, indices = tree.query(zip_coords, k=1)
        valid_mask = distances < 0.03
        
        matched_count = np.sum(valid_mask)
        self.log(f"    Matched {matched_count}/{len(zips)} ZIPs")
        
        # Step 3: For each variable, create minimal lookup
        self.log(f"    Reading variable values...")
        
        var_lookups = {}
        
        for var_name, var_info in var_csv_paths.items():
            csv_path = var_info['path']
            config = var_info['config']
            transform = config['transform']
            
            # Read CSV line by line, store only index->value
            value_lookup = {}
            
            with open(csv_path, 'r') as f:
                idx = 0
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) >= 7:
                        try:
                            value = float(parts[6])
                            if not np.isnan(value) and value != 9999:
                                value_lookup[idx] = transform(value)
                        except:
                            pass
                        idx += 1
            
            var_lookups[var_name] = {
                'lookup': value_lookup,
                'field': config['field']
            }
            
            # Delete CSV immediately
            csv_path.unlink(missing_ok=True)
            self.log(f"      {var_name}: {len(value_lookup)} valid values")
        
        # Cleanup
        del grid_coords, tree
        gc.collect()
        
        self.log(f"    Streaming inserts in batches of {self.batch_size}...")
        
        # Step 4: Stream through ZIPs and insert
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        
        batch = []
        total_inserted = 0
        
        for i in range(len(zips)):
            if not valid_mask[i]:
                continue
            
            idx = int(indices[i])
            zip_data = zips[i]
            
            record = {
                'zip': zip_data['zip'],
                'valid_time': valid_time
            }
            
            # Extract values from lookups
            for var_name, var_info in var_lookups.items():
                lookup = var_info['lookup']
                field = var_info['field']
                
                if idx in lookup:
                    record[field] = lookup[idx]
            
            batch.append(record)
            
            # INSERT BATCH
            if len(batch) >= self.batch_size:
                self.insert_batch(batch, conn, cur)
                total_inserted += len(batch)
                self.log(f"    → Inserted {total_inserted} records (f{fhr_str})")
                
                batch = []
                gc.collect()
        
        # Insert remaining
        if batch:
            self.insert_batch(batch, conn, cur)
            total_inserted += len(batch)
            self.log(f"    → Inserted {total_inserted} records (f{fhr_str}) [final]")
        
        cur.close()
        conn.close()
        
        # Final cleanup
        del var_lookups
        gc.collect()
        
        return total_inserted
    
    def process_forecast_hour(self, run_date, run_hour, fhr, zips):
        """Process a single forecast hour with streaming inserts"""
        fhr_str = f"{fhr:03d}"
        
        url = f"https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{run_date}/{run_hour}/core/blend.t{run_hour}z.core.f{fhr_str}.co.grib2"
        grib_path = self.tmp_dir / f"{self.job_id}_f{fhr_str}.grib2"
        
        try:
            # Download
            start_time = time.time()
            grib_size = self.download_grib(url, grib_path)
            download_time = time.time() - start_time
            self.log(f"  ✓ Downloaded f{fhr_str}: {grib_size:.1f} MB in {download_time:.1f}s")
            
            # Extract and insert in streaming fashion
            start_time = time.time()
            record_count = self.extract_nbm_data_streaming(grib_path, zips, fhr_str)
            process_time = time.time() - start_time
            self.log(f"  ✓ Completed f{fhr_str} in {process_time:.1f}s - {record_count} total records")
            
            # Delete GRIB immediately
            grib_path.unlink()
            
            # Force GC
            gc.collect()
            
            return record_count
            
        except Exception as e:
            self.log(f"  ✗ Failed f{fhr_str}: {str(e)}")
            if grib_path.exists():
                grib_path.unlink()
            return 0

    def run(self):
        """Main ingestion process"""
        try:
            # Get latest cycle
            cycle = self.get_latest_nbm_cycle()
            self.log(f"Latest NBM run: {cycle['run_date']} {cycle['run_hour']}Z")
            
            # Determine what to download
            forecast_plan = self.determine_forecast_hours_to_process(cycle)
            forecast_hours = forecast_plan['hours']
            
            self.log(f"Strategy: {forecast_plan['reason']}")
            self.log(f"Will process {len(forecast_hours)} hours: {forecast_hours}")
            
            # Load ZIP codes
            zips = self.load_zip_codes()
            
            # Process each forecast hour
            total_records = 0
            
            for fhr in forecast_hours:
                record_count = self.process_forecast_hour(
                    cycle['run_date'],
                    cycle['run_hour'],
                    fhr,
                    zips
                )
                total_records += record_count
                self.log(f"TOTAL: {total_records} records inserted so far")
                
                gc.collect()
            
            # Log this run
            self.log_ingestion_run(
                cycle['run_date'],
                cycle['run_hour'],
                forecast_hours,
                total_records
            )
            
            self.log(f"✓ Ingestion completed - {total_records} records")
            return 0
            
        except Exception as e:
            self.log(f"✗ ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return 1


if __name__ == '__main__':
    ingester = StreamingNBMIngester()
    exit_code = ingester.run()
    sys.exit(exit_code)