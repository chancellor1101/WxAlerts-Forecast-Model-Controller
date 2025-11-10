#!/usr/bin/env python3
"""
ingest_nbm_smart_memory_optimized.py - Intelligent NBM ingestion with memory optimization
Only downloads NEW model runs and UPDATED forecast hours
Inserts data IMMEDIATELY to avoid OOM
"""

import os
import sys
import json
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


class SmartNBMIngester:
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
        """
        Determine the latest available NBM cycle.
        NBM runs at 00z, 06z, 12z, 18z and is typically available 3 hours after run time.
        """
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
            
            # Get the most recent run_date/run_hour we've processed
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
            self.log(f"Could not fetch last run (table may not exist): {e}")
            return None
    
    def log_ingestion_run(self, run_date, run_hour, forecast_hours, record_count):
        """Log this ingestion run to the database"""
        try:
            conn = psycopg2.connect(self.db_url)
            cur = conn.cursor()
            
            # Create log table if it doesn't exist
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
            
            # Insert this run
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
            self.log(f"Warning: Could not log ingestion run: {e}")
    
    def determine_forecast_hours_to_process(self, current_run):
        """
        Intelligently determine which forecast hours to process.
        
        Strategy:
        1. If this is a NEW model run → download all forecast hours (f001-f036)
        2. If running within same model cycle → only download SHORT RANGE updates (f001-f006)
           (because near-term forecasts get better/updated, long-range stays same)
        """
        last_run = self.get_last_processed_run()
        
        # First run ever OR new model cycle
        if not last_run or \
           last_run['run_date'] != current_run['run_date'] or \
           last_run['run_hour'] != current_run['run_hour']:
            
            self.log("📥 NEW MODEL RUN detected - downloading full forecast")
            return {
                'reason': 'new_run',
                'hours': [1, 2, 3, 4, 5, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36]
            }
        
        # Same model run, just updating near-term forecasts
        else:
            self.log("🔄 Same model run - updating near-term forecasts only")
            return {
                'reason': 'update',
                'hours': [1, 2, 3, 4, 5, 6]  # Only first 6 hours
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
    
    def extract_variable_csv(self, grib_path, pattern, csv_path):
        """Extract a single variable to CSV using wgrib2"""
        # Use the wgrib2 that is included with this container
        cmd = f"./wgrib2 '{grib_path}' -match '{pattern}' -csv '{csv_path}'"
        
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode != 0:
            raise RuntimeError(f"wgrib2 extraction failed: {result.stderr}")
        
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return None
        
        return csv_path
    
    def read_wgrib2_csv(self, csv_file):
        """Read wgrib2 CSV output into a DataFrame"""
        df = pd.read_csv(csv_file, header=None, names=[
            'ref_time', 'valid_time', 'var', 'level', 'lon', 'lat', 'value'
        ])
        return df
    
    def extract_nbm_data(self, grib_path, zips):
        """Extract NBM data using wgrib2 CSV output - MEMORY OPTIMIZED"""
        
        var_data = {}
        valid_time = None
        
        # Extract each variable
        for var_name, var_config in self.variables.items():
            csv_path = self.tmp_dir / f"{self.job_id}_{var_name}.csv"
            
            try:
                result_path = self.extract_variable_csv(
                    grib_path,
                    var_config['pattern'],
                    csv_path
                )
                
                if not result_path:
                    continue
                
                df = self.read_wgrib2_csv(csv_path)
                
                if len(df) == 0:
                    csv_path.unlink(missing_ok=True)
                    continue
                
                # Get valid time from first variable
                if valid_time is None:
                    valid_time = df['valid_time'].iloc[0]
                
                var_data[var_name] = {
                    'df': df,
                    'config': var_config
                }
                
                # Clean up CSV IMMEDIATELY
                csv_path.unlink(missing_ok=True)
                
            except Exception as e:
                csv_path.unlink(missing_ok=True)
                continue
        
        if not var_data:
            return []
        
        # Use first variable's grid for KDTree
        first_var = list(var_data.values())[0]['df']
        grid_lats = first_var['lat'].values
        grid_lons = first_var['lon'].values
        grid_coords = np.column_stack([grid_lats, grid_lons])
        
        tree = cKDTree(grid_coords)
        
        # Prepare ZIP coordinates
        zip_lats = np.array([z['lat'] for z in zips])
        zip_lons = np.array([z['lon'] for z in zips])
        
        # Handle longitude wrap (if grid uses 0-360)
        if np.max(grid_lons) > 180:
            zip_lons = np.where(zip_lons < 0, zip_lons + 360, zip_lons)
        
        zip_coords = np.column_stack([zip_lats, zip_lons])
        
        distances, indices = tree.query(zip_coords, k=1)
        valid_mask = distances < 0.03
        
        # Build results
        results = []
        
        for i in range(len(zips)):
            if not valid_mask[i]:
                continue
            
            idx = int(indices[i])  # Convert numpy.int64 to Python int
            zip_data = zips[i]
            
            record = {
                'zip': zip_data['zip'],
                'valid_time': valid_time
            }
            
            # Extract values for each variable
            for var_name, var_info in var_data.items():
                df = var_info['df']
                config = var_info['config']
                transform = config['transform']
                field = config['field']
                
                try:
                    value = float(df.iloc[idx]['value'])  # Convert to Python float
                    if not np.isnan(value) and value != 9999:
                        record[field] = transform(value)
                except:
                    pass
            
            results.append(record)
        
        # Cleanup DataFrames
        del var_data
        gc.collect()
        
        return results
    
    def insert_to_database(self, records):
        """Bulk insert records to database"""
        if not records:
            self.log("No records to insert")
            return
        
        conn = psycopg2.connect(self.db_url)
        cur = conn.cursor()
        
        # Prepare data for bulk insert
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
            for r in records
        ]
        
        # Upsert query
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
        
        # Insert in chunks
        chunk_size = 5000
        for i in range(0, len(values), chunk_size):
            chunk = values[i:i + chunk_size]
            execute_values(cur, query, chunk)
            conn.commit()
        
        cur.close()
        conn.close()
        
        self.log(f"✓ Database updated with {len(records)} records")
    
    def process_forecast_hour(self, run_date, run_hour, fhr, zips):
        """Process a single forecast hour - INSERT IMMEDIATELY"""
        fhr_str = f"{fhr:03d}"
        
        url = f"https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{run_date}/{run_hour}/core/blend.t{run_hour}z.core.f{fhr_str}.co.grib2"
        grib_path = self.tmp_dir / f"{self.job_id}_f{fhr_str}.grib2"
        
        try:
            # Download
            start_time = time.time()
            grib_size = self.download_grib(url, grib_path)
            download_time = time.time() - start_time
            self.log(f"  ✓ Downloaded f{fhr_str}: {grib_size:.1f} MB in {download_time:.1f}s")
            
            # Extract data directly from GRIB using CSV
            start_time = time.time()
            records = self.extract_nbm_data(grib_path, zips)
            process_time = time.time() - start_time
            self.log(f"  ✓ Processed f{fhr_str} in {process_time:.1f}s - {len(records)} records")
            
            # INSERT IMMEDIATELY - don't accumulate in memory
            if records:
                start_time = time.time()
                self.insert_to_database(records)
                insert_time = time.time() - start_time
                self.log(f"  ✓ Inserted {len(records)} records in {insert_time:.1f}s")
            
            # Cleanup GRIB IMMEDIATELY
            grib_path.unlink()
            
            # Force garbage collection
            record_count = len(records)
            del records
            gc.collect()
            
            return record_count
            
        except Exception as e:
            self.log(f"  ✗ Failed f{fhr_str}: {str(e)}")
            # Cleanup on error
            if grib_path.exists():
                grib_path.unlink()
            return 0

    def run(self):
        """Main ingestion process with intelligent update strategy"""
        try:
            # Get latest cycle
            cycle = self.get_latest_nbm_cycle()
            self.log(f"Latest NBM run: {cycle['run_date']} {cycle['run_hour']}Z")
            
            # Determine what to download
            forecast_plan = self.determine_forecast_hours_to_process(cycle)
            forecast_hours = forecast_plan['hours']
            
            self.log(f"Strategy: {forecast_plan['reason']}")
            self.log(f"Will process {len(forecast_hours)} forecast hours: {forecast_hours}")
            
            # Load ZIP codes ONCE
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
                self.log(f"Total records inserted: {total_records}")
                
                # Force garbage collection between hours
                gc.collect()
            
            # Log this run
            self.log_ingestion_run(
                cycle['run_date'],
                cycle['run_hour'],
                forecast_hours,
                total_records
            )
            
            self.log(f"✓ Ingestion completed - {total_records} total records")
            return 0
            
        except Exception as e:
            self.log(f"✗ ERROR: {str(e)}")
            import traceback
            traceback.print_exc()
            return 1


if __name__ == '__main__':
    ingester = SmartNBMIngester()
    exit_code = ingester.run()
    sys.exit(exit_code)