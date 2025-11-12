#!/usr/bin/env python3
"""
ingest_nbm_streaming_optimized.py - Ultra-high performance NBM ingestion
Key optimizations:
- Parallel variable processing with ThreadPoolExecutor
- PostgreSQL COPY for 5-10x faster bulk inserts
- Redis caching for ZIP coordinates
- Connection pooling for database efficiency
- Parallel forecast hour downloads
- NumPy-accelerated CSV parsing
"""

import os
import sys
import gc
import time
import json
import subprocess
import threading
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import redis as redis_lib
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
        
        # Connection pool
        self.conn_pool = None
        self.init_connection_pool()
        
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
        
        # Configuration - optimized for speed
        self.hot_zips = int(os.environ.get('HOT_ZIPS', '5000'))
        self.batch_size = 10000  # Larger batches with COPY
        self.max_parallel_vars = 4  # Parallel variable processing
        self.max_parallel_hours = 3  # Parallel forecast hour downloads
        
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
        
        self.log(f"Starting optimized NBM ingestion job {self.job_id}")
    
    def init_connection_pool(self):
        """Initialize PostgreSQL connection pool"""
        try:
            self.conn_pool = pg_pool.SimpleConnectionPool(
                1, 10,  # min/max connections
                self.db_url
            )
            self.log("✓ Initialized connection pool")
        except Exception as e:
            self.log(f"✗ Failed to create connection pool: {e}")
            raise
    
    def get_db_connection(self):
        """Get connection from pool"""
        return self.conn_pool.getconn()
    
    def return_db_connection(self, conn):
        """Return connection to pool"""
        if self.conn_pool:
            self.conn_pool.putconn(conn)
    
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
            conn = self.get_db_connection()
            cur = conn.cursor()
            
            cur.execute("""
                SELECT run_date, run_hour 
                FROM nbm_ingestion_log 
                ORDER BY run_date DESC, run_hour DESC 
                LIMIT 1
            """)
            
            result = cur.fetchone()
            cur.close()
            self.return_db_connection(conn)
            
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
            conn = self.get_db_connection()
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
            self.return_db_connection(conn)
            
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
        """Load ZIP code data with Redis caching"""
        cache_key = "nbm_zip_coords_v1"
        
        # Try Redis cache first
        if self.redis_client:
            try:
                cached = self.redis_client.get(cache_key)
                if cached:
                    zips = json.loads(cached)
                    self.log(f"✓ Loaded {len(zips)} ZIP codes from Redis cache")
                    return zips
            except Exception as e:
                self.log(f"Cache miss: {e}")
        
        # Load from database
        self.log("Loading ZIP codes from database...")
        
        conn = self.get_db_connection()
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
        self.return_db_connection(conn)
        
        self.log(f"✓ Loaded {len(zips)} ZIP codes from database")
        
        # Cache in Redis for 24 hours
        if self.redis_client:
            try:
                self.redis_client.setex(cache_key, 86400, json.dumps(zips))
                self.log("✓ Cached ZIP codes in Redis")
            except Exception as e:
                self.log(f"⚠ Could not cache in Redis: {e}")
        
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
    
    def bulk_insert_batch_copy(self, batch, conn, cur):
        """Insert batch using PostgreSQL COPY for maximum speed"""
        if not batch:
            return
        
        # Generate unique temp table name for this batch
        temp_table = f"temp_forecasts_{int(time.time() * 1000000)}"
        
        # Create temporary table
        cur.execute(f"""
            CREATE TEMP TABLE {temp_table} (
                zip VARCHAR(5),
                valid_time TIMESTAMPTZ,
                temp_f NUMERIC(5,1),
                rh_pct NUMERIC(5,1),
                wind_mph NUMERIC(5,1),
                wdir_deg NUMERIC(5,1),
                gust_mph NUMERIC(5,1),
                vis_mi NUMERIC(6,2),
                cape NUMERIC(7,1)
            )
        """)
        
        # Prepare CSV data
        csv_data = StringIO()
        for r in batch:
            temp = r.get('temp_f', '')
            rh = r.get('rh_pct', '')
            wind = r.get('wind_mph', '')
            wdir = r.get('wdir_deg', '')
            gust = r.get('gust_mph', '')
            vis = r.get('vis_mi', '')
            cape = r.get('cape', '')
            
            null_val = '\\N'
            csv_data.write(
                f"{r['zip']}\t{r['valid_time']}\t"
                f"{temp if temp != '' else null_val}\t"
                f"{rh if rh != '' else null_val}\t"
                f"{wind if wind != '' else null_val}\t"
                f"{wdir if wdir != '' else null_val}\t"
                f"{gust if gust != '' else null_val}\t"
                f"{vis if vis != '' else null_val}\t"
                f"{cape if cape != '' else null_val}\n"
            )
        
        csv_data.seek(0)
        
        # COPY into temp table
        cur.copy_expert(
            f"""
            COPY {temp_table} (
                zip, valid_time, temp_f, rh_pct, wind_mph, 
                wdir_deg, gust_mph, vis_mi, cape
            ) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')
            """,
            csv_data
        )
        
        # Upsert from temp table to main table
        cur.execute(f"""
            INSERT INTO forecasts (
                zip, valid_time, temp_f, rh_pct, wind_mph, 
                wdir_deg, gust_mph, vis_mi, cape
            )
            SELECT * FROM {temp_table}
            ON CONFLICT (zip, valid_time) 
            DO UPDATE SET
                temp_f = EXCLUDED.temp_f,
                rh_pct = EXCLUDED.rh_pct,
                wind_mph = EXCLUDED.wind_mph,
                wdir_deg = EXCLUDED.wdir_deg,
                gust_mph = EXCLUDED.gust_mph,
                vis_mi = EXCLUDED.vis_mi,
                cape = EXCLUDED.cape
        """)
        
        # Drop the temp table
        cur.execute(f"DROP TABLE {temp_table}")
        
        conn.commit()
    
    def extract_variable_csv(self, grib_path, pattern, csv_path):
        """Extract a single variable to CSV using wgrib2"""
        cmd = f"./wgrib2 '{grib_path}' -match '{pattern}' -csv '{csv_path}'"
        
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
    
    def build_spatial_mapping_fast(self, first_csv_path, zips):
        """
        Build grid-to-ZIP mapping with NumPy-accelerated parsing.
        Returns: (grid_to_zip_map, valid_time)
        """
        self.log("    Building spatial mapping (NumPy-accelerated)...")
        
        try:
            # Use NumPy for fast CSV loading
            # Read first line for valid_time
            with open(first_csv_path, 'r') as f:
                first_line = f.readline().strip().split(',')
                valid_time = first_line[1] if len(first_line) > 1 else None
            
            # Load grid coordinates with NumPy (much faster)
            grid_data = np.loadtxt(
                first_csv_path, 
                delimiter=',',
                usecols=(4, 5),  # lon, lat columns
                dtype=np.float32,
                skiprows=0
            )
            
            # grid_data is [lon, lat], we need [lat, lon] for cKDTree
            grid_coords = grid_data[:, [1, 0]]
            
        except Exception as e:
            self.log(f"    NumPy parsing failed, falling back: {e}")
            # Fallback to line-by-line
            grid_coords = []
            valid_time = None
            
            with open(first_csv_path, 'r') as f:
                for line in f:
                    parts = line.strip().split(',')
                    if len(parts) < 7:
                        continue
                    if valid_time is None:
                        valid_time = parts[1]
                    lat = float(parts[5])
                    lon = float(parts[4])
                    grid_coords.append([lat, lon])
            
            grid_coords = np.array(grid_coords, dtype=np.float32)
        
        tree = cKDTree(grid_coords)
        
        # Prepare ZIP coordinates
        zip_coords = np.column_stack([
            np.array([z['lat'] for z in zips], dtype=np.float32),
            np.array([z['lon'] for z in zips], dtype=np.float32)
        ])
        
        # Handle longitude wrapping if needed
        if np.max(grid_coords[:, 1]) > 180:
            zip_coords[:, 1] = np.where(zip_coords[:, 1] < 0, 
                                        zip_coords[:, 1] + 360, 
                                        zip_coords[:, 1])
        
        # Find nearest grid point for each ZIP
        distances, grid_indices = tree.query(zip_coords, k=1)
        
        # Build reverse mapping: grid_idx -> zip_code
        grid_to_zip = {}
        matched_count = 0
        
        for zip_idx, (grid_idx, distance) in enumerate(zip(grid_indices, distances)):
            if distance < 0.03:  # ~3km threshold
                grid_to_zip[grid_idx] = zips[zip_idx]['zip']
                matched_count += 1
        
        self.log(f"    Mapped {matched_count}/{len(zips)} ZIPs to {len(grid_coords)} grid points")
        
        # Clean up
        del grid_coords, tree, zip_coords, grid_indices, distances
        gc.collect()
        
        return grid_to_zip, valid_time
    
    def process_variable_parallel(self, var_name, var_info, grid_to_zip, valid_time, records, records_lock):
        """Process a single variable in parallel (thread-safe)"""
        path = var_info['path']
        field = var_info['config']['field']
        transform = var_info['config']['transform']
        
        # Process locally first, then merge
        local_updates = {}
        
        with open(path, 'r') as f:
            for grid_idx, line in enumerate(f):
                parts = line.strip().split(',')
                if len(parts) < 7:
                    continue
                
                # Direct lookup
                zip_code = grid_to_zip.get(grid_idx)
                if zip_code is None:
                    continue
                
                # Transform and store
                try:
                    value = float(parts[6])
                    if value == 9999 or np.isnan(value):
                        continue
                    local_updates[zip_code] = transform(value)
                except Exception:
                    continue
        
        # Merge into shared records dict with lock
        with records_lock:
            for zip_code, value in local_updates.items():
                if zip_code not in records:
                    records[zip_code] = {
                        'zip': zip_code,
                        'valid_time': valid_time
                    }
                records[zip_code][field] = value
        
        # Delete CSV immediately
        path.unlink(missing_ok=True)
        
        return var_name, len(local_updates)
    
    def extract_nbm_data_parallel(self, grib_path, zips, fhr_str):
        """
        Parallel extraction with all optimizations:
        - Pre-computed spatial mapping
        - Parallel variable processing
        - COPY-based bulk insert
        """
        self.log(f"    Extracting variables from GRIB...")
        
        # Step 1: Extract all variables to CSV
        var_csv_paths = {}
        for var_name, var_config in self.variables.items():
            csv_path = self.tmp_dir / f"{self.job_id}_{var_name}_{fhr_str}.csv"
            try:
                result_path = self.extract_variable_csv(grib_path, var_config['pattern'], csv_path)
                if result_path and csv_path.exists():
                    var_csv_paths[var_name] = {
                        'path': csv_path,
                        'config': var_config
                    }
            except Exception:
                csv_path.unlink(missing_ok=True)
                continue
        
        if not var_csv_paths:
            self.log("    ⚠ No variables extracted")
            return 0
        
        self.log(f"    Found {len(var_csv_paths)} variables")
        
        # Step 2: Build spatial mapping ONCE using first variable
        first_var_name = list(var_csv_paths.keys())[0]
        first_csv = var_csv_paths[first_var_name]['path']
        
        grid_to_zip, valid_time = self.build_spatial_mapping_fast(first_csv, zips)
        
        # Step 3: Process all variables IN PARALLEL
        records = {}
        records_lock = threading.Lock()
        
        self.log(f"    Processing {len(var_csv_paths)} variables in parallel...")
        
        with ThreadPoolExecutor(max_workers=self.max_parallel_vars) as executor:
            futures = {
                executor.submit(
                    self.process_variable_parallel,
                    var_name,
                    var_info,
                    grid_to_zip,
                    valid_time,
                    records,
                    records_lock
                ): var_name
                for var_name, var_info in var_csv_paths.items()
            }
            
            for future in as_completed(futures):
                var_name = futures[future]
                try:
                    result_name, update_count = future.result()
                    self.log(f"    ✓ Processed {result_name} ({update_count} updates)")
                except Exception as e:
                    self.log(f"    ✗ Failed {var_name}: {e}")
        
        # Step 4: Bulk insert with COPY
        self.log(f"    Inserting {len(records)} records with COPY...")
        
        conn = self.get_db_connection()
        cur = conn.cursor()
        
        batch = list(records.values())
        
        # Insert in chunks
        chunk_size = self.batch_size
        total_inserted = 0
        
        for i in range(0, len(batch), chunk_size):
            chunk = batch[i:i + chunk_size]
            self.bulk_insert_batch_copy(chunk, conn, cur)
            total_inserted += len(chunk)
            if i + chunk_size < len(batch):
                self.log(f"    → Inserted {total_inserted}/{len(batch)} records...")
        
        self.log(f"    ✓ Inserted all {total_inserted} records")
        
        cur.close()
        self.return_db_connection(conn)
        
        # Clean up
        del records, grid_to_zip, batch
        gc.collect()
        
        return total_inserted

    def process_forecast_hour(self, run_date, run_hour, fhr, zips):
        """Process a single forecast hour with all optimizations"""
        fhr_str = f"{fhr:03d}"
        
        url = f"https://noaa-nbm-grib2-pds.s3.amazonaws.com/blend.{run_date}/{run_hour}/core/blend.t{run_hour}z.core.f{fhr_str}.co.grib2"
        grib_path = self.tmp_dir / f"{self.job_id}_f{fhr_str}.grib2"
        
        try:
            # Download
            start_time = time.time()
            grib_size = self.download_grib(url, grib_path)
            download_time = time.time() - start_time
            self.log(f"  ✓ Downloaded f{fhr_str}: {grib_size:.1f} MB in {download_time:.1f}s")
            
            # Extract and insert with parallel processing
            start_time = time.time()
            record_count = self.extract_nbm_data_parallel(grib_path, zips, fhr_str)
            process_time = time.time() - start_time
            self.log(f"  ✓ Completed f{fhr_str} in {process_time:.1f}s - {record_count} records")
            
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
        """Main ingestion process with parallel forecast hour processing"""
        try:
            # Get latest cycle
            cycle = self.get_latest_nbm_cycle()
            self.log(f"Latest NBM run: {cycle['run_date']} {cycle['run_hour']}Z")
            
            # Determine what to download
            forecast_plan = self.determine_forecast_hours_to_process(cycle)
            forecast_hours = forecast_plan['hours']
            
            self.log(f"Strategy: {forecast_plan['reason']}")
            self.log(f"Will process {len(forecast_hours)} hours: {forecast_hours}")
            
            # Load ZIP codes once (with Redis caching)
            zips = self.load_zip_codes()
            
            # Process forecast hours IN PARALLEL
            total_records = 0
            overall_start = time.time()
            
            self.log(f"Processing {len(forecast_hours)} hours with {self.max_parallel_hours} parallel workers...")
            
            with ThreadPoolExecutor(max_workers=self.max_parallel_hours) as executor:
                futures = {
                    executor.submit(
                        self.process_forecast_hour,
                        cycle['run_date'],
                        cycle['run_hour'],
                        fhr,
                        zips
                    ): fhr
                    for fhr in forecast_hours
                }
                
                for future in as_completed(futures):
                    fhr = futures[future]
                    try:
                        record_count = future.result()
                        total_records += record_count
                        elapsed = time.time() - overall_start
                        self.log(f"PROGRESS: f{fhr:03d} done | {total_records} records | {elapsed:.1f}s elapsed")
                    except Exception as e:
                        self.log(f"✗ Failed f{fhr:03d}: {e}")
                
                gc.collect()
            
            # Log this run
            self.log_ingestion_run(
                cycle['run_date'],
                cycle['run_hour'],
                forecast_hours,
                total_records
            )
            
            total_time = time.time() - overall_start
            self.log(f"✓ Ingestion completed - {total_records} records in {total_time:.1f}s")
            self.log(f"⚡ Throughput: {total_records/total_time:.0f} records/second")
            
            # Close connection pool
            if self.conn_pool:
                self.conn_pool.closeall()
            
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