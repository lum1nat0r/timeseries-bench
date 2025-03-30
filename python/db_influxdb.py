import pandas as pd
import numpy as np
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.delete_api import DeleteApi
from influxdb_client.rest import ApiException
from typing import Dict, Any, Optional, List
import os
import sys
import yaml
import argparse
import time # For basic verification delay

from .db_interface import DatabaseInterface

class InfluxdbHandler(DatabaseInterface):
    """InfluxDB implementation of the DatabaseInterface."""

    def __init__(self):
        self.client: Optional[InfluxDBClient] = None
        self.write_api = None
        self.query_api = None
        self.delete_api: Optional[DeleteApi] = None
        self.config: Dict[str, Any] = {}
        self.bucket: Optional[str] = None
        self.org: Optional[str] = None
        self.measurement: Optional[str] = None

    def connect(self, config: Dict[str, Any]) -> None:
        """Establish connection to InfluxDB."""
        self.config = config
        url = f"http://{config['host']}:{config['port']}"
        token = config['token']
        self.org = config['org']
        self.bucket = config['bucket']
        self.measurement = config.get('measurement', 'sensor_data') # Default measurement name

        try:
            self.client = InfluxDBClient(url=url, token=token, org=self.org, timeout=30_000) # 30s timeout
            self.write_api = self.client.write_api(write_options=SYNCHRONOUS)
            self.query_api = self.client.query_api()
            self.delete_api = self.client.delete_api()
            # Verify connection
            self.client.ping()
            print(f"Successfully connected to InfluxDB at {url}")
        except Exception as e:
            print(f"Error connecting to InfluxDB: {e}")
            self.client = None
            raise ConnectionError(f"Failed to connect to InfluxDB: {e}")

    def setup_schema(self, drop_existing: bool = True) -> None:
        """Create the bucket, optionally dropping it first."""
        if not self.client or not self.org or not self.bucket:
            raise ConnectionError("Not connected to InfluxDB.")

        buckets_api = self.client.buckets_api()
        existing_bucket = buckets_api.find_bucket_by_name(self.bucket)

        if existing_bucket and drop_existing:
            print(f"Deleting existing bucket: {self.bucket}")
            try:
                buckets_api.delete_bucket(existing_bucket)
                print(f"Bucket '{self.bucket}' deleted.")
            except ApiException as e:
                # Handle cases where bucket might be in use or deletion fails
                print(f"Could not delete bucket '{self.bucket}': {e}. Proceeding might cause issues.")


        if not existing_bucket or drop_existing:
             print(f"Creating bucket: {self.bucket}")
             try:
                 buckets_api.create_bucket(bucket_name=self.bucket, org_id=self.org)
                 print(f"Bucket '{self.bucket}' created successfully.")
             except ApiException as e:
                 # Handle cases where bucket creation fails (e.g., already exists if drop failed)
                 if e.status == 422: # Unprocessable Entity - often means bucket exists
                     print(f"Bucket '{self.bucket}' likely already exists.")
                 else:
                     print(f"Failed to create bucket '{self.bucket}': {e}")
                     raise


    def insert_individual(self, timestamp: pd.Timestamp, data: Dict[str, Any], recording_id: str) -> None:
        """Insert a single data point."""
        if not self.write_api or not self.bucket or not self.measurement:
            raise ConnectionError("InfluxDB write API not initialized.")

        point = Point(self.measurement)
        point.tag("recording_id", recording_id)
        point.time(timestamp, write_precision='ns') # Use nanosecond precision

        for key, value in data.items():
            if key == 'timestamp': continue # Timestamp is handled separately
            # Ensure correct type casting for InfluxDB
            if isinstance(value, (int, float)):
                point.field(key, float(value)) # Store numeric types as float
            elif isinstance(value, bool):
                point.field(key, value)
            else:
                point.field(key, str(value)) # Store others as string

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)
        except ApiException as e:
            print(f"Error writing individual point to InfluxDB: {e}")
            # Consider adding retry logic or specific error handling

    def insert_batch(self, data_batch_df: pd.DataFrame, recording_id: str) -> None:
        """Insert a batch of data points from a pandas DataFrame."""
        if not self.write_api or not self.bucket or not self.measurement:
            raise ConnectionError("InfluxDB write API not initialized.")

        # Optimized point creation using list comprehension
        points = []
        # Convert timestamp column to datetime objects upfront if not already
        if not pd.api.types.is_datetime64_any_dtype(data_batch_df['timestamp']):
             data_batch_df['timestamp'] = pd.to_datetime(data_batch_df['timestamp'])

        # Prepare columns excluding timestamp
        field_columns = [col for col in data_batch_df.columns if col != 'timestamp']

        # Iterate over rows using df.itertuples() which is faster than iterrows()
        # or convert to records list for potentially even faster access
        records = data_batch_df.to_dict('records') # Convert DF to list of dicts

        for record in records:
            point = Point(self.measurement)
            point.tag("recording_id", recording_id)
            point.time(record['timestamp'], write_precision='ns')

            for col in field_columns:
                value = record[col]
                if pd.isna(value):
                    continue # Skip NaN fields

                # Type casting
                if isinstance(value, (int, float, np.number)):
                    point.field(col, float(value))
                elif isinstance(value, bool):
                    point.field(col, value)
                else:
                    point.field(col, str(value))
            points.append(point)

        if not points:
             print("Warning: No points generated for batch insert.")
             return # Avoid writing empty list

        try:
            self.write_api.write(bucket=self.bucket, org=self.org, record=points)
        except ApiException as e:
            print(f"Error writing batch to InfluxDB: {e}")
            # Consider adding retry logic or specific error handling

    def _execute_flux_query(self, query: str) -> Optional[pd.DataFrame]:
        """Helper to execute Flux query and return DataFrame."""
        if not self.query_api:
            raise ConnectionError("InfluxDB query API not initialized.")
        try:
            # Use stream for potentially large results, convert to DataFrame
            result_df = self.query_api.query_data_frame(query=query, org=self.org)
            if isinstance(result_df, List): # Sometimes returns list of DFs
                if not result_df: return pd.DataFrame() # Empty list
                result_df = pd.concat(result_df, ignore_index=True)

            if result_df is None or result_df.empty:
                return pd.DataFrame() # Return empty DataFrame for no results

            # Standardize column names (Flux often returns _time, _value, _field, etc.)
            result_df = result_df.rename(columns={'_time': 'timestamp', '_value': 'value', '_field': 'field'})
            # Convert timestamp to datetime objects if not already
            if 'timestamp' in result_df.columns:
                 result_df['timestamp'] = pd.to_datetime(result_df['timestamp'])

            # Drop InfluxDB internal columns if they exist
            internal_cols = ['result', 'table', '_start', '_stop', '_measurement']
            cols_to_drop = [col for col in internal_cols if col in result_df.columns]
            if cols_to_drop:
                result_df = result_df.drop(columns=cols_to_drop)

            return result_df

        except ApiException as e:
            print(f"Error executing Flux query: {e}\nQuery:\n{query}")
            return None
        except Exception as e:
            print(f"Unexpected error during Flux query execution: {e}\nQuery:\n{query}")
            return None


    def query_time_range(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str) -> Optional[pd.DataFrame]:
        """Query data within a specific time range."""
        start_iso = start.isoformat() + "Z"
        end_iso = end.isoformat() + "Z"
        query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_iso}, stop: {end_iso})
              |> filter(fn: (r) => r["_measurement"] == "{self.measurement}")
              |> filter(fn: (r) => r["recording_id"] == "{recording_id}")
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> keep(columns: ["_time", "temperature", "humidity", "gyro_x", "gyro_y", "gyro_z", "acceleration_x", "acceleration_y", "acceleration_z", "magnetometer_x", "magnetometer_y", "magnetometer_z", "anomaly_flag"])
              |> rename(columns: {{ "_time": "timestamp" }})
        '''
        return self._execute_flux_query(query)


    def query_filtered(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str, filter_condition: Dict[str, Any]) -> Optional[pd.DataFrame]:
        """Query data with a filter condition within a time range."""
        start_iso = start.isoformat() + "Z"
        end_iso = end.isoformat() + "Z"
        field = filter_condition['field']
        op = filter_condition['op']
        value = filter_condition['value']

        # Format value correctly for Flux query
        if isinstance(value, str):
            flux_value = f'"{value}"'
        elif isinstance(value, bool):
            flux_value = str(value).lower() # true or false
        else: # Numeric
            flux_value = str(value)

        query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_iso}, stop: {end_iso})
              |> filter(fn: (r) => r["_measurement"] == "{self.measurement}")
              |> filter(fn: (r) => r["recording_id"] == "{recording_id}")
              |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
              |> filter(fn: (r) => r["{field}"] {op} {flux_value})
              |> keep(columns: ["_time", "temperature", "humidity", "gyro_x", "gyro_y", "gyro_z", "acceleration_x", "acceleration_y", "acceleration_z", "magnetometer_x", "magnetometer_y", "magnetometer_z", "anomaly_flag"])
              |> rename(columns: {{ "_time": "timestamp" }})
        '''
        return self._execute_flux_query(query)


    def query_aggregate(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str, interval: str, aggregation_func: str, field: str) -> Optional[pd.DataFrame]:
        """Query aggregated data over a time range."""
        start_iso = start.isoformat() + "Z"
        end_iso = end.isoformat() + "Z"

        # Map common function names to Flux functions if needed
        flux_func = aggregation_func.lower()
        # Add more mappings if necessary (e.g., stddev, median)
        if flux_func == 'mean': flux_func = 'mean'
        elif flux_func == 'count': flux_func = 'count'
        elif flux_func == 'sum': flux_func = 'sum'
        elif flux_func == 'min': flux_func = 'min'
        elif flux_func == 'max': flux_func = 'max'
        # else: raise ValueError(f"Unsupported aggregation function: {aggregation_func}") # Or default/log

        query = f'''
            from(bucket: "{self.bucket}")
              |> range(start: {start_iso}, stop: {end_iso})
              |> filter(fn: (r) => r["_measurement"] == "{self.measurement}")
              |> filter(fn: (r) => r["recording_id"] == "{recording_id}")
              |> filter(fn: (r) => r["_field"] == "{field}")
              |> aggregateWindow(every: {interval}, fn: {flux_func}, createEmpty: false)
              |> yield(name: "{aggregation_func}_{field}")
        '''
        # Note: aggregateWindow result format is different, might not need pivot/keep/rename like others
        # The result will typically have _time, _value, and grouping keys.
        # We rename _value to the function name for clarity.
        df = self._execute_flux_query(query)
        if df is not None and not df.empty:
             if 'value' in df.columns:
                 df = df.rename(columns={'value': f'{aggregation_func}_{field}'})
             # Ensure timestamp column exists
             if 'timestamp' not in df.columns and '_time' in df.columns:
                 df = df.rename(columns={'_time': 'timestamp'})
             # Select relevant columns
             cols_to_keep = ['timestamp', f'{aggregation_func}_{field}']
             df = df[[col for col in cols_to_keep if col in df.columns]]

        return df


    def delete_time_range(self, start: pd.Timestamp, end: pd.Timestamp, recording_id: str) -> None:
        """Delete data within a specific time range."""
        if not self.delete_api or not self.org or not self.bucket or not self.measurement:
            raise ConnectionError("InfluxDB delete API not initialized.")

        start_iso = start.isoformat() + "Z"
        end_iso = end.isoformat() + "Z"
        predicate = f'_measurement="{self.measurement}" AND recording_id="{recording_id}"'

        try:
            self.delete_api.delete(start=start_iso, stop=end_iso, predicate=predicate, bucket=self.bucket, org=self.org)
            print(f"Data deleted for recording '{recording_id}' between {start_iso} and {end_iso}")
        except ApiException as e:
            print(f"Error deleting data from InfluxDB: {e}")
            # Consider adding retry logic or specific error handling

    def delete_recording(self, recording_id: str) -> None:
        """Delete all data associated with a specific recording_id."""
        if not self.delete_api or not self.org or not self.bucket or not self.measurement:
            raise ConnectionError("InfluxDB delete API not initialized.")

        # Use a very wide time range to ensure all data for the recording is deleted
        # InfluxDB requires start and stop times for deletion.
        start_time = "1970-01-01T00:00:00Z"
        stop_time = "2200-01-01T00:00:00Z" # Far future date
        predicate = f'_measurement="{self.measurement}" AND recording_id="{recording_id}"'

        try:
            self.delete_api.delete(start=start_time, stop=stop_time, predicate=predicate, bucket=self.bucket, org=self.org)
            print(f"All data deleted for recording '{recording_id}'")
        except ApiException as e:
            print(f"Error deleting recording data from InfluxDB: {e}")

    def close(self) -> None:
        """Close the InfluxDB connection."""
        if self.write_api:
            self.write_api.close()
            self.write_api = None
        if self.client:
            self.client.close()
            self.client = None
            print("InfluxDB connection closed.")

    def get_connection_info(self) -> Dict[str, Any]:
        """Return basic information about the current connection."""
        return {
            "type": "influxdb",
            "host": self.config.get('host'),
            "port": self.config.get('port'),
            "org": self.org,
            "bucket": self.bucket,
            "measurement": self.measurement
        }

    def test_insert_csv(self, csv_file_path: str, drop_existing_bucket: bool = False) -> None:
        """
        Reads a CSV file, extracts recording_id, and inserts data into InfluxDB.
        Includes basic verification.
        """
        if not self.client or not self.bucket or not self.org or not self.measurement:
            raise ConnectionError("Not connected to InfluxDB.")

        print(f"\n--- Testing CSV Insertion: {csv_file_path} ---")

        # 1. Optionally drop and recreate the bucket
        if drop_existing_bucket:
            print("Attempting to drop and recreate bucket...")
            self.setup_schema(drop_existing=True)
            # Short delay to ensure bucket operations complete if needed
            time.sleep(1)
        else:
            # Ensure bucket exists without dropping
            self.setup_schema(drop_existing=False)


        # 2. Read CSV file
        try:
            print(f"Reading CSV file: {csv_file_path}")
            data_df = pd.read_csv(csv_file_path)
            if 'timestamp' not in data_df.columns:
                 raise ValueError("CSV file must contain a 'timestamp' column.")
            print(f"Successfully read {len(data_df)} rows from CSV.")
        except FileNotFoundError:
            print(f"Error: CSV file not found at {csv_file_path}")
            return
        except Exception as e:
            print(f"Error reading CSV file {csv_file_path}: {e}")
            return

        # 3. Extract recording_id from filename (e.g., recording_rec_0000_0001.csv -> rec_0000_0001)
        try:
            base_name = os.path.basename(csv_file_path)
            # Assuming format like 'prefix_rec_id_part1_id_part2.csv'
            parts = base_name.split('.')[0].split('_')
            if len(parts) >= 3 and parts[1] == 'rec':
                 recording_id = "_".join(parts[1:]) # e.g., rec_0000_0001
            else:
                 # Fallback or default if pattern doesn't match
                 recording_id = base_name.split('.')[0]
                 print(f"Warning: Could not extract standard recording_id from filename '{base_name}'. Using '{recording_id}'.")
            print(f"Extracted recording_id: {recording_id}")
        except Exception as e:
            print(f"Error extracting recording_id from filename {csv_file_path}: {e}")
            recording_id = "unknown_recording" # Default fallback
            print(f"Using default recording_id: {recording_id}")


        # 4. Insert data using existing insert_batch method
        try:
            print(f"Inserting batch for recording_id '{recording_id}'...")
            start_insert_time = time.time()
            self.insert_batch(data_df, recording_id)
            end_insert_time = time.time()
            print(f"Batch insertion completed in {end_insert_time - start_insert_time:.2f} seconds.")
        except Exception as e:
            print(f"Error during batch insertion for {csv_file_path}: {e}")
            return # Stop if insertion fails

        # 5. Basic Verification (Optional but recommended)
        print("Performing basic verification...")
        time.sleep(2) # Allow some time for data to become queryable

        try:
            # Derive time range from data for verification query
            if not pd.api.types.is_datetime64_any_dtype(data_df['timestamp']):
                 data_df['timestamp'] = pd.to_datetime(data_df['timestamp'])
            min_ts = data_df['timestamp'].min()
            max_ts = data_df['timestamp'].max()

            # Query to count points for this recording_id in the data's time range
            start_iso = min_ts.isoformat() + "Z"
            end_iso = (max_ts + pd.Timedelta(seconds=1)).isoformat() + "Z" # Add buffer to end time

            count_query = f'''
                from(bucket: "{self.bucket}")
                  |> range(start: {start_iso}, stop: {end_iso})
                  |> filter(fn: (r) => r["_measurement"] == "{self.measurement}")
                  |> filter(fn: (r) => r["recording_id"] == "{recording_id}")
                  |> count()
            '''
            print(f"Executing verification query for recording '{recording_id}' between {start_iso} and {end_iso}")
            count_df = self._execute_flux_query(count_query)

            if count_df is not None and not count_df.empty:
                inserted_count = count_df['value'].iloc[0]
                print(f"Verification: Found {inserted_count} points in InfluxDB for recording_id '{recording_id}'. Expected ~{len(data_df)}.")
                if inserted_count == len(data_df):
                    print("Verification successful: Row count matches.")
                else:
                    print("Verification warning: Row count mismatch.")
            elif count_df is not None and count_df.empty:
                 print("Verification warning: No points found in the query range for this recording.")
            else:
                print("Verification failed: Could not retrieve count from InfluxDB.")

        except Exception as e:
            print(f"Error during verification query: {e}")

        print(f"--- CSV Insertion Test Finished: {csv_file_path} ---")


# Command-line interface for CSV insertion testing
if __name__ == '__main__':
    # Set up command-line argument parsing
    parser = argparse.ArgumentParser(description='Test CSV file insertion into InfluxDB')
    parser.add_argument('csv_file', help='Path to the CSV file to insert')
    parser.add_argument('--drop', action='store_true', help='Drop and recreate the bucket before insertion')
    parser.add_argument('--config', default=None, help='Path to config.yaml file (default: ../config.yaml)')
    args = parser.parse_args()

    # Determine config path
    script_dir = os.path.dirname(__file__)
    config_path = args.config if args.config else os.path.join(script_dir, '..', 'config.yaml')
    
    try:
        print(f"Loading configuration from {config_path}")
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)['database']

        # Validate configuration
        if 'YOUR_INFLUXDB_API_TOKEN' in config.get('token', '') or 'your-org' in config.get('org', ''):
            print("Error: Please replace placeholder values in config.yaml (token, org) before running this test.")
            sys.exit(1)

        # Initialize handler and connect to InfluxDB
        print("Initializing InfluxDB handler...")
        handler = InfluxdbHandler()
        
        try:
            # Connect to InfluxDB
            print(f"Connecting to InfluxDB at {config.get('host', 'localhost')}:{config.get('port', '8086')}...")
            handler.connect(config)
            
            # Test CSV insertion
            handler.test_insert_csv(args.csv_file, drop_existing_bucket=args.drop)
            
        except ConnectionError as e:
            print(f"Connection failed: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"An error occurred during testing: {e}")
            sys.exit(1)
        finally:
            # Close connection
            if handler and handler.client:
                handler.close()

    except FileNotFoundError:
        print(f"Error: config.yaml not found at {config_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error loading or parsing config.yaml: {e}")
        sys.exit(1)
