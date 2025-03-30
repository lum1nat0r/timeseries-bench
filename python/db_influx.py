import pandas as pd
from influxdb_client import InfluxDBClient, Point, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client.client.delete_api import DeleteApi
from influxdb_client.rest import ApiException
from typing import Dict, Any, Optional, List

from .db_interface import DatabaseInterface

class InfluxDBHandler(DatabaseInterface):
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

        points = []
        for index, row in data_batch_df.iterrows():
            point = Point(self.measurement)
            point.tag("recording_id", recording_id)
            # Ensure timestamp is timezone-aware or handle appropriately
            ts = pd.to_datetime(row['timestamp'])
            point.time(ts, write_precision='ns')

            for col_name in data_batch_df.columns:
                if col_name == 'timestamp': continue
                value = row[col_name]
                # Handle potential NaN values if necessary
                if pd.isna(value):
                    continue # Skip NaN fields or handle as needed

                # Type casting
                if isinstance(value, (int, float, np.number)):
                     point.field(col_name, float(value))
                elif isinstance(value, bool):
                     point.field(col_name, value)
                else:
                     point.field(col_name, str(value))

            points.append(point)

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

# Example usage (for testing purposes, typically run from benchmark.py)
if __name__ == '__main__':
    # This block is for basic testing of the handler
    # You would need a running InfluxDB instance and a valid token/org/bucket
    import yaml
    import os

    # Load config relative to this file's location if run directly
    script_dir = os.path.dirname(__file__)
    config_path = os.path.join(script_dir, '..', 'config.yaml') # Assumes config.yaml is in parent dir

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)['database']

        # !!! Replace placeholders in config.yaml before running !!!
        if 'YOUR_INFLUXDB_API_TOKEN' in config['token'] or 'your-org' in config['org']:
             print("Please replace placeholder values in config.yaml (token, org) before running this test.")
        else:
            handler = InfluxDBHandler()
            try:
                handler.connect(config)
                handler.setup_schema(drop_existing=True)

                # --- Test Insertions ---
                print("\n--- Testing Insertions ---")
                test_time = pd.Timestamp.now(tz='UTC')
                test_data_single = {'temperature': 25.5, 'humidity': 55.1, 'anomaly_flag': False}
                handler.insert_individual(test_time, test_data_single, "test_rec_001")
                print("Inserted single point.")

                test_batch_data = {
                    'timestamp': [test_time + pd.Timedelta(seconds=i) for i in range(3)],
                    'temperature': [26.0, 26.1, 26.2],
                    'humidity': [56.0, 56.5, 57.0],
                    'anomaly_flag': [False, True, False]
                }
                test_batch_df = pd.DataFrame(test_batch_data)
                handler.insert_batch(test_batch_df, "test_rec_001")
                print(f"Inserted batch of {len(test_batch_df)} points.")

                # Allow time for data to be processed if necessary
                import time
                time.sleep(2)

                # --- Test Queries ---
                print("\n--- Testing Queries ---")
                start_q = test_time - pd.Timedelta(minutes=1)
                end_q = test_time + pd.Timedelta(minutes=1)

                print(f"\nQuery Time Range ({start_q} to {end_q}):")
                df_range = handler.query_time_range(start_q, end_q, "test_rec_001")
                print(df_range)

                print("\nQuery Filtered (humidity > 56.2):")
                filter_cond = {'field': 'humidity', 'op': '>', 'value': 56.2}
                df_filtered = handler.query_filtered(start_q, end_q, "test_rec_001", filter_cond)
                print(df_filtered)

                print("\nQuery Aggregate (mean temperature per 1s):")
                df_agg = handler.query_aggregate(start_q, end_q, "test_rec_001", '1s', 'mean', 'temperature')
                print(df_agg)

                # --- Test Deletions ---
                print("\n--- Testing Deletions ---")
                del_start = test_time + pd.Timedelta(seconds=0.5)
                del_end = test_time + pd.Timedelta(seconds=1.5)
                handler.delete_time_range(del_start, del_end, "test_rec_001")
                # Verify deletion
                print("\nQuery after partial deletion:")
                df_after_del = handler.query_time_range(start_q, end_q, "test_rec_001")
                print(df_after_del)

                handler.delete_recording("test_rec_001")
                 # Verify deletion
                print("\nQuery after full recording deletion:")
                df_after_full_del = handler.query_time_range(start_q, end_q, "test_rec_001")
                print(df_after_full_del)


            except ConnectionError as e:
                print(f"Connection failed: {e}")
            except Exception as e:
                print(f"An error occurred during testing: {e}")
            finally:
                if handler and handler.client:
                    handler.close()

    except FileNotFoundError:
        print(f"Error: config.yaml not found at {config_path}")
    except Exception as e:
        print(f"Error loading or parsing config.yaml: {e}")
