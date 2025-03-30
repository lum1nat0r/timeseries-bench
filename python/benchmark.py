import yaml
import pandas as pd
import time
import os
import importlib
import csv
import random
from typing import Dict, Any, List, Optional, Type
from datetime import datetime
from pathlib import Path

# Assuming db_interface and handlers are in the same directory or accessible via PYTHONPATH
from db_interface import DatabaseInterface
# Import specific handlers dynamically later

# --- Configuration Loading ---

def load_config(config_path: str = '../config.yaml') -> Dict[str, Any]:
    """Loads the YAML configuration file."""
    # Adjust path relative to this script's location
    script_dir = Path(__file__).parent
    abs_config_path = (script_dir / config_path).resolve()
    print(f"Loading configuration from: {abs_config_path}")
    try:
        with open(abs_config_path, 'r') as f:
            config = yaml.safe_load(f)
        print("Configuration loaded successfully.")
        # Basic validation
        if 'database' not in config or 'type' not in config['database']:
            raise ValueError("Config missing 'database.type' section.")
        if 'benchmark_params' not in config:
            raise ValueError("Config missing 'benchmark_params' section.")
        if 'data_source' not in config or 'directory' not in config['data_source']:
             raise ValueError("Config missing 'data_source.directory' section.")
        if 'results' not in config or 'output_file' not in config['results']:
             raise ValueError("Config missing 'results.output_file' section.")
        return config
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found at {abs_config_path}")
        raise
    except Exception as e:
        print(f"ERROR: Failed to load or parse configuration: {e}")
        raise

# --- Database Handler Loading ---

def get_db_handler(db_type: str) -> Type[DatabaseInterface]:
    """Dynamically imports and returns the appropriate DB handler class."""
    try:
        module_name = f"db_{db_type}"
        # Capitalize convention for class name (e.g., influxdb -> InfluxDBHandler)
        class_name = f"{db_type.capitalize()}Handler"
        # Import the module
        db_module = importlib.import_module(module_name)
        # Get the class from the module
        handler_class = getattr(db_module, class_name)
        if not issubclass(handler_class, DatabaseInterface):
             raise TypeError(f"{class_name} does not implement DatabaseInterface")
        print(f"Successfully loaded database handler: {class_name}")
        return handler_class
    except ModuleNotFoundError:
        print(f"ERROR: Database handler module '{module_name}.py' not found.")
        raise
    except AttributeError:
        print(f"ERROR: Class '{class_name}' not found in module '{module_name}.py'.")
        raise
    except Exception as e:
        print(f"ERROR: Failed to load database handler for type '{db_type}': {e}")
        raise

# --- Data Loading ---

def load_data_files(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Loads specified CSV data files into pandas DataFrames."""
    data_dir_rel = config['data_source']['directory']
    script_dir = Path(__file__).parent
    data_dir = (script_dir / '..' / data_dir_rel).resolve() # Assumes data dir is relative to project root
    num_files = config['data_source'].get('num_files_to_use')
    specific_files = config['data_source'].get('filenames')
    all_files_data = []

    print(f"Loading data from: {data_dir}")

    if not data_dir.is_dir():
        print(f"ERROR: Data directory not found: {data_dir}")
        # Optionally, offer to run data_generator.py here
        print("Please ensure data has been generated using data_generator.py")
        raise FileNotFoundError(f"Data directory not found: {data_dir}")

    if specific_files:
        files_to_load = [data_dir / f for f in specific_files]
    else:
        all_csv_files = sorted(list(data_dir.glob('*.csv')))
        if not all_csv_files:
             print(f"ERROR: No CSV files found in {data_dir}")
             raise FileNotFoundError(f"No CSV files found in {data_dir}")
        if num_files and num_files < len(all_csv_files):
            files_to_load = random.sample(all_csv_files, num_files)
            print(f"Randomly selected {num_files} files.")
        else:
            files_to_load = all_csv_files
            print(f"Using all {len(all_csv_files)} files found.")

    print(f"Attempting to load {len(files_to_load)} data file(s)...")
    loaded_count = 0
    skipped_count = 0
    for file_path in files_to_load:
        try:
            print(f"  Loading {file_path.name}...")
            # Extract recording_id from filename (e.g., recording_rec_0000_0000.csv -> rec_0000_0000)
            recording_id = "_".join(file_path.stem.split('_')[1:]) # Assumes 'recording_' prefix

            df = pd.read_csv(file_path)
            # Convert timestamp column immediately
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df.dropna(subset=['timestamp']) # Drop rows where timestamp conversion failed
            df = df.sort_values(by='timestamp').reset_index(drop=True) # Ensure order

            if df.empty:
                print(f"  WARNING: Skipped empty or invalid file: {file_path.name}")
                skipped_count += 1
                continue

            all_files_data.append({
                'filename': file_path.name,
                'recording_id': recording_id,
                'dataframe': df,
                'start_time': df['timestamp'].min(),
                'end_time': df['timestamp'].max()
            })
            loaded_count += 1
        except Exception as e:
            print(f"  ERROR: Failed to load or process {file_path.name}: {e}")
            skipped_count += 1

    if loaded_count == 0:
         raise ValueError("Failed to load any valid data files.")

    print(f"Successfully loaded {loaded_count} data file(s), skipped {skipped_count}.")
    return all_files_data


# --- Benchmarking Functions ---

def run_insert_benchmarks(db_handler: DatabaseInterface, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Runs individual and batch insert benchmarks."""
    results = []
    batch_sizes = config['benchmark_params']['batch_sizes']
    num_individual = config['benchmark_params']['num_individual_inserts']
    total_rows_inserted = 0

    print("\n--- Running Insert Benchmarks ---")

    # Batch Inserts
    for batch_size in batch_sizes:
        print(f"  Testing Batch Insert (Size: {batch_size})...")
        total_time = 0
        rows_in_batch_run = 0
        start_overall = time.time()

        for file_data in data:
            df = file_data['dataframe']
            recording_id = file_data['recording_id']
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i + batch_size]
                if batch_df.empty: continue

                start_batch = time.time()
                try:
                    db_handler.insert_batch(batch_df, recording_id)
                    end_batch = time.time()
                    total_time += (end_batch - start_batch)
                    rows_in_batch_run += len(batch_df)
                except Exception as e:
                    print(f"    ERROR during batch insert (size {batch_size}, recording {recording_id}): {e}")
                    # Decide whether to continue or stop benchmark on error

        end_overall = time.time()
        throughput = rows_in_batch_run / total_time if total_time > 0 else 0
        total_rows_inserted += rows_in_batch_run # Track total rows for subsequent tests

        print(f"    Batch Size: {batch_size}, Rows: {rows_in_batch_run}, Total Time: {total_time:.4f}s, Throughput: {throughput:.2f} rows/s")
        results.append({
            'operation': 'insert_batch',
            'batch_size': batch_size,
            'rows_inserted': rows_in_batch_run,
            'total_time_s': round(total_time, 4),
            'throughput_rows_per_s': round(throughput, 2)
        })

    # Individual Inserts
    print(f"\n  Testing Individual Insert (Count: {num_individual})...")
    individual_times = []
    rows_to_insert = []
    # Collect samples from loaded data for individual inserts
    samples_collected = 0
    for file_data in data:
        if samples_collected >= num_individual: break
        df = file_data['dataframe']
        recording_id = file_data['recording_id']
        take_count = min(num_individual - samples_collected, len(df))
        samples = df.head(take_count) # Take first N samples from each file until limit
        for _, row in samples.iterrows():
             rows_to_insert.append({'timestamp': row['timestamp'], 'data': row.drop('timestamp').to_dict(), 'recording_id': recording_id})
        samples_collected += take_count

    if not rows_to_insert:
        print("    WARNING: No data available for individual inserts.")
    else:
        start_overall_ind = time.time()
        inserted_count = 0
        for row_info in rows_to_insert:
            start_ind = time.time()
            try:
                db_handler.insert_individual(row_info['timestamp'], row_info['data'], row_info['recording_id'])
                end_ind = time.time()
                individual_times.append(end_ind - start_ind)
                inserted_count += 1
            except Exception as e:
                 print(f"    ERROR during individual insert (recording {row_info['recording_id']}): {e}")
        end_overall_ind = time.time()

        avg_latency = sum(individual_times) / len(individual_times) if individual_times else 0
        total_time_ind = end_overall_ind - start_overall_ind
        print(f"    Inserted: {inserted_count}, Avg Latency: {avg_latency:.6f}s, Total Time: {total_time_ind:.4f}s")
        results.append({
            'operation': 'insert_individual',
            'rows_inserted': inserted_count,
            'avg_latency_s': round(avg_latency, 6),
            'total_time_s': round(total_time_ind, 4)
        })
        total_rows_inserted += inserted_count

    print(f"--- Insert Benchmarks Complete (Total rows in DB approx: {total_rows_inserted}) ---")
    return results


def run_query_benchmarks(db_handler: DatabaseInterface, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Runs various query benchmarks based on config."""
    results = []
    query_defs = config['benchmark_params']['query_definitions']

    if not data:
        print("WARNING: No data loaded, skipping query benchmarks.")
        return results

    print("\n--- Running Query Benchmarks ---")

    # Use time ranges from the loaded data
    # Select one file's time range for consistency in relative queries
    ref_file = data[0]
    ref_start = ref_file['start_time']
    ref_end = ref_file['end_time']
    ref_recording_id = ref_file['recording_id']
    print(f"  Using reference recording '{ref_recording_id}' (Time range: {ref_start} to {ref_end}) for queries.")

    for i, q_def in enumerate(query_defs):
        print(f"  Running Query {i+1}: {q_def.get('description', q_def['type'])}")
        q_type = q_def['type']
        latency = -1.0
        rows_returned = -1
        query_params = q_def.copy() # Store params for results

        try:
            # Calculate start/end times based on duration relative to reference
            duration = pd.to_timedelta(q_def['duration'])
            q_start = ref_start # Start from the beginning of the reference recording
            q_end = min(ref_start + duration, ref_end) # Ensure query doesn't exceed recording end

            start_time = time.time()
            result_df: Optional[pd.DataFrame] = None

            if q_type == 'range':
                result_df = db_handler.query_time_range(q_start, q_end, ref_recording_id)
            elif q_type == 'filtered':
                filter_cond = {k: v for k, v in q_def.items() if k.startswith('filter_')}
                # Rename keys for the handler method
                filter_arg = {
                    'field': filter_cond['filter_field'],
                    'op': filter_cond['filter_op'],
                    'value': filter_cond['filter_value']
                }
                result_df = db_handler.query_filtered(q_start, q_end, ref_recording_id, filter_arg)
            elif q_type == 'aggregate':
                result_df = db_handler.query_aggregate(q_start, q_end, ref_recording_id, q_def['interval'], q_def['func'], q_def['field'])
            else:
                print(f"    WARNING: Unknown query type '{q_type}' defined in config.")
                continue

            end_time = time.time()
            latency = end_time - start_time
            rows_returned = len(result_df) if result_df is not None else 0

            print(f"    Latency: {latency:.4f}s, Rows Returned: {rows_returned}")

        except Exception as e:
            print(f"    ERROR running query {i+1}: {e}")
            latency = -1.0 # Indicate error
            rows_returned = -1

        results.append({
            'operation': 'query',
            'query_type': q_type,
            'query_params': query_params,
            'latency_s': round(latency, 4),
            'rows_returned': rows_returned
        })

    print("--- Query Benchmarks Complete ---")
    return results


def run_delete_benchmarks(db_handler: DatabaseInterface, data: List[Dict[str, Any]], config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Runs delete benchmarks based on config."""
    results = []
    delete_defs = config['benchmark_params'].get('delete_params', [])

    if not data:
        print("WARNING: No data loaded, skipping delete benchmarks.")
        return results
    if not delete_defs:
        print("INFO: No delete operations defined in config, skipping delete benchmarks.")
        return results

    print("\n--- Running Delete Benchmarks ---")

    # Use time ranges/recordings from the loaded data
    # We might delete data needed for subsequent tests if not careful,
    # so maybe use recordings not used as reference for queries? Or run deletes last.
    # For simplicity, let's use the reference recording for range deletes
    # and potentially a different one for full recording delete.

    ref_file = data[0]
    ref_start = ref_file['start_time']
    ref_end = ref_file['end_time']
    ref_recording_id = ref_file['recording_id']

    # Find another recording ID if possible for the full delete test
    delete_recording_id = ref_recording_id
    if len(data) > 1:
        delete_recording_id = data[-1]['recording_id'] # Use the last loaded file's ID
    print(f"  Using reference recording '{ref_recording_id}' for range deletes.")
    print(f"  Using recording '{delete_recording_id}' for full recording delete.")


    for i, d_def in enumerate(delete_defs):
        print(f"  Running Delete Op {i+1}: {d_def.get('description', d_def['type'])}")
        d_type = d_def['type']
        latency = -1.0
        delete_params = d_def.copy()

        try:
            start_time = time.time()

            if d_type == 'range':
                duration = pd.to_timedelta(d_def['duration'])
                d_start = ref_start
                d_end = min(ref_start + duration, ref_end)
                db_handler.delete_time_range(d_start, d_end, ref_recording_id)
                delete_params['start_time_deleted'] = d_start.isoformat()
                delete_params['end_time_deleted'] = d_end.isoformat()
                delete_params['recording_id'] = ref_recording_id
            elif d_type == 'recording':
                db_handler.delete_recording(delete_recording_id)
                delete_params['recording_id'] = delete_recording_id
                 # After deleting a full recording, remove it from data list
                 # to avoid issues if benchmarks were re-ordered
                data = [d for d in data if d['recording_id'] != delete_recording_id]
                if not data: print("    WARNING: Deleted the last available recording's data.")
            else:
                print(f"    WARNING: Unknown delete type '{d_type}' defined in config.")
                continue

            end_time = time.time()
            latency = end_time - start_time
            print(f"    Latency: {latency:.4f}s")

        except Exception as e:
            print(f"    ERROR running delete op {i+1}: {e}")
            latency = -1.0 # Indicate error

        results.append({
            'operation': 'delete',
            'delete_type': d_type,
            'delete_params': delete_params,
            'latency_s': round(latency, 4)
        })

    print("--- Delete Benchmarks Complete ---")
    return results


# --- Results Handling ---

def save_results(results: List[Dict[str, Any]], config: Dict[str, Any], db_info: Dict[str, Any]):
    """Saves the benchmark results to a CSV file."""
    output_file_rel = config['results']['output_file']
    script_dir = Path(__file__).parent
    output_file = (script_dir / '..' / output_file_rel).resolve() # Assumes output file relative to project root
    output_file.parent.mkdir(parents=True, exist_ok=True) # Ensure directory exists

    print(f"\nSaving results to: {output_file}")

    if not results:
        print("No results to save.")
        return

    # Flatten results for CSV
    flat_results = []
    run_timestamp = datetime.now().isoformat()

    for res in results:
        flat_res = {
            'run_timestamp': run_timestamp,
            'db_type': db_info.get('type'),
            'db_host': db_info.get('host'),
            'db_details': f"{db_info.get('bucket', '')}/{db_info.get('measurement', '')}" if db_info.get('type') == 'influxdb' else '', # Add more DB specific details
            **res # Spread the original result dictionary
        }
        # Handle nested dicts like query_params or delete_params by converting to string or extracting keys
        for key, value in list(flat_res.items()):
             if isinstance(value, dict):
                 # Option 1: Convert dict to string
                 # flat_res[key] = str(value)
                 # Option 2: Promote keys from nested dict (if simple)
                 if key in ['query_params', 'delete_params']:
                      for sub_key, sub_value in value.items():
                           flat_res[f"{key}_{sub_key}"] = sub_value
                      del flat_res[key] # Remove original nested dict


        flat_results.append(flat_res)

    if not flat_results:
        print("No flattened results to save.")
        return

    # Write to CSV
    try:
        # Determine headers from the first result, assuming structure is consistent
        headers = list(flat_results[0].keys())
        file_exists = output_file.exists()

        with open(output_file, 'a', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader() # Write header only if file is new
            writer.writerows(flat_results)
        print(f"Results appended successfully to {output_file}")
    except Exception as e:
        print(f"ERROR: Failed to save results to CSV: {e}")


# --- Main Orchestration ---

def main():
    """Main function to run the benchmark."""
    all_results = []
    db_handler: Optional[DatabaseInterface] = None

    try:
        config = load_config()
        db_type = config['database']['type']
        db_config = config['database']

        # Load data first
        data_files = load_data_files(config)

        # Get and instantiate the handler
        HandlerClass = get_db_handler(db_type)
        db_handler = HandlerClass()

        # Connect and setup schema
        print(f"\nConnecting to {db_type}...")
        db_handler.connect(db_config)
        db_info = db_handler.get_connection_info()
        print(f"Setting up schema (Bucket/Table: {db_info.get('bucket') or db_info.get('table')})...")
        # Set drop_existing=True for clean benchmark runs
        db_handler.setup_schema(drop_existing=True)

        # Run benchmarks
        insert_results = run_insert_benchmarks(db_handler, data_files, config)
        all_results.extend(insert_results)

        query_results = run_query_benchmarks(db_handler, data_files, config)
        all_results.extend(query_results)

        delete_results = run_delete_benchmarks(db_handler, data_files, config)
        all_results.extend(delete_results)

    except (FileNotFoundError, ValueError, ConnectionError, ModuleNotFoundError, AttributeError, TypeError) as e:
        print(f"\nBenchmark run failed: {e}")
        # No results saving if setup failed
        return
    except Exception as e:
        print(f"\nAn unexpected error occurred during benchmark: {e}")
        # Optionally save partial results if desired
    finally:
        # Save results collected so far, even if errors occurred mid-run
        if all_results and db_handler:
             try:
                 db_info = db_handler.get_connection_info() # Get info again in case connection failed earlier
                 save_results(all_results, config, db_info)
             except Exception as save_e:
                 print(f"ERROR: Failed to save results during cleanup: {save_e}")

        # Ensure connection is closed
        if db_handler:
            print("\nClosing database connection...")
            db_handler.close()

    print("\n--- Benchmark Run Finished ---")


if __name__ == "__main__":
    main()
