import random
import csv  # Added for CSV writing
import os   # Added for directory creation
import time
from datetime import datetime, timedelta
from faker import Faker
import numpy as np
from tqdm import tqdm  # For progress bars

fake = Faker()

class HighFrequencyDataGenerator:
    def __init__(self):
        self.sensor_types = {
            'temperature': {'unit': '°C', 'range': (20, 40), 'variance': 0.5},
            'humidity': {'unit': '%', 'range': (30, 70), 'variance': 1},
            'gyro_x': {'unit': 'm/s²', 'range': (0, 10), 'variance': 0.3},
            'gyro_y': {'unit': 'm/s²', 'range': (0, 10), 'variance': 0.3},
            'gyro_z': {'unit': 'm/s²', 'range': (0, 10), 'variance': 0.3},
            'acceleration_x': {'unit': 'm/s²', 'range': (-10, 10), 'variance': 0.5},
            'acceleration_y': {'unit': 'm/s²', 'range': (-10, 10), 'variance': 0.5},
            'acceleration_z': {'unit': 'm/s²', 'range': (-10, 10), 'variance': 0.5},
            'magnetometer_x': {'unit': 'μT', 'range': (-100, 100), 'variance': 5},
            'magnetometer_y': {'unit': 'μT', 'range': (-100, 100), 'variance': 5},
            'magnetometer_z': {'unit': 'μT', 'range': (-100, 100), 'variance': 5},
        }
    
    def generate_sensor_value(self, sensor_name, previous_value=None):
        """Generate realistic sensor values with temporal dependencies"""
        sensor = self.sensor_types[sensor_name]
        
        if previous_value is None:
            value = random.uniform(*sensor['range'])
        else:
            # Make values temporally correlated (new value depends on previous)
            change = random.normalvariate(0, sensor['variance'])
            value = previous_value + change
            # Ensure value stays within range
            value = max(sensor['range'][0], min(sensor['range'][1], value))
        
        # Add small noise
        value += random.uniform(-0.01, 0.01)
        
        # Apply consistent precision
        return round(value, 2)

    def generate_sample(self, timestamp, previous_values=None):
        """Generate one high-frequency sample with realistic sensor relationships"""
        if previous_values is None:
            previous_values = {}
        sample = {"timestamp": timestamp.isoformat(), "anomaly_flag": False} # Initialize anomaly_flag

        # Generate base sensors
        for sensor in self.sensor_types:
            sample[sensor] = self.generate_sensor_value(
                sensor,
                previous_values.get(sensor)
            )

        # Add 5% chance of anomaly
        if random.random() < 0.05:
            anomaly_sensor = random.choice(list(self.sensor_types.keys()))
            # Apply significant deviation for anomaly
            anomaly_factor = random.uniform(1.5, 10) * random.choice([-1, 1]) # Can be positive or negative anomaly
            sample[anomaly_sensor] += anomaly_factor * self.sensor_types[anomaly_sensor]['variance'] * 10 # Scale anomaly by variance
            # Ensure value stays within range even after anomaly
            sensor_range = self.sensor_types[anomaly_sensor]['range']
            sample[anomaly_sensor] = max(sensor_range[0], min(sensor_range[1], sample[anomaly_sensor]))
            sample[anomaly_sensor] = round(sample[anomaly_sensor], 2) # Keep precision
            sample['anomaly_flag'] = True

        return sample

    def generate_and_write_recording(self, filename, duration_minutes):
        """Generate a high-frequency recording and write directly to CSV"""
        duration_seconds = duration_minutes * 60
        num_samples = duration_seconds * 100  # 100Hz sample rate

        start_time = datetime.now() - timedelta(days=random.randint(0, 30))
        previous_values = None

        # Define CSV header based on sensor types + timestamp + anomaly flag
        header = ['timestamp'] + list(self.sensor_types.keys()) + ['anomaly_flag']

        print(f"Generating {num_samples:,} samples for {os.path.basename(filename)}...")

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(header) # Write header

            for i in tqdm(range(num_samples)):
                current_time = start_time + timedelta(seconds=i/100)
                sample = self.generate_sample(current_time, previous_values)

                # Prepare row data in the order of the header
                row = [sample['timestamp']]
                for sensor_key in self.sensor_types.keys():
                    row.append(sample.get(sensor_key, '')) # Use get for safety, though generate_sample should provide all
                row.append(sample.get('anomaly_flag', False)) # Add anomaly flag

                writer.writerow(row)
                previous_values = sample # Update previous values for next iteration


# Usage example
if __name__ == "__main__":
    generator = HighFrequencyDataGenerator()

    print("Generating high-frequency benchmark dataset (CSV format)...")
    start_time = time.time()

    # --- Configuration ---
    NUM_USERS = 5               # Number of unique users
    RECORDINGS_PER_USER = 10     # Recordings per user
    OUTPUT_DIR = "hf_data_csv"   # Directory for CSV files
    # ---------------------

    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"Output directory: '{OUTPUT_DIR}'")

    total_recordings = 0
    for user_idx in range(NUM_USERS):
        user_id = f"user_{user_idx:04d}"
        print(f"\nGenerating recordings for {user_id}...")

        for rec_idx in range(RECORDINGS_PER_USER):
            recording_id = f"rec_{user_idx:04d}_{rec_idx:04d}"
            duration = random.choice([5, 15, 30, 60, 90, 150])  # minutes
            filename = os.path.join(OUTPUT_DIR, f"recording_{recording_id}.csv")

            generator.generate_and_write_recording(
                filename=filename,
                duration_minutes=duration
            )
            total_recordings += 1

    end_time = time.time()
    print(f"\n--- Generation Complete ---")
    print(f"Generated {total_recordings} recordings.")
    print(f"Total time: {end_time - start_time:.2f} seconds")
    print(f"CSV files saved in '{OUTPUT_DIR}' directory.")
