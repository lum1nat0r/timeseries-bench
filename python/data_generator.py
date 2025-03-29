import random
import json
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
        
        sample = {"timestamp": timestamp.isoformat()}
        
        # Generate base sensors
        for sensor in self.sensor_types:
            sample[sensor] = self.generate_sensor_value(
                sensor,
                previous_values.get(sensor)
            )

        # Add 5% chance of anomaly
        if random.random() < 0.05:
            anomaly_sensor = random.choice(list(self.sensor_types.keys()))
            sample[anomaly_sensor] *= random.uniform(1.5, 10)
            sample['anomaly_flag'] = True
        
        return sample
    
    def generate_recording(self, user_id, recording_id, duration_minutes):
        """Generate a high-frequency recording"""
        duration_seconds = duration_minutes * 60
        num_samples = duration_seconds * 100  # 100Hz sample rate
        
        start_time = datetime.now() - timedelta(days=random.randint(0, 30))
        samples = []
        previous_values = None
        
        print(f"Generating {num_samples:,} samples for recording {recording_id}...")
        
        for i in tqdm(range(num_samples)):
            current_time = start_time + timedelta(seconds=i/100)
            sample = self.generate_sample(current_time, previous_values)
            samples.append(sample)
            previous_values = sample
        
        return {
            "metadata": {
                "userId": user_id,
                "recordingId": recording_id,
                "name": f"HF Recording {recording_id}",
                "startTime": start_time.isoformat(),
                "durationMinutes": duration_minutes,
                "sampleRateHz": 100,
                "deviceId": f"DEV_{random.randint(1000, 9999)}",
                "tags": {
                    "location": random.choice(["lab", "field", "factory"]),
                    "environment": random.choice(["controlled", "harsh", "outdoor"])
                }
            },
            "samples": samples
        }
    
    def generate_dataset(self, num_users, recordings_per_user):
        """Generate a complete high-frequency dataset"""
        dataset = []
        
        for user_idx in range(num_users):
            user_id = f"user_{user_idx:04d}"
            
            for rec_idx in range(recordings_per_user):
                recording_id = f"rec_{user_idx:04d}_{rec_idx:04d}"
                duration = random.choice([5, 15, 30, 60, 90, 150])  # minutes
                
                recording = self.generate_recording(
                    user_id=user_id,
                    recording_id=recording_id,
                    duration_minutes=duration
                )
                
                dataset.append(recording)
        
        return dataset

# Usage example
if __name__ == "__main__":
    generator = HighFrequencyDataGenerator()
    
    print("Generating high-frequency benchmark dataset...")
    start_time = time.time()
    
    # Adjust these parameters based on your needs
    dataset = generator.generate_dataset(
        num_users=50,               # Number of unique users
        recordings_per_user=10      # Recordings per user
    )
    
    # Save recordings to individual files
    print("\nSaving recordings...")
    for i, recording in enumerate(tqdm(dataset)):
        filename = f"hf_data/recording_{recording['metadata']['recordingId']}.json"
        with open(filename, "w") as f:
            json.dump(recording, f)
    
    print(f"\nDone! Generated {len(dataset)} recordings in {time.time()-start_time:.2f} seconds")
    print("Files saved in 'hf_data' directory.")
