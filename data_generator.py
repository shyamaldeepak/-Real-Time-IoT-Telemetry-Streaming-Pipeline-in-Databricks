import os
import time
import json
import random
from datetime import datetime

# Configure the directory where raw streaming data will be saved
# By default, it saves to a folder named 'landing_zone' in the current directory.
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "landing_zone")

# List of device IDs to simulate
DEVICES = ["sensor-north-01", "sensor-east-02", "sensor-south-03", "sensor-west-04"]

def generate_telemetry_record():
    """Generates a single mock telemetry reading."""
    device_id = random.choice(DEVICES)
    
    # Normally temperature is between 20C and 35C, but let's occasionally simulate an overheat anomaly (>80C)
    is_anomaly = random.random() < 0.05
    if is_anomaly:
        temperature = round(random.uniform(80.0, 95.0), 2)
        status = "WARNING"
    else:
        temperature = round(random.uniform(18.0, 32.0), 2)
        status = "OK"
        
    # Humidity normally between 40% and 60%
    # Occasionally generate a bad record (null humidity) to test cleaning
    is_corrupt = random.random() < 0.03
    if is_corrupt:
        humidity = None
    else:
        humidity = round(random.uniform(35.0, 65.0), 2)
        
    return {
        "device_id": device_id,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
        "temperature": temperature,
        "humidity": humidity,
        "status": status
    }

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"Created landing zone directory at: {OUTPUT_DIR}")
        
    print("==============================================================")
    print("IoT Real-Time Telemetry Generator Started!")
    print(f"Writing JSON files to: {OUTPUT_DIR}")
    print("Press Ctrl+C to stop.")
    print("==============================================================")
    
    file_counter = 0
    try:
        while True:
            file_counter += 1
            # Generate a batch of 5 records
            batch = [generate_telemetry_record() for _ in range(5)]
            
            # Create a unique filename based ontimestamp
            filename = f"telemetry_{int(time.time())}_{file_counter}.json"
            filepath = os.path.join(OUTPUT_DIR, filename)
            
            # Write batch to JSON file
            with open(filepath, "w") as f:
                json.dump(batch, f, indent=2)
                
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Generated file: {filename} with 5 readings")
            
            # Wait 5 seconds before generating the next batch
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nGenerator stopped by user. Happy streaming!")

if __name__ == "__main__":
    main()
