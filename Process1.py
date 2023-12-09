import pandas as pd
import os
import argparse
from datetime import datetime, timedelta


def process_gps_data(parquet_path, output_dir):
    # Read Parquet file
    df = pd.read_parquet(parquet_path)

    # Convert timestamp string to datetime object
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Sort dataframe by unit and timestamp
    df.sort_values(by=['unit', 'timestamp'], inplace=True)

    # Initialize variables for trip identification
    current_unit = None
    current_trip_number = 0
    current_trip_data = []

    # Iterate through rows to identify and store trips
    for _, row in df.iterrows():
        # Check for unit change or time difference exceeding 7 hours
        if (current_unit is None or row['unit'] != current_unit) or (
                current_trip_data and row['timestamp'] - current_trip_data[-1]['timestamp'] > timedelta(hours=7)):
            # Starting a new unit or a new trip
            if current_unit is not None:
                save_trip_to_csv(current_unit, current_trip_number, current_trip_data, output_dir)
            current_unit = row['unit']
            current_trip_number = 0
            current_trip_data = []

        # Append data to the current trip
        current_trip_data.append({
            'latitude': row['latitude'],
            'longitude': row['longitude'],
            'timestamp': row['timestamp'],
        })

    # Save the final trip data if any
    if current_trip_data:
        save_trip_to_csv(current_unit, current_trip_number, current_trip_data, output_dir)

def save_trip_to_csv(unit, trip_number, trip_data, output_dir):
    # Create a DataFrame for the trip
    trip_df = pd.DataFrame(trip_data)

    # Create CSV filename
    csv_filename = f"{unit}_{trip_number}.csv"

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Save trip data to CSV
    trip_df.to_csv(os.path.join(output_dir, csv_filename), index=False)




process_gps_data('/content/raw_data.parquet', '/content/output')


