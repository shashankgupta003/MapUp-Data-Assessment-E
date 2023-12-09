import os
import json
import csv
import argparse

def process_json_files(json_folder, output_dir):
    # Initialize a list to store toll data for all trips
    all_toll_data = []

    # Get a list of JSON files in the folder
    json_files = [f for f in os.listdir(json_folder) if f.endswith('.json')]

    # Process each JSON file
    for json_file in json_files:
        json_path = os.path.join(json_folder, json_file)

        # Load JSON data from file
        with open(json_path, 'r') as file:
            json_data = json.load(file)

        # Process toll information for each trip
        for trip_info in json_data.get('trips', []):
            toll_data = extract_toll_data(trip_info, json_file)
            if toll_data:
                all_toll_data.append(toll_data)

    # Save the consolidated toll data to a CSV file
    save_to_csv(all_toll_data, output_dir)

def extract_toll_data(trip_info, trip_file):
    toll_data = {}

    # Extract relevant data from the JSON file
    toll_data['unit'] = trip_info.get('unit', '')
    toll_data['trip_id'] = os.path.splitext(trip_file)[0]
    toll_data['toll_loc_id_start'] = trip_info.get('toll_start', {}).get('toll_id', '')
    toll_data['toll_loc_id_end'] = trip_info.get('toll_end', {}).get('toll_id', '')
    toll_data['toll_loc_name_start'] = trip_info.get('toll_start', {}).get('toll_name', '')
    toll_data['toll_loc_name_end'] = trip_info.get('toll_end', {}).get('toll_name', '')
    toll_data['toll_system_type'] = trip_info.get('toll_start', {}).get('toll_system_type', '')
    toll_data['entry_time'] = trip_info.get('toll_start', {}).get('entry_time', '')
    toll_data['exit_time'] = trip_info.get('toll_end', {}).get('exit_time', '')
    toll_data['tag_cost'] = trip_info.get('toll_end', {}).get('tag_cost', '')
    toll_data['cash_cost'] = trip_info.get('toll_end', {}).get('cash_cost', '')
    toll_data['license_plate_cost'] = trip_info.get('toll_end', {}).get('license_plate_cost', '')

    return toll_data

def save_to_csv(all_toll_data, output_dir):
    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create the CSV file path
    csv_path = os.path.join(output_dir, 'transformed_data.csv')

    # Write toll data to CSV file
    with open(csv_path, 'w', newline='') as csvfile:
        fieldnames = [
            'unit', 'trip_id', 'toll_loc_id_start', 'toll_loc_id_end',
            'toll_loc_name_start', 'toll_loc_name_end', 'toll_system_type',
            'entry_time', 'exit_time', 'tag_cost', 'cash_cost', 'license_plate_cost'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write CSV header
        writer.writeheader()

        # Write toll data rows
        writer.writerows(all_toll_data)


process_json_files('/content/output2', '/content/output3')


