import os
import requests
import concurrent.futures
import argparse
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

TOLLGURU_API_KEY = os.getenv('TOLLGURU_API_KEY')
TOLLGURU_API_URL = os.getenv('TOLLGURU_API_URL', 'https://apis.tollguru.com/toll/v2/gps-tracks-csv-upload?mapProvider=osrm&vehicleType=5AxlesTruck')

def send_request(file_path, output_dir):
    with open(file_path, 'rb') as file:
        headers = {'x-api-key': TOLLGURU_API_KEY, 'Content-Type': 'text/csv'}
        response = requests.post(TOLLGURU_API_URL, data=file, headers=headers)

        # Extracting filename from the path
        filename = os.path.basename(file_path)

        # Create the output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save JSON response to a file
        json_output_path = os.path.join(output_dir, f"{os.path.splitext(filename)[0]}.json")
        with open(json_output_path, 'w') as json_file:
            json_file.write(response.text)

def process_csv_files(csv_folder, output_dir):
    # Get a list of CSV files in the folder
    csv_files = [f for f in os.listdir(csv_folder) if f.endswith('.csv')]

    # Use concurrent futures to send requests concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Map each CSV file to the send_request function
        executor.map(
            lambda file: send_request(os.path.join(csv_folder, file), output_dir),
            csv_files
        )


process_csv_files('/content/output', '/content/output2')


