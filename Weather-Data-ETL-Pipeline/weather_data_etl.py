import os
import requests
import pandas as pd
import boto3
import json
from datetime import datetime
from geopy.geocoders import Nominatim
from dotenv import load_dotenv

class WeatherDataFetcher:
    def __init__(self, locations, start_date, end_date):
        self.locations = locations  # Now accepting a list of locations
        self.start_date = start_date
        self.end_date = end_date
        self.geolocator = Nominatim(user_agent="my_geocoder")
        self.base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"  # Updated API endpoint
        self.params = 'TMAX,TMIN,RH2M,PRECTOTCORR,WS2M'
        self.log_directory = r"P:\2025 _Data Engineering Projects\ETL_Projects\Weather_API\Logs"
        self.data_directory = r"P:\2025 _Data Engineering Projects\ETL_Projects\Weather_API\Data"
        self.info_log_file = None
        self.error_log_file = None

    def _setup_logger(self):
        """Sets up info and error logging with date-specific folders."""
        today_date = datetime.now().strftime('%Y-%m-%d')
        info_log_dir = os.path.join(self.log_directory, "Info_logs", today_date)
        error_log_dir = os.path.join(self.log_directory, "Error_logs", today_date)
        
        os.makedirs(info_log_dir, exist_ok=True)
        os.makedirs(error_log_dir, exist_ok=True)

        self.info_log_file = os.path.join(info_log_dir, f"info_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        self.error_log_file = os.path.join(error_log_dir, f"error_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")

    def get_coordinates(self, location):
        """Fetch coordinates of the location."""
        try:
            location_data = self.geolocator.geocode(location)
            if not location_data:
                self._log_error(f"Location '{location}' not found.")
            return location_data.longitude, location_data.latitude
        except Exception as e:
            self._log_error(f"Error fetching coordinates for {location}: {e}")
            return None, None
        
    def fetch_weather_data(self, longitude, latitude):
        """Fetch weather data from the API, and return None if response is invalid."""
        try:
            url = f"{self.base_url}?parameters={self.params}&community=RE&longitude={longitude}&latitude={latitude}&start={self.start_date}&end={self.end_date}&format=JSON"
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
            else:
                self._log_error(f"API request failed with status code {response.status_code} for coordinates: ({longitude}, {latitude})")
                return None
        except Exception as e:
            self._log_error(f"Error fetching weather data for coordinates ({longitude}, {latitude}): {e}")
            return None
        
    def process_weather_data(self, weather_json, location):
        """Process raw weather data into a structured DataFrame."""
        try:
            records = weather_json.get('properties', {}).get('parameter', {})
            if not records:
                self._log_error(f"No weather data available for {location}.")
                return pd.DataFrame()

            weather_df = pd.DataFrame.from_dict(records)

            # Extract year, month, and day from the index
            weather_df = weather_df[['RH2M', 'WS2M', 'PRECTOTCORR', 'T2M_MAX', 'T2M_MIN']]
            weather_df.columns = ['Humidity','Wind_Speed','Precipitation','Temperature_Max', 'Temperature_Min']
            weather_df = weather_df.reset_index().rename(columns={'index': 'date'})
            weather_df['date'] = pd.to_datetime(weather_df['date'], format='%Y%m%d')

            # Add location name to the DataFrame
            weather_df['state'] = location
            weather_df = weather_df[['date', 'state', 'Temperature_Max', 'Temperature_Min', 'Humidity', 'Precipitation', 'Wind_Speed']]

            return weather_df
        except KeyError as e:
            self._log_error(f"Missing expected data field for {location}: {e}")
            return pd.DataFrame()
        except Exception as e:
            self._log_error(f"Error processing weather data for {location}: {e}")
            return pd.DataFrame()

    def save_merged_weather_data(self, weather_df_list):
        """Merge all weather DataFrames and save them to a CSV file."""
        try:
            merged_weather_df = pd.concat(weather_df_list, ignore_index=True)

            # Create today's date folder if it doesn't exist
            today_date = datetime.now().strftime('%Y-%m-%d')
            today_folder = os.path.join(self.data_directory, today_date)
            if not os.path.exists(today_folder):
                os.makedirs(today_folder)

            file_name = f"weather_data_{self.start_date}_{self.end_date}.csv"
            file_path = os.path.join(today_folder, file_name)

            merged_weather_df.to_csv(file_path, index=False)
            return file_path
        except Exception as e:
            self._log_error(f"Error saving merged weather data: {e}")
            return None

    def upload_csv_to_s3(self):
        """
        Uploads all CSV files from the current day's data folder to the S3 bucket.
        """
        load_dotenv()

        # Get AWS credentials and S3 configuration from environment variables
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        region_name = os.getenv('AWS_REGION', 'us-east-1')  # Default to 'us-east-1' if not specified
        bucket_name = os.getenv('S3_BUCKET_NAME')

        # Ensure all required variables are set
        if not aws_access_key_id or not aws_secret_access_key or not bucket_name:
            self._log_error("Error: AWS credentials or bucket name are missing in the environment variables.")
            return

        # Initialize S3 client with the loaded credentials
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        # Get today's date
        today_date = datetime.now().strftime('%Y-%m-%d')

        # Define today's folder path
        today_folder = os.path.join(self.data_directory, today_date)

        # Check if today's folder exists
        if not os.path.exists(today_folder):
            self._log_error(f"Error: Today's folder {today_folder} does not exist.")
            return

        # Loop through all files in the local folder (current day's folder)
        for file_name in os.listdir(today_folder):
            if file_name.endswith('.csv'):  # Process only CSV files
                local_file_path = os.path.join(today_folder, file_name) 

                # Define the S3 object key (path inside the bucket)
                s3_object_key = f"{today_date}/{file_name}"  # Upload to a folder named by today's date

                try:
                    # Upload the file to S3
                    s3.upload_file(local_file_path, bucket_name, s3_object_key)
                    self._log_info(f"Successfully uploaded {file_name} to {s3_object_key}")
                except Exception as e:
                    self._log_error(f"Failed to upload {file_name}: {e}")

    def _log_info(self, message):
        """Logs an info message if the info log file is set."""
        if self.info_log_file:
            log_entry = {
                "Date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "Content": message,
                "Path": self.info_log_file
            }
            self._write_log(self.info_log_file, log_entry)

    def _log_error(self, message):
        """Logs an error message if the error log file is set."""
        if self.error_log_file:
            log_entry = {
                "Date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "Content": message,
                "Path": self.error_log_file
            }
            self._write_log(self.error_log_file, log_entry)

    def _write_log(self, log_file, log_entry):
        """Writes the log entry as a JSON object to the file."""
        try:
            with open(log_file, 'a') as f:
                json.dump(log_entry, f)
                f.write("\n")
        except Exception as e:
            print(f"Error writing to log file {log_file}: {e}")

    def fetch_and_process_weather(self):
        """Main method to fetch, process, and save weather data."""
        try:
            self._setup_logger()  # Set up logger

            all_weather_data = []

            for location in self.locations:
                # Fetch coordinates
                longitude, latitude = self.get_coordinates(location)

                # Fetch weather data
                weather_json = self.fetch_weather_data(longitude, latitude)

                # Process the weather data
                weather_df = self.process_weather_data(weather_json, location)
                
                all_weather_data.append(weather_df)

            if all_weather_data:
                # Merge all the weather data
                merged_weather_df = pd.concat(all_weather_data, ignore_index=True)

                # Remove duplicate rows based on 'date' and 'state'
                merged_weather_df = merged_weather_df.drop_duplicates()

                # Save the merged data to a single CSV
                file_path = self.save_merged_weather_data([merged_weather_df])

                # If everything is successful, log the info
                self._log_info(f"Weather data successfully fetched and saved to: {file_path}")

                self.upload_csv_to_s3()  # Upload CSV files from today's folder

            else:
                self._log_error("No weather data fetched or processed successfully.")

        except Exception as e:
            # If any error occurs, log the error but skip info log
            self._log_error(f"Error in fetching and processing weather data: {e}")


if __name__ == "__main__":
    # Example Usage with multiple locations
    locations = ['Chennai']
    start_date = "20241201"
    end_date = "20241231"

    weather_fetcher = WeatherDataFetcher(locations, start_date, end_date)
    weather_fetcher.fetch_and_process_weather()  # Fetch, process, and upload weather data

    print('\nExecution Successfully Completed\n') 