

# **Weather Data ETL Pipeline**

![image](https://github.com/user-attachments/assets/b1deed9e-f46a-4d8f-930a-3f2dae955af6)



---

### YouTube:
[Weather-Data-ETL-Pipeline](https://www.linkedin.com/in/praveennarasimman/)

---

### Project Overview:
The **Weather Data ETL Pipeline** is an automated system designed to extract, transform, and load (ETL) weather data from the **NASA POWER API** for specified locations and date ranges. The system fetches weather data, processes it into a structured format, stores it locally, and uploads it to an **Amazon S3** bucket for future analysis and reporting. The pipeline is designed to be scalable, efficient, and easily deployable.

---

### Features:
- **Data Extraction**: 
  - Fetch weather data from the NASA POWER API for multiple locations.
  - Automatically fetch geographic coordinates for each location using the **Nominatim API**.
  
- **Data Transformation**: 
  - Process raw data into a structured **DataFrame** format (Temperature, Humidity, Wind Speed, Precipitation).
  - Handle missing or invalid data gracefully.
  
- **Data Loading**: 
  - Save the transformed data to **CSV** files.
  - Upload the processed files to **Amazon S3** for downstream Tasks.
  
- **Logging**: 
  - Implement **info** and **error logging** for every step in the ETL pipeline.
  - Logs are stored in a date-specific folder for easy troubleshooting.
  
- **Cloud Integration**: 
  - Upload processed weather data to **Amazon S3** with a dynamic folder structure for each execution day.

---

### Architecture:
1. **Data Extraction**:
   - Fetch coordinates (latitude, longitude) for given locations.
   - Request weather data from the NASA POWER API.

2. **Data Transformation**:
   - Process weather data (Temperature, Wind Speed, Precipitation) into a **pandas DataFrame**.
   - Cleanse data by converting date formats and handling missing values.

3. **Data Loading**:
   - Save the processed data as **CSV** files.
   - Upload the files to **Amazon S3**.

4. **Logging**:
   - Logs are saved in **JSON format** with date-specific directories.
   - Logs contain information about successful steps and errors encountered.

---

### Prerequisites:
1. Python 3.10
2. Required Python Libraries:
   - **requests**: To fetch data from APIs.
   - **pandas**: For data processing and transformation.
   - **boto3**: For interacting with AWS S3.
   - **geopy**: For geocoding locations.
   - **python-dotenv**: For managing environment variables.

---

### Setup Instructions:
1. **Clone the Repository**:
   ```bash
   git clone https://github.com/<your-username>/Weather-Data-ETL-Pipeline.git
   cd Weather-Data-ETL-Pipeline
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables**:
   Create a `.env` file in the project root directory with the following details:
   ```
   AWS_ACCESS_KEY_ID=<your-aws-access-key>
   AWS_SECRET_ACCESS_KEY=<your-aws-secret-key>
   AWS_REGION=<your-aws-region>
   S3_BUCKET_NAME=<your-s3-bucket-name>
   ```

4. **Run the Pipeline**:
   ```bash
   python weather_data_etl.py
   ```

---

### Code Explanation:
1. **WeatherDataFetcher Class**: This class handles the entire ETL pipeline:
   - **`get_coordinates`**: Fetches the latitude and longitude for each location.
   - **`fetch_weather_data`**: Retrieves weather data from the NASA POWER API.
   - **`process_weather_data`**: Transforms raw weather data into a structured DataFrame.
   - **`save_merged_weather_data`**: Saves the transformed data to a local CSV file.
   - **`upload_csv_to_s3`**: Uploads CSV files to an S3 bucket.
   - **`_setup_logger`**: Sets up logging for both info and error messages.
   
2. **Logging**:
   - Logs are saved in `JSON` format under date-specific directories, ensuring traceability and easy debugging.

---

### Example Usage:
```python
locations = ['Chennai', 'Mumbai']
start_date = "20241201"
end_date = "20241231"

weather_fetcher = WeatherDataFetcher(locations, start_date, end_date)
weather_fetcher.fetch_and_process_weather()
```

---

### File Structure:
```
Weather-Data-ETL-Pipeline/
│
├── weather_data_etl.py             # Main script to fetch, process and upload weather data
├── requirements.txt                # Python dependencies
├── .env                            # Environment variables for AWS credentials and configuration
└── README.md                       # Project documentation
```

---

### Logging:
- Info and error logs are generated and saved in the following folder structure:
  ```
  Logs/
  ├── Info_logs/
  │   └── 2025-01-05/
  │       └── info_log_20250105_123456.json
  └── Error_logs/
      └── 2025-01-05/
          └── error_log_20250105_123456.json
  ```

---

### Contribution:
Feel free to fork this repository, make changes, and submit a pull request. Contributions are welcome!

---

### License:
This project is licensed under the MIT License.

---

### Contact:
If you have any questions or need support, please feel free to open an issue or contact me directly via GitHub.

---
