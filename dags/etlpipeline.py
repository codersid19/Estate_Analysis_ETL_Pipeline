from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from pymongo import MongoClient

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'real_estate_etl',
    default_args=default_args,
    description='ETL pipeline for real estate data',
    schedule_interval=timedelta(days=1),  # Runs daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    
    def extract_data():
        """Fetch data from the API."""
        url = "https://realestateapi-aqk6.onrender.com/properties"  # Replace with your API URL
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        data = response.json()

        # Save raw data to a temporary file
        with open('/tmp/real_estate_raw.json', 'w') as f:
            f.write(response.text)

    def transform_data():
        """Transform the extracted data."""
        # Load raw data
        df = pd.read_json('/tmp/real_estate_raw.json')

        # Example transformation: Handle missing values and calculate price per sqft
        df['price'] = pd.to_numeric(df['price'], errors='coerce')  # Convert price to numeric
        df['price_per_sqft'] = df['price'] / df['house_area']  # Example calculation
        df.fillna({'furnished': 0, 'garden_area': 0, 'terrace_area': 0}, inplace=True)  # Fill missing values

        # Save the transformed data
        df.to_csv('/tmp/real_estate_transformed.csv', index=False)

    def load_data():
        """Load the transformed data into MongoDB."""
        uri = "mongodb+srv://siddhant19shirodkar12:Admin19@cluster0.7cc2g.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        client = MongoClient(uri)
        db = client['real_estate_db']
        collection = db['properties']

        # Load the transformed data
        df = pd.read_csv('/tmp/real_estate_transformed.csv')

        # Convert DataFrame to a list of dictionaries
        records = df.to_dict(orient='records')

        # Insert records into MongoDB
        collection.insert_many(records)

        print(f"Inserted {len(records)} records into MongoDB Atlas.")
    

    # Define tasks
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
    )


     # Define task dependencies
    extract_task >> transform_task >> load_task