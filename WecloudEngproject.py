#Import libraries
import requests
import pandas as pd
import boto3
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv('Wecloudentrance/AWSkeys.env')  

# Retrieve AWS credentials
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Load configuration from config.toml
config = toml.load('path/to/config.toml')
base_url = config['api']['base_url']
bucket_name = config['s3']['bucket_name']
file_name = config['s3']['file_name']

def fetch_data(base_url, start_page):
    all_data = []
    page = start_page
    while len(all_data) < 50:
        url = f"{base_url}?page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("Failed to fetch data from API")
        page_data = response.json().get('results', [])
        
        if not page_data:
            break

        print(f"Retrieved {len(page_data)} records from page {page}")
        all_data.extend(page_data)

        if len(all_data) >= 50:
            break

        page += 1

    return all_data[:50]

def process_data(data):
    processed_data = []
    for job in data:
        publication_date = job['publication_date']
        job_name = job['name']
        job_type = job['type']
        job_location = job['locations'][0]['name'] if job['locations'] else 'Not Specified'
        company_name = job['company']['name']
        processed_data.append([publication_date, job_name, job_type, job_location, company_name])
    return processed_data

def save_to_s3(data, bucket_name, file_name):
    df = pd.DataFrame(data, columns=['Publication Date', 'Job Name', 'Job Type', 'Job Location', 'Company Name'])
    csv_file = f"{file_name}.csv"
    df.to_csv(csv_file, index=False)

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    
    try:
        s3.upload_file(csv_file, bucket_name, csv_file)
        print("File uploaded to S3")
    except Exception as e:
        print("An error occurred:", e)

def main():
    base_url = "https://www.themuse.com/api/public/jobs"
    start_page = 1
    bucket_name = "mywecloudawsbucket"
    file_name = "job_data"
    
    print("Starting data fetch...")
    data = fetch_data(base_url, start_page)
    print(f"Data fetched: {len(data)} records")

    print("Processing data...")
    processed_data = process_data(data)
    print(f"Data processed: {len(processed_data)} records")

    print("Saving data to S3...")
    save_to_s3(processed_data, bucket_name, file_name)
    print("Script execution completed.")

if __name__ == "__main__":
    main()
