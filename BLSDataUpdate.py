import requests
from bs4 import BeautifulSoup
import boto3
import os
import pandas as pd
import csv
from io import BytesIO, StringIO
from datetime import datetime
import json
import logging
### Creating Boto3 client

s3 = boto3.client(
        's3'
        )
### To determine what type of file should be written, CSV or txt
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def file_type_decision(content):
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(content)
        logger.info(f"This file is likely a CSV (Comma-Separated Values) file with '{dialect.delimiter}' as the delimiter.")
        return "csv", dialect.delimiter
    except csv.Error:
        # Not a CSV/TSV
        logger.info("This file is likely a plain text file, will write as .txt")
        return "txt", None


### Get list of current BLS datasets
def get_current_bls_datasets(url):
    headers = {
    'User-Agent': 'EDAMarch2024_for_DataEngineering/1.0 (bdoyle.core@gmail.com; Fetching data for academic research purposes)'
}
    response = requests.get(url, headers = headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    a_links = soup.find_all('a')
    data = []
    for link in a_links:
        text = link.find_previous_sibling(text=True)
        print(text)
        if text and ('AM' in text or 'PM' in text):  # Grouped AM/PM checks
            clean_text = ' '.join(text.strip().split())
            data.append({'metadata': clean_text, 'filePath': link.get('href')})
    df = pd.DataFrame(data)
    df[['date', 'time', 'timeOfDay', 'metadataValue']] = df['metadata'].str.split(" ", expand=True, n=3)
    df['dateOfUpdate'] = df['date'].astype(str) + ' '+ df['time'].astype(str) + ' '+df['timeOfDay'].astype(str)
    tuple_pair_dict = dict(zip(df['filePath'], df['dateOfUpdate']))
    return tuple_pair_dict
bls_files= get_current_bls_datasets('https://download.bls.gov/pub/time.series/pr/')

### Get current list of files in s3, including associated metadata on time of update from BLS website (if available)

def get_current_s3_info(bucket_name):
    response = s3.list_objects_v2(Bucket=bucket_name)
    s3_resource = boto3.resource('s3')
    files = []
    file_data = []
    try:
        for file in response['Contents']:
            name = file['Key']
            last_modified = file['LastModified']
            data = {'filename':name, 'lastModified':last_modified}
            files.append(data)
        for file in files:
            metadata_response = s3.head_object(Bucket='bls-data-sharing', Key=file['filename'])
            metadata = metadata_response.get('Metadata', {})
            for key, value in metadata.items():
                if key == 'bls-last-update':
                    update_time = value
                    file_data.append({'filename':file['filename'], 'bls-update-date':value})
        file_data_df = pd.DataFrame(file_data)
        tuple_pair_dict = dict(zip(file_data_df['filename'], file_data_df['bls-update-date']))
        return tuple_pair_dict
    except:
        print(f"Check response for {bucket_name} for contents")
        file_data.append({'filename':None, 'bls-update-date':None})
        file_data_df = pd.DataFrame(file_data)

        tuple_pair_dict = dict(zip(file_data_df['filename'], file_data_df['bls-update-date']))
        return tuple_pair_dict



def files_to_update(dict1, dict2):
    # Convert all date strings in the dictionaries to datetime objects for accurate comparison.
    base_path = '/pub/time.series/pr/'

    # Skip conversion if the dictionary has {None: None}
    if dict1 != {None: None}:
        for key in list(dict1.keys()):  # Use list to avoid RuntimeError for changing dict size during iteration
            if key is not None and dict1[key] is not None:
                dict1[key] = datetime.strptime(dict1[key], '%m/%d/%Y %I:%M %p')
            else:
                del dict1[key]  # Remove None entries

    if dict2 != {None: None}:
        for key in list(dict2.keys()):  # Use list to avoid RuntimeError for changing dict size during iteration
            if key is not None and dict2[key] is not None:
                dict2[key] = datetime.strptime(dict2[key], '%m/%d/%Y %I:%M %p')
            else:
                del dict2[key]  # Remove None entries

    # Adding base path and ensuring keys are valid before adding
    dict2 = {base_path + key: value for key, value in dict2.items() if key is not None}

    # Prepare a modified version of dict2 with stripped file extensions for comparison.
    dict2_stripped = {key.rsplit('.', 1)[0]: value for key, value in dict2.items() if key is not None}

    # Compare and find unique files in dict1.
    unique_files = []
    for file, date in dict1.items():
        if file not in dict2_stripped or dict2_stripped.get(file) != date:
            unique_files.append((file, date.strftime('%m/%d/%Y %I:%M %p')))

    return unique_files


#Download files and upload to s3
def upload_files_to_s3(updates_needed, bucket_name):
    headers = {
        'User-Agent': 'EDAMarch2024_for_DataEngineering/1.0 (email@example.com; Fetching data for academic research purposes)'
    }
    for path, timestamp in updates_needed:
        full_url = 'https://download.bls.gov' + path
        response = requests.get(full_url, headers=headers)
        print(f'for {full_url} the response was {response.status_code}')
        file_name = path.replace('/pub/time.series/pr/', "")
        content = response.content.decode('utf-8')
        file_type, delimiter = file_type_decision(content)
        s3_key = file_name + '.' + ('csv' if file_type == 'csv' else 'txt')
        metadata = {
            'bls-last-update': timestamp
        }
        if file_type == 'csv':
            print(f"Going to write {s3_key} to {bucket_name}")
            # If the file is detected as CSV, we'll assume it's a TSV (tab-separated) as per your data sample.
            delimiter = '\t'  # Set the delimiter to a tab explicitly.
            csv_content = StringIO(content)
            with StringIO() as output_csv:
                writer = csv.writer(output_csv, delimiter=',', quoting=csv.QUOTE_MINIMAL)
                for line in csv.reader(csv_content, delimiter=delimiter):
                    writer.writerow(line)
                output_csv.seek(0)
                s3.put_object(Bucket=bucket_name, Key=s3_key, Body=output_csv.getvalue(), Metadata=metadata)
                print(f'file {s3_key} was written')
        elif file_type == 'txt':
            print(f"Going to write {s3_key} to {bucket_name}")
            s3.put_object(Bucket=bucket_name, Key=s3_key, Body=content, Metadata=metadata)
            print(f'file {s3_key} was written')
        elif path ==None:
            print("No file to write")

                

### Syncing files
def sync_files(source_url, bucket_name):
    bls_current_files = get_current_bls_datasets(source_url)
    s3_current_files = get_current_s3_info(bucket_name)
    update_files = files_to_update(bls_current_files, s3_current_files)
    upload_files_to_s3(update_files, bucket_name)
    logger.info("Syncing BLS data...")
    
    ### Make sure source_files includes file metadata, names
    ### Get S3 file info
    ### needs_update() returns dictionary of files to update
    #### Upload to S3 uploads only files from needs_update dictionary

source_url = 'https://download.bls.gov/pub/time.series/pr/'
bucket_name = 'bls-data-sharing'
sync_files(source_url, bucket_name)