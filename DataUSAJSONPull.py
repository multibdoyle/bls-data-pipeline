import json
import requests
import boto3

def api_data_fetcher(url, headers, bucket_name, object_name):
    s3 = boto3.client('s3')
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    json_data = json.dumps(data)
    s3.put_object(Bucket=bucket_name, Key=object_name, Body=json_data)
    print(f'{object_name} was written to {bucket_name}')

url = 'https://datausa.io/api/data?drilldowns=Nation&measures=Population'
headers = {}  # Add any necessary headers here
bucket_name = 'bls-data-sharing'  # Replace with your S3 bucket name
object_name = 'datausa_population.json'

api_data_fetcher(url, headers, bucket_name, object_name)
    
    