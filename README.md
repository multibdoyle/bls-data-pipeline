# bls-data-pipeline
Code, assets and associated documentation for BLS data syncing and reporting
## About BLSDataUpdate.py
The script republishes the open dataset at https://download.bls.gov/pub/time.series/pr/ to the S3 bucket 'bls-data-sharing.'
The script, when executed, should keep the s3 bucket in sycn with the source when data on the website is updated, added or deleted. The script should be able to handle added or removed files, and will not upload the same file more than once. 
Before running, ensure your AWS environment variables are set (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY). 
