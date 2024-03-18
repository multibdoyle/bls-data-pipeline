# bls-data-pipeline
Code, assets and associated documentation for BLS data syncing and reporting
## Task 1 - BLSDataUpdate.py and Associated BLS Datasets
The script republishes the open dataset at https://download.bls.gov/pub/time.series/pr/ to the S3 bucket 'bls-data-sharing.'
The script, when executed, should keep the s3 bucket in sycn with the source when data on the website is updated, added or deleted. The script should be able to handle added or removed files, and will not upload the same file more than once. 
Before running, ensure your AWS environment variables are set (AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY). 
Below are links to each file's S3 download location; these pre-signed URLs are valid for about 400 days:


* [pr.class.csv](https://bls-data-sharing.s3.amazonaws.com/pr.class.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=CsBdqaFPEqVkA2nnXXIaKbiKFvc%3D&Expires=1746727155)
* [pr.contacts.csv](https://bls-data-sharing.s3.amazonaws.com/pr.contacts.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=aR5FmL5A5MEARMKsUHqHO3ta3xE%3D&Expires=1746727155)
* [pr.data.0.Current.csv](https://bls-data-sharing.s3.amazonaws.com/pr.data.0.Current.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=wBD1VYQyeq2JSMq75cfKEHZY47E%3D&Expires=1746727155)
* [pr.data.1.AllData.csv](https://bls-data-sharing.s3.amazonaws.com/pr.data.1.AllData.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=P0oHhFIV%2BcIsQ7mEKoTQQg%2F1Nr4%3D&Expires=1746727155)
* [pr.duration.csv](https://bls-data-sharing.s3.amazonaws.com/pr.duration.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=Q42UmMSoVDVwL9kljRu0fi5EfJ0%3D&Expires=1746727155)
* [pr.footnote.csv](https://bls-data-sharing.s3.amazonaws.com/pr.footnote.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=nHWJkgRTEK0oMB2t4ya8SqPYhMY%3D&Expires=1746727155)
* [pr.measure.csv](https://bls-data-sharing.s3.amazonaws.com/pr.measure.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=WxHoFuGgkiAVplozoDj9LAHT0YE%3D&Expires=1746727155)
* [pr.period.csv](https://bls-data-sharing.s3.amazonaws.com/pr.period.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=7TAgG3%2F0CGfuviJUPVIYIhp%2Fl4I%3D&Expires=1746727155)
* [pr.seasonal.csv](https://bls-data-sharing.s3.amazonaws.com/pr.seasonal.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=eS8d5w%2FvUGN1mM3Z0UInJNIlLnE%3D&Expires=1746727155)
* [pr.sector.csv](https://bls-data-sharing.s3.amazonaws.com/pr.sector.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=CggF3gKgkT0TX87T8%2FFNd7pYwj4%3D&Expires=1746727155)
* [pr.series.csv](https://bls-data-sharing.s3.amazonaws.com/pr.series.csv?AWSAccessKeyId=AKIAYAMWB76KNHT5AIMB&Signature=fCPmGaGmy6v6Pe1lumjmAgcPVhc%3D&Expires=1746727155)


## Task 2 - Source code 
The script
