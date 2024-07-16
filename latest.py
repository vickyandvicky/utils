import boto3
from datetime import datetime

def get_latest_file(bucket_name, prefix):
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    if 'Contents' not in response:
        print(f"No files found in bucket {bucket_name} with prefix {prefix}.")
        return None
    
    # Sort the objects by LastModified in descending order
    sorted_objects = sorted(response['Contents'], key=lambda obj: obj['LastModified'], reverse=True)
    
    latest_file = sorted_objects[0]
    latest_file_key = latest_file['Key']
    last_modified = latest_file['LastModified']
    
    print(f"Latest file: {latest_file_key}")
    print(f"Last modified: {last_modified}")

    return latest_file_key

# Replace 'your-bucket-name' with your S3 bucket name and 'abc/' with your prefix
bucket_name = 'your-bucket-name'
prefix = 'abc/'
latest_file_key = get_latest_file(bucket_name, prefix)
