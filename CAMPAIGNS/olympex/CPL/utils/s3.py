import os
import boto3
from botocore.exceptions import ClientError, NoCredentialsError

# a function to list the name of files inside a s3 dir.
def fileList(bucket_name, path_prefix):
    """list out the files that are avaible in the s3 bucket + path_prefix.
    similar to doing 'ls' in linux, but inside a `s3bucket/prefix`

    Args:
        bucket_name (String): The name of the bucket, where the list to be obtained from.
        path_prefix (String): The prefix path inside the bucket, where the file list is to be obtained from.
                            example: "Olympex/instrument-raw-data/{instrument-raw-data}/olympex")
    """    
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(Prefix=path_prefix):
        keys.append(obj.key)

    return keys

# a function to upload the data to a given s3 dir.
def uploadFiles(bucket_name, src, dest):
    """uploads multiple files from local source directory to s3 destination

    Args:
        src (string): source destination in local
        dest (string): s3 destination path
    """
    # UPLOAD CONVERTED FILES.
    s3 = boto3.client('s3')
    files = os.listdir(src)
    for file in files:
        sourcePath = os.path.join(src, file) # SOURCE
        s3name = f"{dest}/{file}" # DESTINATION
        uploadFile(sourcePath, bucket_name, s3_name=s3name)
        print(f"uploaded to {s3name}.")

def uploadFile(file_name, bucket, s3_name=None):
    """Upload a single file to an S3 bucket
     file_name: File to upload
     bucket: S3 bucket to upload to
     object_name: S3 object name. If not specified then file_name is used
    """
    if s3_name is None: s3_name = file_name

    s3 = boto3.client('s3')
    try:
        s3.upload_file(file_name, bucket, s3_name)
    except ClientError as e:
       print(e)
    except NoCredentialsError:
        print("%%Credentials not available")