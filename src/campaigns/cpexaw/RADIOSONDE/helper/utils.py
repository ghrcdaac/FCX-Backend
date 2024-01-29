import boto3
from boto3 import client as boto_client
from botocore.exceptions import ClientError, NoCredentialsError

from io import StringIO
import os
import pandas as pd
from datetime import datetime


def get_files(bucket_name="ghrc-fcx-field-campaigns-szg", prefix="CPEX-AW/instrument-raw-data/radiosonde"):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{prefix}/SCRX_Radiosonde_CPEXAW_win_20210914"):
        url = "s3://" + bucket_name + "/" + obj.key
        # url = f"https://{bucket_name}.s3.amazonaws.com" + "/" + prefix + "/" + obj.key
        keys.append(url)
    return keys

def data_reader(s3_url):
    ## Open data file
    bucket_name = s3_url.split("/")[2]
    key = s3_url.split(f"{bucket_name}/")[-1] # need key without starting /
    s3 = boto_client('s3')
    fileobj = s3.get_object(Bucket=bucket_name, Key=key)
    file = fileobj['Body'].read().decode('ISO-8859-1')
    data = pd.read_table(StringIO(file))
    return data


def upload_file(type, source_file_path, bucket_name="ghrc-fcx-field-campaigns-szg", prefix="CPEX-AW/instrument-processed-data/radiosonde"):
  s3 = boto3.client('s3')
  try:
    if(type == "3dTiles"):
        files = os.listdir(source_file_path)
        for file in files:
            fname = os.path.join(source_file_path, file) # SOURCE
            actualprefix = f"{prefix}/{file}" # DESTINATION
        s3.upload_file(fname, bucket_name, actualprefix)
    elif(type == "skewT"):
        s3.upload_file(source_file_path, bucket_name, prefix)
  except ClientError as e:
    print(e)
  except NoCredentialsError:
      print("%%Credentials not available")


def formatted_datetime(input_file):
    date_str, time_str = input_file.split("_")[-2:]
    time_str = time_str.replace(".txt", "")
    datetime_str = f"{date_str}_{time_str}"
    original_datetime = datetime.strptime(datetime_str, "%Y%m%d_%H%M")
    return date_str, time_str, original_datetime.strftime("%Y-%m-%dT%H:%M:%S")

def clean_data(data, column_name_changes):
    data.rename(columns=column_name_changes, inplace=True)
    valid_columns = list(column_name_changes.values())
    data = data[valid_columns]

    data.loc[:, :] = data.replace(r'^-{5,}\s*$', pd.NA, regex=True)
    data = data.map(lambda x: x.strip() if isinstance(x, str) else x) 
    data.dropna(inplace=True)
    return data