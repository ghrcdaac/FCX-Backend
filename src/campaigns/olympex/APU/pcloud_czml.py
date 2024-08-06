import boto3
from ingest_utils import generator_to_df, color_encode, get_lat_lon_by_site_id
import pandas as pd
import json
import os
from pathlib import Path
import shutil

def makeSinglePoint(fdate, apu_number):
    fdate = fdate.replace('-', '')
    s3_resource = boto3.resource('s3')
    bucket_name = "ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "apu"
    s3bucket = s3_resource.Bucket(bucket_name)
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/{apu_number}/olympex_{apu_number}_{fdate}"):
        keys.append(obj.key)

    result = keys
    print(keys)
    s3_client = boto3.client('s3')

    for infile in result:    
        s3_file = s3_client.get_object(Bucket=bucket_name, Key=infile)
        data = s3_file['Body'].iter_lines()
        
        data = generator_to_df(data)
        data = pd.DataFrame(data)
        data.columns = ['Year', 'Day', 'Hour', 'Min', 'drops', 'conc', 'water content', 'rain rate', 'reflectivity', 'mass-weight d.d', 'max d.d']
        
        data['Timestamp'] = pd.to_datetime(fdate + ' ' + data['Hour'].astype(str) + ':' + data['Min'].astype(str), format='%Y%m%d %H:%M')
        data['Seconds'] = pd.to_timedelta(data['Timestamp'].dt.time.astype(str)).dt.total_seconds()

        encoded_colors = color_encode(data, data['rain rate'].min(), data['rain rate'].max(), RGBA=False)

        fixed_lat, fixed_lon = get_lat_lon_by_site_id(apu_number)

        main_dict = {}
        for index, row in data.iterrows():
            row_dict = {index: {"id": int(data['Seconds'].iloc[index].item()), "r": encoded_colors[index][0].item(), "b": encoded_colors[index][2].item(), "Lon": [fixed_lon], "Lat": [fixed_lat]}}
            main_dict.update(row_dict)
        main_dict
        
        folder = f"/Users/Indhuja/Desktop/APU/{apu_number}/results"
        local_file_path = f"{folder}/olympex_{apu_number}_{fdate}_rainparameter_min.json"
        if os.path.exists(folder): shutil.rmtree(f"{folder}")
        # os.mkdir(folder)
        Path(folder).mkdir(parents=True, exist_ok=True)
        outfile = local_file_path.split("/")[-1]
        with open(local_file_path, "w") as f:
            json.dump(main_dict, f, indent=4)

        s3_file_key = f"{field_campaign}/{output_data_dir}/{instrument_name}/{apu_number}/{outfile}"
        with open(local_file_path, "rb") as f:
            s3_client.upload_fileobj(f, bucket_name, s3_file_key)


# for different apu##, find few common dates
fdate = "2015-12-03"
apu_number = "apu08"
source_path = f"/Users/Indhuja/Desktop/APU/olympex_{apu_number}_{fdate}_rainparameter_min.txt" #dealing with rain rate alone
makeSinglePoint(fdate, apu_number)
# print(os.getcwd())