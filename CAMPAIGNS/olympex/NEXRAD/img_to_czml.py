import boto3
import os
from itertools import groupby

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, instrument_location):
    # get the instrument data list
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    filenames = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/{instrument_location}/olympex"):
        filenames.append(obj.key)

    # remove tilt 5 degree (ELEV02) data. Only visualizing parallel (ELEV01) data
    filtered_file_names = [filename for filename in filenames if "ELEV_02" not in filename]
    groupedFilenames = group_by_unique_dates(filtered_file_names)

    # for each grouped data, i.e. for each date, create a czml and upload it.
    for fileGroup in groupedFilenames:
        # create czml for each group.
        for filenames in fileGroup:
            # insert inside czml
            pass

        # save the czml in s3.

        # # SOURCE DIR.
        # sdate = s3_raw_file_key.split('_')[3]
        # print(f'processing CRS file {s3_raw_file_key}')

        # # create a czml file.

        # # UPLOAD CONVERTED FILES.
        # output_czml = writer.get_string()
        # output_name = os.path.splitext(os.path.basename(s3_raw_file_key))[0]
        # output_name_wo_time = output_name.split("-")[0]
        # outfile = f"{field_campaign}/{output_data_dir}/{instrument_name}/{output_name_wo_time}.czml"
        # s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)
        # print(s3_raw_file_key+" conversion done.")

def group_by_unique_dates(filenames):
    # sort the filenames.
    filenames.sort()
    # get the list of unique dates from the filenames.
    unique_date = lambda filename: filename.split("_")[3]
    # based off the unique dates, group the filenames.
    result = [list(items) for gr, items in groupby(filenames, key=unique_date)]
    return result


def nexrad_img_to_czml():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "nexrad"
    location="katx"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, location)

nexrad_img_to_czml()