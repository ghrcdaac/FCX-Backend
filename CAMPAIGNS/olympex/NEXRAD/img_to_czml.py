import boto3
import os
from itertools import groupby
from nexrad_czml_writer import NexradCzmlWriter

s3_client = boto3.client('s3')

locations_coordinates = {
    "katx": [-123.197, 48.735, -121.812, 49.653],
    "klgx": [-124.783, 46.674, -123.45, 47.582],
    "krtx": [-123.619, 45.260, -122.321, 46.172]
}

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, instrument_location):
    # get the instrument data list
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    filenames = [] # here filenames represent keys of s3 object
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/{instrument_location}/olympex"):
        filenames.append(obj.key)
    # remove tilt 5 degree (ELEV_02) data. Only visualizing parallel (ELEV_01) data
    filtered_file_names = [filename for filename in filenames if "ELEV_02" not in filename]
    groupedFilenames = group_by_unique_dates(filtered_file_names)

    # for each grouped data, i.e. for each date, create a czml and upload it.
    for fileGroup in groupedFilenames:
        group_date = fileGroup[0].split("_")[3]
        print(f'Started processing NEXRAD for {group_date}')

        # create czml for each group.
        czml_writer = NexradCzmlWriter(locations_coordinates[instrument_location])
        date_time_range = collectDateTimeRange(fileGroup)
        for index, filename in enumerate(fileGroup):
            # insert inside czml
            imagery_url = f"https://{bucket_name}.s3.amazonaws.com/{filename}"
            avail_start = date_time_range[index][0]
            avail_end = date_time_range[index][1]
            czml_writer.addTemporalImagery(index, imagery_url, avail_start, avail_end)

        # save the czml in s3.
        print('Uploading file')
        # UPLOAD CONVERTED FILES.
        output_czml = czml_writer.get_string()
        output_name = f"olympex_Level2_KRTX_{group_date}"
        outfile = f"{field_campaign}/{output_data_dir}/{instrument_name}/{instrument_location}/{output_name}.czml"
        s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)
        print(f"NEXRAD czml conversion for {group_date} done.")
    print(f"***All NEXRAD conversion for {instrument_location} Complete!***")

def group_by_unique_dates(filenames):
    # sort the filenames.
    filenames.sort()
    # get the list of unique dates from the filenames.
    unique_date = lambda filename: filename.split("_")[3]
    # based off the unique dates, group the filenames.
    result = [list(items) for gr, items in groupby(filenames, key=unique_date)]
    return result


def collectDateTimeRange(fileGroup):
    """
    Each filegroup (i.e. imagery files, collected in a day) has various nexrad files throughout that particular day.
    A nexrad imagery file is collected in certain frequency. (10 mins)
    This function gets the start and end time for each nexrad file.

    Args:
        fileGroup (array): array of string filenames.
                           Note filename has a filename format that contains date and time when the nexrad was collected
    Returns:
        array: for each mapped filename in a filegroup, returns a array of start and end date time.
        eg: for [['path_to/nexrad/katx/olympex_Level2_KRTX_20160429_0304_ELEV_01.png', 'path_to/katx/olympex_Level2_KRTX_20160429_0333_ELEV_01.png']] as input
        returns [['2016-04-29T03:04:00Z', '2016-04-29T03:33:00Z'], ['2016-04-29T03:33:00Z', '2016-04-29T04:12:00Z']]
    """
    result = []
    end_index = len(fileGroup) - 1
    date = fileGroup[0].split("_")[3]
    formatted_date = '{}-{}-{}'.format(date[:4], date[4:6], date[6:])
    for index, filename in enumerate(fileGroup):
        starttime = filename.split("_")[4]
        if (index == end_index):
            endtime = str((int(starttime) + 10) if (int(starttime) + 10) < 2359  else 2359 ) # every nexrad image has temporal resolution of 10 minutes
        else:
            endtime = fileGroup[index + 1].split("_")[4]
        formatted_start_time = '{}:{}'.format(starttime[:2], starttime[2:])
        formatted_end_time = '{}:{}'.format(endtime[:2], endtime[2:])
        # finalizing final date time in czml date format i.e.2015-09-22T22:38:00Z
        start_date_time = f"{formatted_date}T{formatted_start_time}:00Z"
        end_date_time = f"{formatted_date}T{formatted_end_time}:00Z"
        result.append([start_date_time, end_date_time])
    return result


def nexrad_img_to_czml():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "nexrad"
    location=["katx", "klgx", "krtx"]
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name, location[0])

nexrad_img_to_czml()