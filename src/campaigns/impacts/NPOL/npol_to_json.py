import os
import zarr
import numpy as np
import shutil
import boto3
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError
import glob

from utils.point_cloud import generate_point_cloud
from cf_reader import Reader as CFReader

from npol_czml_writer import NpolCzmlWriter
from helper.conversion_helper import collectAvailabilityDateTimeRange

s3_client = boto3.client('s3')

# META needed for ingest
campaign = 'IMPACTS'
collection = "AirborneRadar"
dataset = "npolDopplerImpacts"
variables = ["ref"]
renderers = ["point_cloud"]
chunk = 262144
to_rad = np.pi / 180
to_deg = 180 / np.pi

def upload_to_s3(file_name, bucket, s3_name=None):
    """Upload a file to an S3 bucket
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

def ingest(folder, filePath):
    """
    Converts Level 1B crs data from s3 to zarr file and then stores it in the provided folder
    Args:
        folder (string): name to hold the raw files.
        file (string): the s3 url to the raw file. WHAT FORMAT IS IT IN in hdf5 format
    """
    store = zarr.DirectoryStore(folder)
    root = zarr.group(store=store)
    
    # Create empty rows for modified data    
    z_chunk_id = root.create_dataset('chunk_id', shape=(0, 2), chunks=None, dtype=np.int64)
    z_location = root.create_dataset('location', shape=(0, 3), chunks=(chunk, None), dtype=np.float32)
    z_time = root.create_dataset('time', shape=(0), chunks=(chunk), dtype=np.int32)
    z_vars = root.create_group('value')
    z_ref = z_vars.create_dataset('atb', shape=(0), chunks=(chunk), dtype=np.float32)
    n_time = np.array([], dtype=np.int64)

    print("Accessing file to convert to zarr ")

    cfr = CFReader(filePath)
    cf_datas = cfr.read_data() # it will return a generator.

    # use plain python array to append, for performance reasons.
    atb = []
    lon = []
    lat = []
    alt = []
    time = []

    # using the generator, populate all the lon, lat, alt and atb values
    for cf_data in cf_datas:
        atb.append(np.float64(cf_data['CZ']))
        lon.append(np.float64(cf_data['lon']))
        lat.append(np.float64(cf_data['lat']))
        alt.append(np.float64(cf_data['height']))
        time.append(np.datetime64(cf_data['timestamp']).astype('timedelta64[s]').astype(np.int64))

    atb = np.array(atb, dtype=np.float64)
    lon = np.array(lon, dtype=np.float64)
    lat = np.array(lat, dtype=np.float64)
    alt = np.array(alt, dtype=np.float64)
    time = np.array(time, dtype=np.int64)

    ## using the values, create a zarr file and return it.
    
    # sort data by time
    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    atb = atb[sort_idx]
    time = time[sort_idx]

    # Now populate (append) the empty rows with modified data.
    z_location.append(np.stack([lon, lat, alt], axis=-1))
    z_ref.append(atb)

    n_time = np.append(n_time, time)

    idx = np.arange(0, n_time.size, chunk)
    chunks = np.zeros(shape=(idx.size, 2), dtype=np.int64)
    chunks[:, 0] = idx
    chunks[:, 1] = n_time[idx]
    z_chunk_id.append(chunks)

    epoch = np.min(n_time)
    n_time = (n_time - epoch).astype(np.int32)
    z_time.append(n_time)

    # save it.
    root.attrs.put({
        "campaign": campaign,
        "collection": collection,
        "dataset": dataset,
        "variables": variables,
        "renderers": renderers,
        "epoch": int(epoch)
    })

# ------------------START--------------------------------

def data_pre_process(bucket_name, field_campaign, instrument_name, date):
    # download raw files from earthdata search
    # list all the files.
    # for each file, run ingest.
    # generate point clouds.
    # upload all of the pointcloud files.

    raw_file_dir = f"/Users/Indhuja/Desktop/npol/{date}" # local dir where raw file resides.
    print(raw_file_dir)
    # create a czml with all the 3dtiles information for a single day
    czml_writer = NpolCzmlWriter()

    minutely_datas = glob.glob(f"{raw_file_dir}/impacts_NPOL1_*.cf.gz")
    # sort according to date time.
    minutely_datas.sort()
    # for the list of cf files within a single day, find the availability date time for each of them.
    availability_time_range = collectAvailabilityDateTimeRange(minutely_datas)
    print(availability_time_range)
    for index, minute_data_path in enumerate(minutely_datas):
    # iterate to create 3d tiles.
        print(f"\n{index}. converting for {minute_data_path}")
        # CREATE A LOCAL DIR TO HOLD RAW DATA AND CONVERTED DATA
        tileFolder = minute_data_path.split("/")[-1].split(".")[0]
        folder = f"/Users/Indhuja/Desktop/npol/zarr/{date}/{tileFolder}" # intermediate folder for zarr file (date + time), time rep by index.
        point_cloud_folder = f"{folder}/point_cloud" # intermediate folder for 3d tiles, point cloud
        print(tileFolder, folder, point_cloud_folder, "\n\n")
        if os.path.exists(folder): shutil.rmtree(f"{folder}")
        os.makedirs(folder)
        Path(folder).mkdir(parents=True, exist_ok=True)
        # LOAD FROM SOURCE WITH NECESSARY PRE PROCESSING. CONVERT LEVEL 1B RAW FILES INTO ZARR FILE.
        ingest(folder, minute_data_path)
        # CONVERT ZARR FILE INTO 3D TILESET JSON.
        generate_point_cloud("atb",  0,  1000000000000, folder, point_cloud_folder)
        # UPLOAD CONVERTED 3d Tiles (Pointcloud) FILES.
        files = os.listdir(point_cloud_folder)
        for file in files:
            fname = os.path.join(point_cloud_folder, file) # SOURCE
            s3name = f"{field_campaign}/{date}/{instrument_name}/{tileFolder}/{file}" # DESTINATION
            print(fname, s3name)
            print(f"uploaded to {s3name}.")
            upload_to_s3(fname, bucket_name, s3_name=s3name)
        # after uploading the 3d tile point cloud, track them in the czml.
        tileLocation = f"https://{bucket_name}.s3.us-west-2.amazonaws.com/{field_campaign}/{date}/{instrument_name}/{tileFolder}/tileset.json"
        avail_start = availability_time_range[index][0]
        avail_end = availability_time_range[index][1]
        czml_writer.add3dTiles(index, tileLocation, avail_start, avail_end)
        # print(f"NPOL 3d tile conversion for {sdate} done.")
    # upload the czml.
    output_czml = czml_writer.get_string()
    outfile = f"{field_campaign}/{date}/{instrument_name}/knit.czml"
    s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)
    print(f"NPOL CZML conversion for {date} done.")

def npol():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-viz-output"
    field_campaign = "fieldcampaign/impacts"
    instrument_name = "npol"
    fdates = ['2020-02-20']
    # '2020-02-25', '2020-02-20', '2020-01-19', '2020-01-18'
    for date in fdates:
        data_pre_process(bucket_name, field_campaign, instrument_name, date)


npol()
