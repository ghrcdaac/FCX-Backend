import os
import zarr
import numpy as np
import xarray as xr
import shutil
import boto3
from pathlib import Path
import s3fs
import h5py
import pandas as pd
from boto3 import client as boto_client
import tarfile

from npol_utils.point_cloud import generate_point_cloud
from npol_utils.s3_updnload import upload_to_s3
from uf_reader import Reader as UFReader


# META needed for ingest
campaign = 'Olympex'
collection = "AirborneRadar"
dataset = "gpmValidationOlympexcrs"
variables = ["ref"]
renderers = ["point_cloud"]
chunk = 262144
to_rad = np.pi / 180
to_deg = 180 / np.pi

def ingest(folder, file, s3bucket):
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

    # date = file.split("_")[5].split(".")[0]
    # base_time = np.datetime64('{}-{}-{}'.format(date[:4], date[4:6], date[6:]))

    print("Accessing file to convert to zarr ")
    
    # fs = s3fs.S3FileSystem(anon=False)
    # with fs.open(file) as cplfile:
    #     with h5py.File(cplfile, 'r') as f1:
    #         atb = f1['ATB_1064'][()]            
    #         lon  = f1['Longitude'][()]
    #         lat  = f1['Latitude'][()]
    #         alt  = f1['Plane_Alt'][()] * 1000   #[km] ==> [m]

    #         # !!! if over 24 hour fix not applied
    #         delta = [(base_time + (h*3600+m*60+s).astype('timedelta64[s]')) for (h,m,s) in 
    #                 zip(f1['Hour'][()], f1['Minute'][()], f1['Second'][()])] #delta is in seconds

    # !!! input uf file path is inside UFREADER
    ufr = UFReader("/tmp/test_data/olympex_NPOL1_20151203_000005_rhi_20-40.uf.gz")
    uf_datas = ufr.read_data() # it will return a generator.

    # using the generator, populate all the lon, lat, alt and atb values

    atb = np.array([], dtype=np.int64)            
    lon = np.array([], dtype=np.int64)
    lat = np.array([], dtype=np.int64)
    alt = np.array([], dtype=np.int64)
    time = np.array([], dtype=np.int64)

    for uf_data in uf_datas:
        atb = np.append(atb, uf_data['CZ'])
        lon = np.append(lon, uf_data['lon'])
        lat = np.append(lat, uf_data['lat'])
        alt = np.append(alt, uf_data['height'])
        time = np.append(time, np.datetime64(uf_data['timestamp']).astype('timedelta64[s]').astype(np.int64))
    
    # using the values, create a zarr file and return it.


    # # !!! if over 24 hour fix not applied
    # delta = [(base_time + (h*3600+m*60+s).astype('timedelta64[s]')) for (h,m,s) in 
    #         zip(f1['Hour'][()], f1['Minute'][()], f1['Second'][()])] #delta is in seconds


    # # num_col = atb.shape[0] # number of rows, say 7903
    # num_cols = atb.shape[1] # number of cols, say 757

    # # maintain data shape
    # # delta = np.repeat(delta, num_cols)
    # # lon = np.repeat(lon, num_cols)
    # # lat = np.repeat(lat, num_cols)
    # # alt = np.repeat(alt, num_cols)

    
    # # atb = atb.flatten()
    
    # time correction.
    # time = (delta - np.datetime64('1970-01-01')).astype('timedelta64[s]').astype(np.int64)

    # !!! no lon alt lat correction for now.
    
    # sort data by time
    

    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    atb = atb[sort_idx]
    time = time[sort_idx]

    # # remove infinite atb value and negative altitude values using mask
    # mask = np.logical_and(np.isfinite(atb), alt > 0) # alt value when not avail is defaulted to -999.0
    # lon = lon[mask]
    # lat = lat[mask]
    # alt = alt[mask]
    # atb = atb[mask]
    # time = time[mask]

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

def downloadFromS3(bucket_name, s3_key, dest_dir):
    s3 = boto_client('s3')
    filename = s3_key.split('/')[3]
    dest_dir = '/tmp/npol_olympex/raw/'
    dest = dest_dir + filename
    if os.path.exists(dest_dir): shutil.rmtree(f"{dest_dir}")
    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    print("Downloading file",s3_key,"from bucket",bucket_name, " into dir:", dest_dir)
    s3.download_file(
        Bucket = bucket_name,
        Key = s3_key,
        Filename = dest
    )
    return dest


def untarr(raw_file_dir, raw_file_path, filename):
    unzipped_file_path = raw_file_dir + filename.split(".")[0] # removing the .tar.gz # this is important
    if raw_file_path.endswith("tar.gz"):
        with tarfile.open(raw_file_path, "r:gz") as t:
            t.extractall(unzipped_file_path)
    elif raw_file_path.endswith("tar"):
        with tarfile.open(raw_file_path, "r:") as t:
            t.extractall(unzipped_file_path)
    return unzipped_file_path
# ------------------START--------------------------------

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{field_campaign}/{input_data_dir}/{instrument_name}/olympex_npol"):
        keys.append(obj.key)

    raw_file_dir = '/tmp/npol_olympex/raw/' # local dir where raw file resides.

    for s3_key in keys:
        filename = s3_key.split('/')[3]
        raw_file_path = downloadFromS3(bucket_name, s3_key, raw_file_dir) # inc file name
        unzipped_file_path = untarr(raw_file_dir, raw_file_path, filename)
        print(unzipped_file_path)

    # for s3_raw_file_key in keys:
        # download each input file.
        # unzip it
        # go inside rhi_a dir,
        # list all the files.
        # for each file, run ingest.
        # generate point clouds.
        # upload all of the pointcloud files.

    # # SOURCE DIR.
    # sdate = s3_raw_file_key.split("_")[5].split(".")[0]
    # print(f'processing CRS file {s3_raw_file_key}')
    return
    sdate = "20151203b"
    # CREATE A LOCAL DIR TO HOLD RAW DATA AND CONVERTED DATA
    folder = f"/tmp/npol_olympex/zarr/{sdate}" # intermediate folder for zarr file (date + time)
    point_cloud_folder = f"{folder}/point_cloud" # intermediate folder for 3d tiles, point cloud
    if os.path.exists(folder): shutil.rmtree(f"{folder}")
    # os.mkdir(folder)
    Path(folder).mkdir(parents=True, exist_ok=True)
    # LOAD FROM SOURCE WITH NECESSARY PRE PROCESSING. CONVERT LEVEL 1B RAW FILES INTO ZARR FILE.
    # src_s3_path = f"s3://{bucket_name}/{s3_raw_file_key}"
    src_s3_path = "abc"
    ingest(folder, src_s3_path, bucket_name)
    # # CONVERT ZARR FILE INTO 3D TILESET JSON.
    generate_point_cloud("atb",  0,  1000000000000, folder, point_cloud_folder)
    # # UPLOAD CONVERTED FILES.
    files = os.listdir(point_cloud_folder)
    s3 = boto3.client('s3')
    for file in files:
        fname = os.path.join(point_cloud_folder, file) # SOURCE
        s3name = f"{field_campaign}/{output_data_dir}/npol/{sdate}/{file}" # DESTINATION
        print(f"uploaded to {s3name}.")
        upload_to_s3(fname, bucket_name, s3_name=s3name)


def npol():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "npol"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name)


npol()
