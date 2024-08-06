import os
import fnmatch
import xarray as xr
import zarr
import numpy as np
import time
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from point_cloud import generate_point_cloud
from d3r_czml_writer import D3RCzmlWriter

campaign = 'Olympex'
collection = "AirborneRadar"
dataset = "gpmValidationOlympexD3R"
variables = ["ref"]
renderers = ["point_cloud"]
chunk = 262144

s3_client = boto3.client('s3')

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

def ingest(file_path, folder):

    store = zarr.DirectoryStore(folder)
    root = zarr.group(store=store)

    # Create empty rows for modified data    
    z_chunk_id = root.create_dataset('chunk_id', shape=(0, 2), chunks=None, dtype=np.int64)
    z_location = root.create_dataset('location', shape=(0, 3), chunks=(chunk, None), dtype=np.float32)
    z_time = root.create_dataset('time', shape=(0), chunks=(chunk), dtype=np.int32)
    z_vars = root.create_group('value')
    z_ref = z_vars.create_dataset('ref', shape=(0), chunks=(chunk), dtype=np.float32)
    n_time = np.array([], dtype=np.int64)

    data = xr.open_dataset(file_path)

    azimuth = data['Azimuth'].values
    elevation = data['Elevation'].values
    reflectivity = data['Reflectivity'].values
    gates = data['Gate'].size
    epoch_times = data['Time'].values

    clean_reflectivity = np.where(np.isfinite(reflectivity), reflectivity, np.nan)

    radar_longitude = data.attrs['Longitude']
    radar_latitude = data.attrs['Latitude']
    radar_altitude = data.attrs['Altitude']

    # Convert angles from degrees to radians for calculation
    azimuth_rad = np.deg2rad(azimuth)
    elevation_rad = np.deg2rad(elevation)

    # Assume gate spacing (e.g., 30 meters)
    gate_spacing = 30
    ranges = np.linspace(0, gates * gate_spacing, gates)

    # Convert spherical coordinates to Cartesian coordinates for 3D plotting
    R, AZ = np.meshgrid(ranges, azimuth_rad)
    E, _ = np.meshgrid(ranges, elevation_rad)
    X = R * np.cos(AZ) * np.cos(E)
    Y = R * np.sin(AZ) * np.cos(E)
    Z = R * np.sin(E)

    # Convert Cartesian coordinates (X, Y, Z) to geographic coordinates (lat, lon)
    latitudes = radar_latitude + (Z.flatten() / 111139)  # Conversion factor for degrees to meters at equator
    longitudes = radar_longitude + (X.flatten() / (111139 * np.cos(np.deg2rad(radar_latitude))))
    altitudes = Z.flatten() - radar_altitude
    reflectivity_flat = clean_reflectivity.flatten()

    # Filtering NaNs for consistent array sizes
    mask = ~np.isnan(reflectivity_flat)
    latitudes = latitudes[mask]
    longitudes = longitudes[mask]
    altitudes = altitudes[mask]
    reflectivity_flat = reflectivity_flat[mask]

    # sort data by time
    sort_idx = np.argsort(epoch_times)

    lon = longitudes[sort_idx]
    lat = latitudes[sort_idx]
    alt = altitudes[sort_idx]
    ref = reflectivity_flat[sort_idx]
    times = epoch_times[sort_idx]

    end = len(epoch_times)-1
    start_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(epoch_times[0]))
    end_time = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(epoch_times[end]))

    # Now populate (append) the empty rows with modified data.
    z_location.append(np.stack([lon, lat, alt], axis=-1))
    z_ref.append(ref)

    n_time = np.append(n_time, times)

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

    return start_time, end_time


dir = "/Users/Indhuja/Desktop/D3R/2016-01-15/"
all_entries = os.listdir(dir)
raw_files_ka = fnmatch.filter(all_entries, "olympex_d3r_ka*")
raw_files_ku = fnmatch.filter(all_entries, "olympex_d3r_ku*")
raw_files_ka.sort()
raw_files_ku.sort()

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name):
    czml_writer = D3RCzmlWriter()
    files = [file for file in raw_files_ku if file.endswith('.nc')]
    index = 0
    for file in files:
        index = index + 1
        file_path = os.path.join(dir, file)
        # print(file_path)
        tileFolder = file.split(".")[0]
        sdate = f"20160115"
        folder = f"/Users/Indhuja/Desktop/D3R/zarr/20160115-all_ku1/{tileFolder}"
        # print(folder)
        start_date_time, end_date_time = ingest(file_path, folder)
        point_cloud_folder = f"{folder}/point_cloud"
        generate_point_cloud("ref",  0,  1000000000000, folder, point_cloud_folder)
        out_files = os.listdir(point_cloud_folder)
        for out_file in out_files:
            fname = os.path.join(point_cloud_folder, out_file) # SOURCE
            s3name = f"{field_campaign}/{output_data_dir}/{instrument_name}/20160115ku/{tileFolder}/{out_file}" # DESTINATION
            print(f"uploaded to {s3name}.")
            upload_to_s3(fname, bucket_name, s3_name=s3name)
        tileLocation = f"https://{bucket_name}.s3.amazonaws.com/{field_campaign}/{output_data_dir}/{instrument_name}/20160115ku/{tileFolder}/tileset.json"
        czml_writer.add3dTiles(index, tileLocation, start_date_time, end_date_time)

    # upload the czml.
    output_czml = czml_writer.get_string()
    outfile = f"{field_campaign}/{output_data_dir}/{instrument_name}/20160115ku/knit.czml"
    s3_client.put_object(Body=output_czml, Bucket=bucket_name, Key=outfile)

def d3r():
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "d3r"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name)

d3r()