from metpy.units import units
import zarr
import numpy as np
import pandas as pd

# META needed for ingest
campaign = 'CPEX-AW'
collection = "AirborneRadar"
dataset = "cpexaw_radiosonde"
variables = ["zku"]
renderers = ["point_cloud"]
chunk = 262144
to_rad = np.pi / 180
to_deg = 180 / np.pi

def ingest(folder, df):
    """
    Converts Level 1B crs data from s3 to zarr file and then stores it in the provided folder
    Args:
        folder (string): name to hold the raw files.
        file (string): file to open
        date (string): date when dropsonde was dropped
        time (string): time when dropsonde was dropped
    """
    store = zarr.DirectoryStore(folder)
    root = zarr.group(store=store)
    
    # Create empty rows for modified data    
    z_chunk_id = root.create_dataset('chunk_id', shape=(0, 2), chunks=None, dtype=np.int64)
    z_location = root.create_dataset('location', shape=(0, 3), chunks=(chunk, None), dtype=np.float32)
    z_time = root.create_dataset('time', shape=(0), chunks=(chunk), dtype=np.int32)
    z_vars = root.create_group('value')
    z_ref = z_vars.create_dataset('ref', shape=(0), chunks=(chunk), dtype=np.float32)
    n_time = np.array([], dtype=np.int64)

    rh = df['RH'].values # relative humidity
    dp = df['DP'].values # dew point
    tdry = df['Temp'].values # temperature
    lat = df['Lat'].values
    lon = df['Lon'].values
    alt = df['Alt'].values
    timesec = df['Time'].values

    time = np.array(timesec, dtype='datetime64[s]').astype(np.int64)

    # Data formation

    # Not needed all the data points at single point
    # ref = np.column_stack((rh, dp, tdry)).reshape(-1)
    # as 3 kind of data at a single point in 3d space(lon lat alt) in a given time

    # instead only show one data point at one location and time (save render computation)
    ref = tdry * units.degC
    ref = np.repeat(tdry, 3)
    lon = np.repeat(lon, 3)
    lat = np.repeat(lat, 3)
    alt = np.repeat(alt, 3)
    time = np.repeat(time, 3)
    print(len(ref), len(lon))

    # sort data by time
    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    ref = ref[sort_idx]
    time = time[sort_idx]
    print(lon)
    print("hello")

    # remove nan and infinite using mask ???
    mask = np.logical_and(alt != -999.0, lon != -999.0, lat != -999.0)
    # mask = np.logical_and(np.isfinite(ref), alt > 0, alt != -999.0, lon != -999.0, lat != -999.0)
    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    ref = ref[mask]
    time = time[mask]
    print("hello")

    # Now populate (append) the empty rows with modified data.
    z_location.append(np.stack([lon, lat, alt], axis=-1))
    z_ref.append(ref)
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