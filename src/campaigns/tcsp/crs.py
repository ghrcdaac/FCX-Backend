import pandas as pd
import os
import glob
from datetime import datetime, timedelta
from utils.ingest_utils import *
from utils.pclouds import *
from flight_track import process_tracks
from utils.tiles_writer import write_tiles
import zarr
import shutil

tInstr = {'2005-07-27':'T06:27:00Z'}

to_rad = np.pi / 180
to_deg = 180 / np.pi

folder = "/Users/Indhuja/Desktop/tcsp/CRS"
files = glob.glob(os.path.join(folder, "CR050727*__REF.ER2"))
fileVEL = "/Users/Indhuja/Desktop/CR050727_1045__VEL.ER2"
fileER2 = "/Users/Indhuja/Desktop/tcsp/ER2/tcsp_naver2_20050727_9035.txt"

start = 31
step = 29

time = []
altitude = []
longitude = []
latitude = []
refdata = []


campaign = 'Olympex'
collection = "AirborneRadar"
dataset = "gpmValidationOlympexcrs"
variables = ["zku"]
renderers = ["point_cloud"]
chunk = 262144

def _create_zarr_dir():
    """Create a directory to hold zarr file
    """
    date = "2005-07-27"
    tempdir = 'temp/' + str(date) + '/zarr'
    if os.path.exists(tempdir):
        shutil.rmtree(tempdir)
    os.makedirs(tempdir)
    return tempdir

def readCRS():
    t1970 = datetime(1970,1,1)
    for fileREF in files:
        with open(fileREF, "r") as file:
            lines = file.readlines()
            dateline = lines[7].strip().split()
            line = lines[start::step]
            # print(line)
            for i in range(start+1, len(lines), step):
                line1 = lines[i:i+28]
                rad_array = list(map(int,(" ".join(line1)).split()))
                newList = [x / 100 for x in rad_array]
                refdata.append(newList)

            # print(refdata)

            for item in line:
                parts = item.strip().split()
                if len(parts) >= 9:
                    # Creating timestamp to compare and retrieve heading, pitch, roll from ER2 data
                    date = f"{dateline[0]}-{dateline[1]}-{dateline[2]} {int(parts[4]):02d}:{int(parts[5]):02d}:{int(parts[6]):02d}"
                    time.append((datetime.strptime(date, "%Y-%m-%d %H:%M:%S")))
                    altitude.append(int(parts[2]))
                    longitude.append(int(parts[7]))
                    latitude.append(int(parts[8]))

    return time, altitude, longitude, latitude, refdata

def readER2(fileER2):
    data = process_tracks()
    return data


def _transformation(df,rad_range):
        reference_date = pd.Timestamp("1970-01-01")
        df['timediff'] = (df['timestamp'] - reference_date).dt.total_seconds()
        # print(df)

        num_col = df.shape[0]
        num_row = len(df['ref'][0])

        print("numcol: ", num_col, "nummrow:", num_row)
        # df = df.to_numpy()
        time = np.repeat(df['timediff'].to_numpy(), num_row)
        # print(time)
        lon = np.repeat(df['lon'].to_numpy(), num_row)
        print("lon size:", lon.shape)
        lat = np.repeat(df['lat'].to_numpy(), num_row)
        alt = np.repeat(df['alt'].to_numpy(), num_row)
        roll = np.repeat(df['roll'].to_numpy() * to_rad, num_row)
        pitch = np.repeat(df['pitch'].to_numpy() * to_rad, num_row)
        head = np.repeat(df['heading'].to_numpy() * to_rad, num_row)
        rad_range = np.tile(rad_range, num_col)
        # print(rad_range.shape)
        ref = np.concatenate(df['ref'])
        print(ref.shape)
        # curtain creation

        x, y, z = down_vector(roll, pitch, head)
        x = np.multiply(x, np.divide(rad_range, 111000 * np.cos(lat * to_rad)))
        y = np.multiply(y, np.divide(rad_range, 111000))
        z = np.multiply(z, rad_range)
        lon = np.add(-x, lon)
        lat = np.add(-y, lat)
        alt = np.add(z, alt)

        # sort by time

        sort_idx = np.argsort(time)
        lon = lon[sort_idx]
        lat = lat[sort_idx]
        alt = alt[sort_idx]
        ref = ref[sort_idx]
        time = time[sort_idx]

        mask = np.logical_and(np.isfinite(ref), alt > 0)
        time = time[mask]
        ref = ref[mask]
        lon = lon[mask]
        lat = lat[mask]
        alt = alt[mask]


        df1 = pd.DataFrame(data = {
        'time': time,
        'lon': lon,
        'lat': lat,
        'alt': alt,
        'ref': ref
        })
        print("Transformed data:",df1)
        return df1

def _integration(data: pd.DataFrame) -> str:
    # data from multiple sources can be integrated into intermediate file format, e.g. zarr file. The intermeidate format should be compatible with viz prepration step
    time = data['time'].values
    ref = data['ref'].values
    lon = data['lon'].values
    lat = data['lat'].values
    alt = data['alt'].values

    # path creation
    zarr_path = _create_zarr_dir()

    # create a ZARR directory in the path provided
    store = zarr.DirectoryStore(zarr_path)
    root = zarr.group(store=store)

    # Create empty rows for modified data inside zarr
    z_chunk_id = root.create_dataset('chunk_id', shape=(0, 2), chunks=None, dtype=np.int64)
    z_location = root.create_dataset('location', shape=(0, 3), chunks=(chunk, None), dtype=np.float32)
    z_time = root.create_dataset('time', shape=(0), chunks=(chunk), dtype=np.int32)
    z_vars = root.create_group('value')
    z_ref = z_vars.create_dataset('ref', shape=(0), chunks=(chunk), dtype=np.float32)
    n_time = np.array([], dtype=np.int64)
    print("n_time:",n_time,"\nz_location",z_location)

    # Now populate (append) the empty rows in ZARR dir with preprocessed data
    z_location.append(np.stack([lon, lat, alt], axis=-1))
    z_ref.append(ref)
    n_time = np.append(n_time, time)
    print("n_time:",n_time,"\nz_location",z_location)

    idx = np.arange(0, n_time.size, chunk)
    chunks = np.zeros(shape=(idx.size, 2), dtype=np.int64)
    print("idx:",idx,"\nchunks:",chunks)
    chunks[:, 0] = idx
    chunks[:, 1] = n_time[idx]
    print("idx:",idx,"\nchunks:",chunks)
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

    return zarr_path        

def prep_visualization(zarr_data_path: str) -> str:
    point_cloud_folder = zarr_data_path+"_point_cloud"
    write_tiles("ref", 0, 1000000000000, zarr_data_path, point_cloud_folder)
    return point_cloud_folder


time, altitude, longitude, latitude, refdata = readCRS()
data = {
    'time': time,
    'alt': altitude,
    'lon': longitude,
    'lat': latitude,
    'ref': refdata
}
df = pd.DataFrame(data)

df['lon'] = df['lon'] / 1000
df['lat'] = df['lat'] / 1000
df = df.sort_values(by=['time'])
df = df.reset_index(drop=True)
# print("df>>>>\n",df)

RAD = df[(df['alt'] >= 0) & (df['alt'] <= 20000)] #<--mid_lat winter storm (12000 would do)
RAD = RAD.reset_index(drop=True)
print(' In range data points:',len(RAD))
dfER2 = readER2(fileER2) #obtain heading, pitch, roll from ER2 data


merged_df = pd.merge(dfER2, df, left_on="timestamp",right_on="time", how="inner")
# print("merged_df>>>>\n",merged_df)

rad_range = [0.0, 37.414062, 74.828125, 112.24219, 149.65625, 187.07031, 224.48438, 261.89844, 299.3125, 336.72656, 374.14062, 411.5547, 448.96875, 486.3833, 523.79736, 561.2114, 598.6255, 636.03955, 673.4536, 710.8677, 748.28174, 785.6958, 823.10986, 860.5239, 897.938, 935.35205, 972.7666, 1010.18066, 1047.5947, 1085.0088, 1122.4229, 1159.8369, 1197.251, 1234.665, 1272.0791, 1309.4932, 1346.9072, 1384.3213, 1421.7358, 1459.1499, 1496.564, 1533.978, 1571.3921, 1608.8062, 1646.2202, 1683.6343, 1721.0483, 1758.4624, 1795.8765, 1833.2905, 1870.7046, 1908.1191, 1945.5332, 1982.9473, 2020.3613, 2057.7754, 2095.1895, 2132.6035, 2170.0176, 2207.4316, 2244.8457, 2282.2598, 2319.6738, 2357.088, 2394.5024, 2431.9165, 2469.3306, 2506.7446, 2544.1587, 2581.5728, 2618.9868, 2656.401, 2693.815, 2731.229, 2768.643, 2806.0571, 2843.4712, 2880.8857, 2918.2998, 2955.7139, 2993.128, 3030.542, 3067.956, 3105.37, 3142.7847, 3180.1987, 3217.6128, 3255.0269, 3292.441, 3329.855, 3367.269, 3404.683, 3442.0972, 3479.5112, 3516.9253, 3554.3394, 3591.7534, 3629.1675, 3666.5815, 3703.9956, 3741.4097, 3778.8237, 3816.2378, 3853.6519, 3891.066, 3928.48, 3965.894, 4003.308, 4040.7222, 4078.1372, 4115.5513, 4152.9653, 4190.3794, 4227.7935, 4265.2075, 4302.6216, 4340.0356, 4377.4497, 4414.864, 4452.278, 4489.692, 4527.106, 4564.52, 4601.934, 4639.348, 4676.762, 4714.1763, 4751.5903, 4789.0044, 4826.4185, 4863.8325, 4901.2466, 4938.6606, 4976.0747, 5013.4897, 5050.904, 5088.318, 5125.732, 5163.146, 5200.56, 5237.974, 5275.388, 5312.8022, 5350.2163, 5387.6304, 5425.0444, 5462.4585, 5499.8726, 5537.2866, 5574.7007, 5612.1147, 5649.529, 5686.943, 5724.357, 5761.771, 5799.185, 5836.599, 5874.013, 5911.4272, 5948.8413, 5986.2563, 6023.6704, 6061.0845, 6098.4985, 6135.9126, 6173.3267, 6210.7407, 6248.155, 6285.569, 6322.983, 6360.397, 6397.811, 6435.225, 6472.639, 6510.053, 6547.4673, 6584.8813, 6622.2954, 6659.7095, 6697.1235, 6734.5376, 6771.9517, 6809.3657, 6846.78, 6884.194, 6921.608, 6959.023, 6996.437, 7033.851, 7071.265, 7108.679, 7146.0933, 7183.5073, 7220.9214, 7258.3354, 7295.7495, 7333.1636, 7370.5776, 7407.9917, 7445.406, 7482.82, 7520.234, 7557.648, 7595.062, 7632.476, 7669.89, 7707.304, 7744.7183, 7782.1323, 7819.5464, 7856.9604, 7894.3755, 7931.7896, 7969.2036, 8006.6177, 8044.0317, 8081.446, 8118.86, 8156.274, 8193.6875, 8231.102, 8268.516, 8305.93, 8343.344, 8380.758, 8418.172, 8455.586, 8493.0, 8530.414, 8567.828, 8605.242, 8642.656, 8680.07, 8717.484, 8754.898, 8792.3125, 8829.727, 8867.143, 8904.557, 8941.971, 8979.385, 9016.799, 9054.213, 9091.627, 9129.041, 9166.455, 9203.869, 9241.283, 9278.697, 9316.111, 9353.525, 9390.939, 9428.354, 9465.768, 9503.182, 9540.596, 9578.01, 9615.424, 9652.838, 9690.252, 9727.666, 9765.08, 9802.494, 9839.908, 9877.322, 9914.736, 9952.15, 9989.564, 10026.979, 10064.393, 10101.807, 10139.221, 10176.635, 10214.049, 10251.463]
transformed_data = _transformation(merged_df,rad_range)
integrated_data = _integration(transformed_data)
point_clouds_tileset = prep_visualization(integrated_data)

