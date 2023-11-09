import boto3
from copy import deepcopy
import numpy as np
import pandas as pd
import json
import math
import os

from utils.ingest_utils import generator_to_np, get_col_index_map, convert_to_datetime

model = {
    "id": "Flight Track",
    "name": "ER2",
    "availability": "{}/{}",
    "model": {
        "gltf": "https://s3.amazonaws.com/visage-czml/iphex_HIWRAP/img/er2.gltf",
        "scale": 100.0,
        "minimumPixelSize": 32,
        "maximumScale": 150.0
    },
    "position": {
        "cartographicDegrees": []
    },
    "path": {
        "material": {
            "solidColor": {
                "color": {
                    "rgba": [0, 255, 128, 255]
                }
            }
        },
        "width": 1,
        "resolution": 5
    },
    "properties": {
        "roll": {},
        "pitch": {},
        "heading": {}
    }
}

czml_head = {
    "id": "document",
    "name": "wall czml",
    "version": "1.0"
}

class FlightTrackCzmlWriter:
    def __init__(self, length):
        self.model = deepcopy(model)
        self.czml_head = deepcopy(czml_head)
        self.length = length
        self.model['position']['cartographicDegrees'] = [0] * 4 * length
        self.model['properties']['roll']['number'] = [0] * 2 * length
        self.model['properties']['pitch']['number'] = [0] * 2 * length
        self.model['properties']['heading']['number'] = [0] * 2 * length

    def set_with_df(self, df):
        self.set_time(*self.get_time_info(df['timestamp']))
        self.set_position(df['longitude'], df['latitude'], df['altitude'])
        self.set_orientation(df['roll'], df['pitch'], df['heading'])

    def set_time(self, time_window, time_steps):
        [epoch, end] = time_window
        self.model['availability'] = f"{epoch}/{end}"
        self.model['position']['epoch'] = epoch
        self.model['position']['cartographicDegrees'][0::4] = time_steps
        self.model['properties']['roll']['epoch'] = epoch
        self.model['properties']['pitch']['epoch'] = epoch
        self.model['properties']['heading']['epoch'] = epoch
        self.model['properties']['roll']['number'][0::2] = time_steps
        self.model['properties']['pitch']['number'][0::2] = time_steps
        self.model['properties']['heading']['number'][0::2] = time_steps

    def set_position(self, longitude, latitude, altitude):
        self.model['position']['cartographicDegrees'][1::4] = longitude
        self.model['position']['cartographicDegrees'][2::4] = latitude
        self.model['position']['cartographicDegrees'][3::4] = altitude

    def set_orientation(self, roll, pitch, heading):
        self.model['properties']['roll']['number'][1::2] = roll
        self.model['properties']['pitch']['number'][1::2] = pitch
        self.model['properties']['heading']['number'][1::2] = heading

    def get_time_info(self, time):
        time = time.values.astype('datetime64[s]') # pandas series to numpy ndarray
        time_window = time[[0, -1]].astype(np.string_)  # get first and last element
        time_window = np.core.defchararray.add(time_window, np.string_('Z')) # add Z to each time window element to make it ISO format
        time_window = np.core.defchararray.decode(time_window, 'UTF-8') # decode to UTF-8 from byte_ object
        time_steps = (time - time[0]).astype(int).tolist()
        return time_window, time_steps

    def get_czml_string(self):
        return json.dumps([self.czml_head, self.model])

class FlightTrackReader():
    def __init__(self):
        pass
  
    def read_csv(self, infile):
        print("read_csv", infile)
        data = generator_to_np(infile)

        col_index_map = get_col_index_map()
        data = pd.DataFrame(data)

        # data extraction
        # scrape necessary data columns 
        time = data.loc[:, col_index_map["time"]]
        heading = data.loc[:, col_index_map["head"]].astype(float)* math.pi / 180. - math.pi / 2.
        pitch = data.loc[:, col_index_map["pitch"]].astype(float)* math.pi / 180.
        roll = data.loc[:, col_index_map["roll"]].astype(float)* math.pi / 180.
        gAltitude = data.loc[:, col_index_map["gAltitude"]].astype(float)
        gLatitude = data.loc[:, col_index_map["gLatitude"]].astype(float)
        gLongitude = data.loc[:, col_index_map["gLongitude"]].astype(float)

        df = pd.DataFrame(data = {"timestamp": time, "heading": heading, "pitch": pitch, "roll": roll,  "altitude": gAltitude, "latitude": gLatitude, "longitude": gLongitude})
        df["timestamp"] = df["timestamp"].apply(convert_to_datetime)
        # print(df)
        mask = (df == 0).any(axis=1)
        df = df[~mask]
        df = df.reset_index(drop=True)
        df_filtered = df.iloc[::3] 
        # print("df>>>\n",df)
        # print(df_filtered)
        return df, df_filtered

def process_tracks():
    s3_resource = boto3.resource('s3')
    bucket = "ghrc-fcx-field-campaigns-szg"
    s3bucket = s3_resource.Bucket(bucket)
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"tcsp/instrument-raw-data/ER2_Flight_Nav/tcsp_naver2_20050727"):
        keys.append(obj.key)

    result = keys
    s3_client = boto3.client('s3')
    
    for infile in result:    
        s3_file = s3_client.get_object(Bucket=bucket, Key=infile)
        print(infile)
        data = s3_file['Body'].iter_lines()
        reader = FlightTrackReader()
        CRSdata, NavData = reader.read_csv(data)
        print("Data passed to CRS>>>>>")
        print(CRSdata)
        return CRSdata
        
        # writer = FlightTrackCzmlWriter(len(NavData))
        # writer.set_with_df(NavData)
        
        # file_name = os.path.splitext(os.path.basename(infile))[0]
        # file_name = "_".join(file_name.split("_")[0:3])
        # print(file_name)
        # output_directory = "tcsp/instrument-processed-data/ER2_Flight_Nav"
        # outfile = os.path.join(output_directory, f"{file_name}.czml")
        # print(file_name, outfile)
        # s3_client.put_object(Body=writer.get_czml_string(), Bucket=bucket, Key=outfile)
        # print(f'Upload complete.\n\n')


process_tracks()
