import numpy as np
import pandas as pd
from copy import deepcopy
import json
import boto3
import os
from datetime import datetime, timedelta

model = {
    "id": "Flight Track",
    "name": "P3",
    "availability": "{}/{}",
    "model": {
        "gltf":"https://fcx-czml.s3.amazonaws.com/img/p3.gltf",
        "scale": 900.0,
        "minimumPixelSize": 500,
        "maximumScale": 1000.0
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
TrackColor = {'P3B': [0, 255, 128, 255],
              'ER2': [0, 255, 255, 128]}

class FlightTrackCzmlWriter:

    def __init__(self, length, plane):
        self.model = deepcopy(model)
        self.length = length
        self.model['name'] = plane
        self.model['path']['material']['solidColor']['color']['rgba'] = TrackColor[plane]
        self.model['position']['cartographicDegrees'] = [0] * 4 * length
        self.model['properties']['roll']['number'] = [0] * 2 * length
        self.model['properties']['pitch']['number'] = [0] * 2 * length
        self.model['properties']['heading']['number'] = [0] * 2 * length

    def set_time(self, time_window, time_steps):
        epoch = time_window[0]
        end = time_window[1]
        self.model['availability'] = "{}/{}".format(epoch, end)
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

    def get_string(self):
        return json.dumps([czml_head, self.model])



# basically, no Na filling in the data, so dropna() check is not necessary
# also removed unique time check as P3B/ER2 met data are recorded every 1s, there's no overlapping accounts. 
class FlightTrackReader:
    def __init__(self,file,plane):
        if (plane=='P3B'): cols=[0,1,2,3,4,12,15,16]
        if (plane=='ER2'): cols=[0,1,2,3,4,10,13,14]
        with open(file) as f:
            lines = f.readlines()
            for il,line in enumerate(lines):
                if('Time_Start,Day_Of_Year,' in line):
                    break
        self.file = file
        self.hlines = il
        self.useCols = cols

    def read_csv(self,nskip=1):
        df = pd.read_csv(self.file,index_col=None,usecols=self.useCols, skiprows=self.hlines)
        df.columns = ['Time_s','Jday', 'lat','lon','alt','heading','pitch','roll']

        df['heading'] = [ h if h<=180 else h-360 for h in df.heading]
        df['heading'] = [ h * np.pi / 180. for h in df.heading]  #<--check if this is right
        df['pitch'] = [ p * np.pi / 180. for p in df.pitch]
        df['roll'] = [ r * np.pi / 180. for r in df.roll]
        df['time_steps'] = [(t - df.Time_s[0]) for t in df.Time_s]

        Cdate=datetime.strptime('2020'+str(df.Jday[0]).zfill(3),"%Y%j")
        time = [ Cdate + timedelta(seconds=s) for s in df.Time_s]
        self.twindow = [time[0].strftime('%Y-%m-%dT%H:%M:%SZ'), 
                        time[-1].strftime('%Y-%m-%dT%H:%M:%SZ')]
        
        df = df[df['Time_s']%(nskip+1) == 0]  #keep every nskip+1 s
        df = df.reset_index(drop=True)
        
        return df


from glob import glob

def process_tracks():

    s3_client = boto3.client('s3')

    #--------to be modified -----
   #bucketOut = os.environ['OUTPUT_DATA_BUCKET']
    bucketOut = 'ghrc-fcx-viz-output'

    plane = 'P3B'
    fdate='2020-02-05'
    sdate=fdate.split('-')[0]+fdate.split('-')[1]+fdate.split('-')[2]
    infile = glob('data/IMPACTS_MetNav_'+plane+'_'+sdate+'*.ict')[0]
    #-----------------------------

    track = FlightTrackReader(infile,plane)
    Nav = track.read_csv()
    print(track.twindow)

    writer = FlightTrackCzmlWriter(len(Nav), plane)
    writer.set_time(track.twindow, Nav.time_steps)
    writer.set_position(Nav.lon, Nav.lat, Nav.alt)
    writer.set_orientation(Nav.roll, Nav.pitch, Nav.heading)

    output_name = os.path.splitext(os.path.basename(infile))[0]
   #outfile = f"{os.environ['OUTPUT_DATA_BUCKET_KEY']}/fieldcampaign/impacts/flight_track/{output_name}"
    outfile = f"fieldcampaign/impacts/flight_track/{output_name}"

    s3_client.put_object(Body=writer.get_string(), Bucket=bucketOut, Key=outfile)
    

process_tracks()
