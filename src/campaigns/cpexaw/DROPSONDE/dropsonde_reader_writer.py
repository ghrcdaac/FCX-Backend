import sys
import numpy as np
from copy import deepcopy
import json
from datetime import datetime, timedelta, timezone
import xarray as xr
from metpy.units import units

model = {
    "id": "cpexaw-dropsonde",
    "name": "Dropsonde",
    "availability": "{}/{}",
    "model": {
        "gltf": "https://ghrc-fcx-field-campaigns-szg.s3.amazonaws.com/CPEX-AW/instrument-processed-data/dropsonde/dropsonde_model.glb",
        "scale": 1.0,
        "minimumPixelSize": 12,
        "maximumScale": 150.0
    },
    "position": {
        "cartographicDegrees": []
    },
}

pin_model = {
    "id": "cpexaw-dropsonde-pin",
    "name": "",
    "availability": "{}/{}",
    "position": {
        "cartographicDegrees": []
    },
}

czml_head = {
    "id": "document",
    "name": "wall czml",
    "version": "1.0"
}

class DropsondeCzmlWriter:

    def __init__(self, colors, lon, lat, alt, time, time_window, time_steps):
        self.model = deepcopy(model)
        self.pin_model = deepcopy(pin_model)
        self.colors = colors
        self.lon = [float(value) for value in lon]
        self.lat = [float(value) for value in lat]
        self.alt = [float(value) for value in alt]
        self.time = time
        self.points = []
        self.time_steps = time_steps
        self.time_window = time_window
        self.model['position']['cartographicDegrees'] = [0] * 4 * len(time_steps)

    def set_pin_model(self):
        #change the pin name
        self.pin_model['name'] = 'Dropsonde-Pin-20210817'
        self.pin_model['position']['cartographicDegrees'] = [0] * 3
        self.pin_model['availability'] = "{}/{}".format(self.time_window[0], self.time_window[1])
        self.pin_model['position']['cartographicDegrees'][0] = self.lon[0]
        self.pin_model['position']['cartographicDegrees'][1] = self.lat[0]
        self.pin_model['position']['cartographicDegrees'][2] = self.alt[0]

    def set_time(self):
        epoch = self.time_window[0]
        end = self.time_window[1]
        self.model['availability'] = "{}/{}".format(epoch, end)
        self.model['position']['epoch'] = epoch
        self.model['position']['cartographicDegrees'][0::4] = self.time_steps

    def set_position(self):
        self.model['position']['cartographicDegrees'][1::4] = self.lon
        self.model['position']['cartographicDegrees'][2::4] = self.lat
        self.model['position']['cartographicDegrees'][3::4] = self.alt

    def set_points(self):
        for i, color in enumerate(self.colors):
            point_id = f"point{i+1}"
            self.points.append({
                "id": point_id,
                "availability": "{}/{}".format(self.time[i], self.time_window[1]),
                "point": {
                    "color": {
                        "rgba": color
                    },
                    "pixelSize": 10,
                    "show": True
                }, 
                "position": {
                    "cartographicDegrees": [
                        self.lon[i], self.lat[i], self.alt[i]
                    ]
                }
            }
            )

    def set_with_df(self, df):
        self.set_time(*self.get_time_info(df['timestamp']))
        self.set_position(df['lon'], df['lat'], df['height_msl'])

    def get_time_info(self, time):
        time_window = time[[0, -1]].astype(np.string_)
        time_window = np.core.defchararray.add(time_window, np.string_('Z'))
        time_window = np.core.defchararray.decode(time_window, 'UTF-8')
        time_steps = (time - time[0]).astype(int)
        return time_window, time_steps

    def get_string(self):
        out_czml = [czml_head, self.pin_model, self.model]
        out_czml.extend(self.points)
       
        return json.dumps(out_czml)
    
class DropsondeCzmlReader:
    def __init__(self, data, date, time):
        self.data = data
        self.date = date
        self.time = time

    def stringToDateTime(self, date_str, time_str):
        date = datetime.strptime(date_str, '%Y%m%d')
        time = datetime.strptime(time_str, '%H%M%S')
        return datetime.combine(date.date(), time.time())

    def addDelta(self, dateTime, s):
        delta = timedelta(milliseconds=s*1000)
        combined_date_time = (dateTime + delta)
        return combined_date_time.isoformat(sep='T', timespec='milliseconds')+'Z'
    
    def epoch_to_iso8601(self, epoch_time):
        dt = datetime.fromtimestamp(epoch_time, timezone.utc)
        return dt.isoformat(timespec='microseconds') + 'Z'
    
    def temperatures_to_colors(self, temp, min_temp, max_temp):
        # Normalize temperature between 0 and 1
        colors = []
        for t in temp:
            normalized_temp = (t - min_temp) / (max_temp - min_temp)
            red = int(255 * normalized_temp)
            blue = int(255 * (1 - normalized_temp))
            colors.append([red, 0, blue, 255])  # RGBA
        return colors

    def read_data(self):
        with xr.open_dataset(self.data, decode_cf=False) as ds:
            tdry = ds['tdry'].values # temp
            lat = ds['lat'].values
            lon = ds['lon'].values
            alt = ds['alt'].values
            timesec = ds['time'].values

        base_time = self.stringToDateTime(self.date, self.time)
        timestr = np.vectorize(self.addDelta)(base_time, timesec) #all unique times with 0.25 sec difference
        ref = tdry * units.degC
        sort_idx = np.argsort(timestr)

        lon = lon[sort_idx]
        lat = lat[sort_idx]
        alt = alt[sort_idx]
        ref = ref[sort_idx]
        time = timestr[sort_idx] # Zulu time

        mask = np.logical_and(alt != -999.0, lon != -999.0, lat != -999.0)
        lon = lon[mask]
        lat = lat[mask]
        alt = alt[mask]
        ref = ref[mask]
        time = time[mask]

        time1 = time.astype('datetime64[ms]')
        time_window = time1[[0, -1]].astype(np.string_)
        time_window = np.core.defchararray.add(time_window, np.string_('Z'))
        time_window = np.core.defchararray.decode(time_window, 'UTF-8')
        time_steps = ((time1 - time1[0])/1000).astype(int).tolist()
        colors = self.temperatures_to_colors(ref, ref.min(), ref.max())
        return colors, lon, lat, alt, time, time_window, time_steps