import numpy as np
from copy import deepcopy
import json

# variable declarations

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

# class declaration

class FlightTrackCzmlWriter:

    def __init__(self, length):
        self.model = deepcopy(model)
        self.length = length
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

    def set_with_df(self, df):
        self.set_time(*self.get_time_info(df['timestamp']))
        self.set_position(df['lon'], df['lat'], df['height_msl'])
        self.set_orientation(df['roll'], df['pitch'], df['track'])

    def get_time_info(self, time):
        time_window = time[[0, -1]].astype(np.string_)
        time_window = np.core.defchararray.add(time_window, np.string_('Z'))
        time_window = np.core.defchararray.decode(time_window, 'UTF-8')
        time_steps = (time - time[0]).astype(int)
        return time_window, time_steps

    def get_string(self):
        return json.dumps([czml_head, self.model])

# class declaration

class FlightTrackReader:

    def __init__(self):
        self.converters = {}
        for i in range(33):
            self.converters[i] = self.ignore
        self.converters[1] = self.string_to_date
        self.converters[2] = self.string_to_float
        self.converters[3] = self.string_to_float
        self.converters[4] = self.string_to_float
        self.converters[14] = self.string_to_float
        self.converters[16] = self.string_to_float
        self.converters[17] = self.string_to_float

    def read_csv(self, infile):
        data = np.loadtxt(infile, delimiter=',', converters=self.converters)
        time = data[:, 1]
        latitude = data[:, 2]
        longitude = data[:, 3]
        altitude = data[:, 4]
        heading = data[:, 14] * np.pi / 180. - np.pi / 2.
        pitch = data[:, 16] * np.pi / 180.
        roll = data[:, 17] * np.pi / 180.

        mask = np.logical_not(np.isnan(latitude))
        mask = np.logical_and(mask, np.logical_not(np.isnan(longitude)))
        mask = np.logical_and(mask, np.logical_not(np.isnan(altitude)))
        mask = np.logical_and(mask, np.logical_not(np.isnan(heading)))
        mask = np.logical_and(mask, np.logical_not(np.isnan(pitch)))
        mask = np.logical_and(mask, np.logical_not(np.isnan(roll)))

        _, unique_idx = np.unique(time, return_index=True)
        unique = np.copy(mask)
        unique[:] = False
        unique[unique_idx] = True

        mask = np.logical_and(mask, unique)

        time = time[mask].astype('datetime64[s]')
        time_window = time[[0, -1]].astype(np.string_)
        time_window = np.core.defchararray.add(time_window, np.string_('Z'))
        self.time_window = np.core.defchararray.decode(time_window, 'UTF-8')
        self.time_steps = (time - time[0]).astype(int).tolist()[::5]
        self.latitude = latitude[mask][::5]
        self.longitude = longitude[mask][::5]
        self.altitude = altitude[mask][::5]
        self.heading = heading[mask][::5]
        self.pitch = pitch[mask][::5]
        self.roll = roll[mask][::5]
        self.length = mask[mask][::5].size

    def string_to_float(self, str):
        value = np.nan
        try:
            value = float(str)
        except:
            pass
        return value

    def string_to_date(self, str):
        time = np.datetime64(str, 's')
        return time.astype(np.int64)

    def ignore(self, value):
        return np.nan
