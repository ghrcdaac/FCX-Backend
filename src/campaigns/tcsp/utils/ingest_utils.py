from datetime import datetime, timedelta
import re
import numpy as np

def get_s3_details(url) -> list:
    url = url.replace("s3://", "")
    temp_url = url.split("/")
    bucket_name = temp_url[0]
    
    temp_url = url.split(bucket_name+"/")
    objectKey = temp_url[1]  # key should not start with /
    return [bucket_name, objectKey]
  
def get_col_index_map():
    # represents the column number for each key, inside the input csv type file.
    return {
        "time": 1,
        "head": 4,
        "pitch": 5,
        "roll": 6,
        "gAltitude": 18,
        "gLatitude": 19,
        "gLongitude": 20
    }
     
def generator_to_np(infile):
    modified_lines = []
    # if(instrument == 'LIP'):
    #     with open(infile, 'r') as f:
    #         lines = f.readlines()
    #         for il, line in enumerate(lines):
    #             if(il > 5):
    #                 line = clean_line(line)
    #                 modified_lines.append(line)
    # if(instrument == 'ER2'):
    i = 0
    for line in infile:
        i += 1
        if(i > 5):
            if(type(line) == str):
                modified_line = clean_line(line)
            else:
                modified_line = clean_line(line.decode())
            modified_lines.append(modified_line)

    return modified_lines
  
def clean_line(line):
    pattern = r'([NSEW])\s*([0-9.]+)'
    line = re.sub(pattern, process_longitude, line)
    line = re.sub(r'\s+',' ', line)
    line = line.strip()
    line = line.split(" ")
    return line
  
def process_longitude(match):
    direction = match.group(1)
    value = match.group(2)
    if direction == 'W' or direction == 'S':
        value = '-' + value
    return value
  
def convert_to_datetime(time_str):
    days, time = time_str.split(":")[0], time_str.split(":")[1:]
    days = int(days)
    hours, minutes, seconds = map(int, time)
    # Calculate the date using the base date and days
    base_date = datetime(year=2005, month=1, day=1)
    target_date = base_date + timedelta(days=days-1, hours=hours, minutes=minutes, seconds=seconds)
    return target_date

def convert_time(decimal_hours):
    seconds = int(decimal_hours * 3600)  # Convert to seconds (3600 seconds per hour)
    time_delta = timedelta(seconds=seconds)
    formatted_time = str(time_delta)
    formatted_time = formatted_time.split(", ")[-1].zfill(8)

    return formatted_time

to_rad = np.pi / 180
to_deg = 180 / np.pi

def regionrad(region): 
    return [r*to_rad for r in region]

def sec2Z(t): 
    return "{}Z".format(datetime.utcfromtimestamp(t).isoformat())


class Tileset:
    def __init__(self, Dataset, bigbox, time0):
        print("time0:", time0)
        self.json = {
            "asset": {"version": "1.0",
                     "type": Dataset },
            "root": {"geometricError": 1000000,
                     "refine" : "REPLACE",
                     "boundingVolume": {"region": regionrad(bigbox)},
                     "children": []  },
            "properties": {"epoch": "{}Z".format(datetime.utcfromtimestamp(time0).isoformat()),
                           "refined": [] }  }
        self.parent=self.json["root"]
        print("{}Z".format(datetime.utcfromtimestamp(time0).isoformat()))

def down_vector(roll, pitch, head):
    x = np.sin(roll) * np.cos(head) + np.cos(roll) * np.sin(pitch) * np.sin(head)
    y = -np.sin(roll) * np.sin(head) + np.cos(roll) * np.sin(pitch) * np.cos(head)
    z = -np.cos(roll) * np.cos(pitch)
    return (x, y, z)

def proj_LatLonAlt(DF):
    """Zdist is distance from Aircraft"""
    
    x, y, z = down_vector(DF['roll'], DF['pitch'], DF['head'])
    x = np.multiply(x, np.divide(DF['Zdist'], 111000 * np.cos(DF['Lat'] * to_rad)))
    y = np.multiply(y, np.divide(DF['Zdist'], 111000))
    z = np.multiply(z, DF['Zdist'])

    lon = np.add(-x, DF['Lon'])
    lat = np.add(-y, DF['Lat'])
    alt = np.add(z,  DF['Alt'])
    return lon,lat,alt