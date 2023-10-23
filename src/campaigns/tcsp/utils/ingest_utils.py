from datetime import datetime, timedelta
import re

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
    i = 0
    for line in infile:
      i += 1
      if(i > 5):
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
    # Convert to hours, minutes, and seconds
    hours = int(decimal_hours)
    minutes = int((decimal_hours - hours) * 60)
    seconds = int(((decimal_hours - hours) * 3600) % 60)

    # Format as hh:mm:ss
    time_string = "{:02d}:{:02d}:{:02d}".format(hours, minutes, seconds)

    return time_string