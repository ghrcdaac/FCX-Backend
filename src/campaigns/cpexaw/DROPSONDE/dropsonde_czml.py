import os
from datetime import datetime, timedelta, timezone
from metpy.units import units

import boto3
from boto3 import client as boto_client
from dropsonde_reader_writer import DropsondeCzmlReader, DropsondeCzmlWriter

class DropsondeCzml:
  def __init__(self):
    # constructor
    pass
  
  def get_files(self, bucket_name="ghrc-fcx-field-campaigns-szg", prefix="CPEX-AW/instrument-raw-data/dropsonde"):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{prefix}/CPEXAW-DROPSONDE_D20210820_224932"): # change this
        url = "s3://" + bucket_name + "/" + obj.key
        # url = f"https://{bucket_name}.s3.amazonaws.com" + "/" + prefix + "/" + obj.key
        keys.append(url)
    return keys
    
  def upload_file(self, source_file_path, bucket_name="ghrc-fcx-field-campaigns-szg", prefix="CPEX-AW/instrument-processed-data/dropsonde"):
    s3 = boto3.resource('s3')
    files = os.listdir(source_file_path)
    for file in files:
        if file.endswith(".czml"):
          fname = os.path.join(source_file_path, file) # SOURCE
          actualprefix = f"{prefix}/{file}" # DESTINATION
          s3.Bucket(bucket_name).upload_file(fname, actualprefix)
  
  def data_reader(self, s3_url):
    ## Open data file
    bucket_name = s3_url.split("/")[2]
    key = s3_url.split(f"{bucket_name}/")[-1] # need key without starting /
    s3 = boto_client('s3')
    fileobj = s3.get_object(Bucket=bucket_name, Key=key)
    file = fileobj['Body'].read()
    return file

# UTILS

def epoch_to_iso8601(epoch_time):
    dt = datetime.fromtimestamp(epoch_time, timezone.utc)
    # dt_naive = dt.replace(tzinfo=None)
    # return dt_naive.isoformat(timespec='microseconds')
    return dt.isoformat(timespec='microseconds') + 'Z'

def temp_to_color(temp, min_temp, max_temp):
    # Normalize temperature between 0 and 1
    colors = []
    for t in temp:
      normalized_temp = (t - min_temp) / (max_temp - min_temp)
      red = int(255 * normalized_temp)
      blue = int(255 * (1 - normalized_temp))
      colors.append([red, 0, blue, 255])  # RGBA
    return colors

def set_temperatures(model, temperatures, longitude, latitude, altitude):
    colors = temp_to_color(temperatures)
    for i, color in enumerate(colors):
        point_id = f"point{i+1}"
        model['point'][point_id] = {
            "id": point_id,
            "point": {
                "color": {
                    "rgba": color
                },
                "pixelSize": 10,
                "show": True
            },
            "position": {
                "cartographicDegrees": [
                    longitude[i], latitude[i], altitude[i]
                ]
            }
        }

def main():
  ds = DropsondeCzml()
  s3_url_list = ds.get_files()
  for s3_url in s3_url_list:
    try:
      name = s3_url.split('/')[-1]
      date = name.split('_D')[1].split('_')[0]
      time = name.split('_D')[1].split('_')[1]
      data = ds.data_reader(s3_url)
      ds_reader = DropsondeCzmlReader(data, date, time)
      colors, lon, lat, alt, time, time_window, time_steps = ds_reader.read_data()
      ds_writer = DropsondeCzmlWriter(colors, lon, lat, alt, time, time_window, time_steps)
      ds_writer.set_time()
      ds_writer.set_position()
      ds_writer.set_pin_model()
      ds_writer.set_points()
      
      output_czml = ds_writer.get_string()

      file_path = "/Users/Indhuja/Desktop/Dropsonde/upload/ds_with_pin_latest.czml" # local path
      with open(file_path, 'w') as f:
         f.write(output_czml)

      ds.upload_file(os.path.dirname(file_path), bucket_name="ghrc-fcx-field-campaigns-szg", prefix=f"CPEX-AW/instrument-processed-data/dropsonde/czml")
    except Exception as e:
      print("Error during conversion for: ", s3_url, ". Error on", e)
  print("Done!")
    
main()