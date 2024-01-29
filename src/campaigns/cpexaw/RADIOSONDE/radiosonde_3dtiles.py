import os
import shutil
import pandas as pd

from helper.pointcloud import generate_point_cloud
from helper.utils import get_files, upload_file, data_reader, formatted_datetime, clean_data
from helper.ingestToZarr import ingest
from metpy.units import units
from pint import UnitRegistry

ureg = UnitRegistry()

# Define 'degCelsius' as a unit
ureg.define('degCelsius = [temperature]')

column_name_changes = {
        'Time [sec]': 'Time',
        'T [°C]': 'Temp',
        'U [%]': 'RH',
        'Lon [°]   ': 'Lon',
        'Lat [°]  ': 'Lat',
        'Altitude [m]': 'Alt',
        'Dew [°C]': 'DP'
    }

def main():
  s3_url_list = get_files()
  for s3_url in s3_url_list:
      try:
        name = s3_url.split('/')[-1]
        print(name)
        date, time, start_time = formatted_datetime(name)
        print(start_time)
        path = r'/Users/Indhuja/Desktop/radiosonde/' + date + '/' + time 
        if not os.path.exists(path):
          os.makedirs(path)
        else:
          shutil.rmtree(path)
          os.makedirs(path)
        data = data_reader(s3_url)
        print(data)
        cleaned_data = clean_data(data, column_name_changes)
        print(cleaned_data)
        ingest(path, cleaned_data)
        point_cloud_folder = f"{path}/point_cloud"
        generate_point_cloud("ref",  0,  1000000000000, path, point_cloud_folder)
        upload_file("3dTiles", f"{path}/point_cloud", bucket_name="ghrc-fcx-field-campaigns-szg", prefix=f"CPEX-AW/instrument-processed-data/radiosonde/3dTiles/{date}")
      except Exception as e:
        print("Error during conversion for:: ", s3_url, ". Error on", e)

main()