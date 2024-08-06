import os
import shutil
import pandas as pd

from helper.czml_utils import get_files, upload_file, data_reader, formatted_datetime, clean_data
from metpy.units import units
from pint import UnitRegistry

from radiosonde_reader_writer import RadiosondeCzmlReader, RadiosondeCzmlWriter

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
        date, time, start_time = formatted_datetime(name)
        data = data_reader(s3_url)
        cleaned_data = clean_data(data, column_name_changes)
        ds = RadiosondeCzmlReader(cleaned_data, start_time)
        colors, lon, lat, alt, time, time_window, time_steps = ds.read_data()
        writer = RadiosondeCzmlWriter(colors, lon, lat, alt, time, time_window, time_steps)
        writer.set_time()
        writer.set_position()
        writer.set_pin_model()
        writer.set_points()

        output_czml = writer.get_string()
        print(output_czml)
        file_path = "/Users/Indhuja/Desktop/radiosonde/upload/rs_with_pin_latest.czml" # local path
        with open(file_path, 'w') as f:
            f.write(output_czml)
        
        upload_file("czml", os.path.dirname(file_path), bucket_name="ghrc-fcx-field-campaigns-szg", prefix=f"CPEX-AW/instrument-processed-data/radiosonde/czml")
      except Exception as e:
        print("Error during conversion for:: ", s3_url, ". Error on", e)

main()