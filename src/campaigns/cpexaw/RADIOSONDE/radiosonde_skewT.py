import os
from helper.utils import get_files, upload_file, data_reader, formatted_datetime, clean_data
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import metpy.calc as mpcalc
from metpy.cbook import get_test_data
from metpy.plots import add_metpy_logo, SkewT
from metpy.units import units
from metpy.calc import wind_components
import pint

column_name_changes = {
        'Time [sec]': 'Time',
        'P [h Pa]': 'Pres',
        'T [°C]': 'Temp',
        'U [%]': 'RH',
        'Wsp [m/s]' : 'Wsp', 
        'Wdir [°]' : 'Wdir',
        'Lon [°]   ': 'Lon',
        'Lat [°]  ': 'Lat',
        'Altitude [m]': 'Alt',
        'Dew [°C]': 'DP'
    }

class RadiosondeSkewT:
    def __init__(self):
        # constructor
        pass

    def extract_data(self, df, time):
        start_time = pd.to_datetime(time)
        df['Datetime'] = start_time + pd.to_timedelta(df['Time'], unit='s')
        df['RH'] = pd.to_numeric(df['RH'], errors='coerce') 
        wsp = pd.to_numeric(df['Wsp'], errors='coerce')
        df = df.sort_values(by='Time').reset_index(drop=True)

        sort_idx = np.argsort(df['Time'])

        lon = df['Lon'][sort_idx]
        lat = df['Lat'][sort_idx]
        alt = df['Alt'][sort_idx]
        time = df['Time'][sort_idx]
        rh = df['RH'][sort_idx]
        dp = df['DP'][sort_idx]
        tdry = df['Temp'][sort_idx]
        pressure = df['Pres'][sort_idx]
        wsp = df['Wsp'][sort_idx]
        wdir = df['Wdir'][sort_idx]


        mask = np.logical_and(alt != -999.0, lon != -999.0, lat != -999.0)
        lon = lon[mask]
        lat = lat[mask]
        alt = alt[mask]
        time = time[mask]
        rh = rh[mask]
        dp = dp[mask]
        tdry = tdry[mask]
        pressure = pressure[mask]
        wsp = wsp[mask]
        wdir = wdir[mask]

        mask = np.logical_and(rh > -100, rh > -100)
        lon = lon[mask]
        lat = lat[mask]
        alt = alt[mask]
        time = time[mask]
        rh = rh[mask]
        dp = dp[mask]
        tdry = tdry[mask]
        pressure = pressure[mask]
        wsp = wsp[mask]
        wdir = wdir[mask]

        lon = lon[mask]
        lat = lat[mask]
        alt = alt[mask]
        time = time[mask]
        rh = rh[mask]
        dp = dp[mask]
        tdry = tdry[mask]
        pressure = pressure[mask]
        wsp = wsp[mask]
        wdir = wdir[mask]

        return (lon, lat, alt, time, rh, dp, tdry, pressure, wsp, wdir)

    def generate_skewT(self, file_path, height, pressure, temperature, dewpoint, wsp, wdir):
        df = pd.DataFrame(dict(zip(('height','pressure','temperature','dewpoint','wsp','wdir'),(height, pressure, temperature, dewpoint, wsp, wdir))))
        print(df)
        # Drop any rows with all NaN values for T, Td, winds
        df = df.dropna(subset=('height','pressure','temperature','dewpoint','wsp','wdir', 
                            ), how='all').reset_index(drop=True)
        
        # Change default to be better for skew-T
        plt.rcParams['figure.figsize'] = (9, 9)

        df['pressure'] = pd.to_numeric(df['pressure'], errors='coerce')
        print(df['pressure'].dtype)
        P = df['pressure'].values.astype(float) * units.hPa
        T = df['temperature'].values * units.degC
        df['dewpoint'] = pd.to_numeric(df['dewpoint'], errors='coerce')
        print(df['dewpoint'].dtype)
        Td = df['dewpoint'].values * units.degC
        print((df['dewpoint']))

        wsp = df['wsp'].values.astype(float) * units('m/s')
        # wsp = wsp * ureg('m/s')

        df['wdir'] = pd.to_numeric(df['wdir'], errors='coerce')
        wdir = np.array(df['wdir'].values)

        wdir_rad = np.radians(wdir)
        # u_wind = wsp * np.sin(wdir_rad)
        # v_wind = wsp * np.cos(wdir_rad)
        u_wind, v_wind = wind_components(wsp , wdir_rad)

        skew = SkewT()
        # Plot the data using normal plotting functions, in this case using
        # log scaling in Y, as dictated by the typical meteorological plot
        skew.plot(P, T, 'r')
        skew.plot(P, Td, 'g')

        # # Set some better labels than the default
        skew.ax.set_xlabel('Temperature (\N{DEGREE CELSIUS})')
        skew.ax.set_ylabel('Pressure (mb)')

        ## for barbs

        # Set spacing interval--Every 50 mb from 1000 to 100 mb
        my_interval = np.arange(100, 1000, 100) * units('mbar')

        # Get indexes of values closest to defined interval
        ix = mpcalc.resample_nn_1d(P, my_interval)
        skew.plot_barbs(P[ix], wsp[ix], wdir_rad[ix])

        # Add the relevant special lines
        skew.plot_dry_adiabats()
        skew.plot_moist_adiabats()
        skew.plot_mixing_lines()
        skew.ax.set_ylim(1000, 100)

        plt.savefig(file_path)
        plt.close()

def main():
    rs = RadiosondeSkewT()
    s3_url_list = get_files()
    for s3_url in s3_url_list:
        try:
            name = s3_url.split('/')[-1]
            date, time, start_time = formatted_datetime(name)
            path = r'/Users/Indhuja/Desktop/radiosonde/skewT/' + date
            if not os.path.exists(path):
                os.makedirs(path)
            data = data_reader(s3_url)
            print(data)
            cleaned_data = clean_data(data, column_name_changes)
            print(f"CPEX-AW/instrument-processed-data/radiosonde/skewT/{date}/radiosonde-{time}.png")
            lon, lat, alt, time1, rh, dp, tdry, pressure, wsp, wdir = rs.extract_data(cleaned_data, start_time)
            rs.generate_skewT(f"{path}/{name}.png", alt, pressure, tdry, dp, wsp, wdir)
            upload_file("skewT", f"{path}/{name}.png", bucket_name="ghrc-fcx-field-campaigns-szg", prefix=f"CPEX-AW/instrument-processed-data/radiosonde/skewT/{date}/radiosonde-{time}.png")
        except Exception as e:
            print("Error during conversion for: ", s3_url, ". Error on", e)

main()