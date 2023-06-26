import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import metpy.calc as mpcalc
from metpy.plots import SkewT
from metpy.units import units

import boto3
from boto3 import client as boto_client

class DropsondeSkewT:
  def __init__(self):
    # constructor
    pass
  
  def get_files(self, bucket_name="ghrc-fcx-field-campaigns-szg", prefix="CPEX-AW/instrument-raw-data/dropsonde"):
    s3_resource = boto3.resource('s3')
    s3bucket = s3_resource.Bucket(bucket_name)    
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"{prefix}/CPEXAW-DROPSONDE_"):
        url = "s3://" + bucket_name + "/" + prefix + "/" + obj.key
        keys.append(url)
    return keys
    
  def upload_file(self):
    pass
  
  def data_reader(self, key):
    ## Open data file
    print(key.split("/")[-1])
    s3 = boto_client('s3')
    fileobj = s3.get_object(Bucket="ghrc-fcx-field-campaigns-szg", Key="/"+key.split("//")[1])
    file = fileobj['Body'].read()
    with xr.open_dataset(file, decode_cf=False) as ds:
        rh = ds['rh'].values # relative humidity
        dp = ds['dp'].values # dew point
        tdry = ds['tdry'].values # temp
        lat = ds['lat'].values
        lon = ds['lon'].values
        alt = ds['alt'].values
        time = ds['time'].values
        pressure = ds['pres'].values
        u_wind = ds['u_wind'].values
        v_wind = ds['v_wind'].values

    ## Data formation
    
    #1. sort data by time
    sort_idx = np.argsort(time)

    lon = lon[sort_idx]
    lat = lat[sort_idx]
    alt = alt[sort_idx]
    time = time[sort_idx]
    rh = rh[sort_idx]
    dp = dp[sort_idx]
    tdry = tdry[sort_idx]
    pressure = pressure[sort_idx]
    u_wind = u_wind[sort_idx]
    v_wind = v_wind[sort_idx]

    #2. remove nan and infinite and invalid values using mask
    mask = np.logical_and(alt != -999.0, lon != -999.0, lat != -999.0)
    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    time = time[mask]
    rh = rh[mask]
    dp = dp[mask]
    tdry = tdry[mask]
    pressure = pressure[mask]
    u_wind = u_wind[mask]
    v_wind = v_wind[mask]

    # contd. remove nan and infinite and invalid values using mask
    mask = np.logical_and(rh > -100, rh > -100)
    lon = lon[mask]
    lat = lat[mask]
    alt = alt[mask]
    time = time[mask]
    rh = rh[mask]
    dp = dp[mask]
    tdry = tdry[mask]
    pressure = pressure[mask]
    u_wind = u_wind[mask]
    v_wind = v_wind[mask]
    
    return (lon, lat, alt, time, rh, dp, tdry, pressure, u_wind, v_wind)
  
  def generate_skewT(self, fileName, height, pressure, temperature, dewpoint, u_wind, v_wind):
    df = pd.DataFrame(dict(zip(('height','pressure','temperature','dewpoint','u_wind','v_wind'),(height, pressure, temperature, dewpoint, u_wind, v_wind))))

    # Drop any rows with all NaN values for T, Td, winds
    df = df.dropna(subset=('height','pressure','temperature','dewpoint','u_wind','v_wind', 
                          ), how='all').reset_index(drop=True)
    P = df['pressure'].values * units.hPa
    T = df['temperature'].values * units.degC
    Td = df['dewpoint'].values * units.degC
    
    # Change default to be better for skew-T
    plt.rcParams['figure.figsize'] = (9, 9)
    
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
    my_interval = np.arange(100, 1000, 50) * units('mbar')
    # Get indexes of values closest to defined interval
    ix = mpcalc.resample_nn_1d(P, my_interval)
    skew.plot_barbs(P[ix], u_wind[ix], v_wind[ix])

    # Add the relevant special lines
    skew.plot_dry_adiabats()
    skew.plot_moist_adiabats()
    skew.plot_mixing_lines()
    skew.ax.set_ylim(1000, 100)

    plt.savefig(f'/tmp/dropsonde/output/skewT/{fileName}.png')


def main():
  ds = DropsondeSkewT()
  keylist = ds.get_files()
  for key in keylist:
    data = ds.data_reader(key)
    (lon, lat, alt, time, rh, dp, tdry, pressure, u_wind, v_wind) = data
    ds.generate_skewT(key.split("/")[-1], alt, pressure, tdry, dp, u_wind, v_wind)
    
main()