import pandas as pd
import boto3
from datetime import datetime, timedelta
import json
import numpy as np
import os
from flight_track import FlightTrackReader


def makeCZML(fileLIP, fdate):

    # Pre-process LIP data, get rid of "NaN"
    valid_lines = []
    for line in fileLIP:
        line = line.decode()
        if ("NaN" not in line):
            valid_lines.append(line.split())
    dfLIP = pd.DataFrame(valid_lines)

    # Process LIP data
    dfLIP.drop(columns=dfLIP.columns[6:10], inplace=True)
    dfLIP = dfLIP.drop(0,axis=0).reset_index(drop=True) #remove 1st row as it has old column names
    dfLIP.columns = ['Date', 'Time', 'Ex', 'Ey', 'Ez', 'Eq']
    dfLIP["Time"] = dfLIP["Time"].str.split('.').str[0]
    
    # Get lat, lon from corresponding ER2 file
    fileER2 = FlightTrackReader()
    dfER2 = fileER2.process_tracks(f"20{fdate}", True)
    
    # Merge ER2 and LIP columns
    merged_df = pd.merge(dfER2, dfLIP, on="Time", how="inner")
    # print("merged_df>>>>\n",merged_df)
    
    merged_df[['Ex','Ey','Ez','Eq']] = merged_df[['Ex','Ey','Ez','Eq']].astype(float)
    
    df = merged_df.groupby(['Time', 'Date'], as_index=False).agg({'Ex': 'mean', 'Ey': 'mean', 'Ez': 'mean', 'Eq': 'mean',
                                                            'latitude': 'mean', 'longitude': 'mean'})

    # Display would last 60 sec
    tform1 = '%Y-%m-%d %H:%M:%S'
    tform2 = '%Y-%m-%dT%H:%M:%SZ'
    df['time2'] = [(datetime.strptime(d + ' ' + h, tform1) +
                    timedelta(seconds=60)).strftime(tform2)
                    for d, h in zip(df['Date'], df['Time'])] 

    # Include electric field of significant value
    df = df[(np.abs(df['Ex']) > 0.15) & (np.abs(df['Ey']) > 0.15) &(np.abs(df['Ez']) > 0.15) ]
    df = df.reset_index(drop=True)

    # Making czml file
    czmlBody = [{"id": "document",
                    "name": "LIP",
                    "version": "1.0", }]

    LIP = df[['Date', 'Time', 'time2',
                'Ex', 'Ey', 'Ez', 'latitude', 'longitude']]

    for d, t, t2, ex, ey, ez, lat, lon in zip(LIP.Date, LIP.Time, LIP.time2,
                                                LIP.Ex, LIP.Ey, LIP.Ez,
                                                LIP.latitude, LIP.longitude):
        xb = ex * .05
        yb = ey * .05
        zb = ez * 2000
        packet = {
            'id': t,
            'availability': d + 'T' + t + 'Z/' + t2,
            'polyline': {
                'positions': {'cartographicDegrees': [lon, lat, 0,
                                                        lon + xb, lat + yb, 0 + zb]},
                'material': {
                    'polylineArrow': {
                        'color': {'rgba': [255, 55, 55, 255], }, }, },
                'width': 5}
        }

        czmlBody.append(packet)

    return json.dumps(czmlBody)


def process_LIP():
    s3_resource = boto3.resource('s3')
    bucket = "ghrc-fcx-field-campaigns-szg"
    s3bucket = s3_resource.Bucket(bucket)
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"tcsp/instrument-raw-data/LIP/TCSP_LIP_fieldmill_2005"):
        keys.append(obj.key)

    result = keys
    s3_client = boto3.client('s3')
    
    for infile in result:
        s3_file = s3_client.get_object(Bucket=bucket, Key=infile)
        data = s3_file['Body'].iter_lines()
        fdate = infile.split(".")[-2].split("_")[1]
        LIPczml = makeCZML(data, fdate)

        file_name = f"TCSP_LIP_20{fdate}.czml"
        output_directory = "tcsp/instrument-processed-data/LIP"
        outfile = os.path.join(output_directory, file_name)
        s3_client.put_object(Body=LIPczml, Bucket=bucket, Key=outfile)
        print(f'Upload complete.\n\n')

process_LIP()