import glob
import pandas as pd
import boto3
from datetime import datetime, timedelta
import json
import numpy as np
from utils.ingest_utils import generator_to_np, convert_time, get_col_index_map

def readCZML(fileER2):

    # df1 = pd.read_csv(fileLIP, sep=r"\s+", header=None, skiprows=1, usecols = [1,7,9])
    # df1.columns = ["Time", "lat", "lon"]
    # df1["Time"] = df1["Time"].apply(convert_time)
    # print(df1)
    
    print(fileER2)
    data = generator_to_np(fileER2)
    # print(data)
    col_index_map = get_col_index_map()
    data = pd.DataFrame(data)
    print(data)
    time = data.loc[:, col_index_map["time"]]
    lat = data.loc[:, col_index_map["gLatitude"]].astype(float)
    lon = data.loc[:, col_index_map["gLongitude"]].astype(float)
    df = pd.DataFrame(data = {"Time": time,"lat": lat, "lon": lon})
    df["Time"] = df["Time"].apply(lambda x: ':'.join(x.split(':')[-3:]))
    print(df)
    return df

#     #compare ER2 and cond file wrt time
#     merged_df_1 = pd.merge(df1, df2, on="Time", how="inner")
#     file_name = "/Users/Indhuja/Desktop/tcsp/lip/TCSP_LIP_cond_2005.187_050706_1948_merged.txt"
#     merged_df_1.to_csv(file_name, sep=' ')
#     print(merged_df_1)

#     return merged_df_1

def makeCZML(fileER2):
    file1 = "/Users/Indhuja/Desktop/tcsp/lip/TCSP_LIP_fieldmill_2005.187_050706.txt"
    file2 = "/Users/Indhuja/Desktop/tcsp/lip/TCSP_LIP_fieldmill_2005.187_050706_valid.txt"
    with open(file1, "r") as f:
        lines = f.readlines()
    with open(file2, "w") as f:
        for line in lines:
            if ("NaN" not in line):
                f.write(line)
    df3 = pd.read_csv(file2, sep=" ", header=None, skiprows=1, usecols=[0,1,2,3,4,5])
    df3.columns = ['Date', 'Time', 'Ex', 'Ey', 'Ez', 'Eq']
    df3["Time"] = df3["Time"].str.split('.').str[0]
    print(df3)

    #compare merged_df_1 with LIP wrt time
    merged_df = pd.merge(readCZML(fileER2), df3, on="Time", how="inner")
    # print(merged_df)

    print("merged_df>>>>\n",merged_df)

    df = merged_df.groupby(['Time', 'Date'], as_index=False).agg({'Ex': 'mean', 'Ey': 'mean', 'Ez': 'mean', 'Eq': 'mean',
                                                            'lat': 'mean', 'lon': 'mean'})

    tform1 = '%Y-%m-%d %H:%M:%S'
    tform2 = '%Y-%m-%dT%H:%M:%SZ'
    df['time2'] = [(datetime.strptime(d + ' ' + h, tform1) +
                    timedelta(seconds=60)).strftime(tform2)
                    for d, h in zip(df['Date'], df['Time'])] 

    # df = df[(np.abs(df['Ex']) > 0.05) & (np.abs(df['Ey']) > 0.05) &(np.abs(df['Ez']) > 0.05) ]
    df = df.reset_index(drop=True)
    print(df)


    # czmlBody = [{"id": "document",
    #                 "name": "LIP",
    #                 "version": "1.0", }]

    # LIP = df[['Date', 'Time', 'time2',
    #             'Ex', 'Ey', 'Ez', 'lat', 'lon']]
    # print(LIP)

    # for d, t, t2, ex, ey, ez, lat, lon in zip(LIP.Date, LIP.Time, LIP.time2,
    #                                             LIP.Ex, LIP.Ey, LIP.Ez,
    #                                             LIP.lat, LIP.lon):
    #     xb = ex * .05
    #     yb = ey * .05
    #     zb = ez * 2000
    #     packet = {
    #         'id': t,
    #         'availability': d + 'T' + t + 'Z/' + t2,
    #         'polyline': {
    #             'positions': {'cartographicDegrees': [lon, lat, 0,
    #                                                     lon + xb, lat + yb, 0 + zb]},
    #             'material': {
    #                 'polylineArrow': {
    #                     'color': {'rgba': [255, 55, 55, 255], }, }, },
    #             'width': 5}
    #     }

    #     czmlBody.append(packet)

    # LIPczml = json.dumps(czmlBody)
    # filepath = "/Users/Indhuja/Desktop/tcsp/lip/LIP0607.czml"
    # CZMLfile = open(filepath, "w")
    # CZMLfile.write(LIPczml)
    # CZMLfile.close()

fileLIP = glob.glob('/Users/Indhuja/Desktop/tcsp/lip/TCSP_LIP_cond_2005.187_050706_1948.txt')[0]
fileER2 = glob.glob('/Users/Indhuja/Desktop/tcsp/ER2/tcsp_naver2_20050706_9026.txt')[0]
print(fileER2)
def process_tracks():
    s3_resource = boto3.resource('s3')
    bucket = "ghrc-fcx-field-campaigns-szg"
    s3bucket = s3_resource.Bucket(bucket)
    keys = []
    for obj in s3bucket.objects.filter(
            Prefix=f"tcsp/instrument-raw-data/ER2_Flight_Nav/tcsp_naver2_20050706"):
        keys.append(obj.key)

    result = keys
    s3_client = boto3.client('s3')
    
    for infile in result:
        s3_file = s3_client.get_object(Bucket=bucket, Key=infile)
        print(infile)
        data = s3_file['Body'].iter_lines()
        #readCZML(data)
        makeCZML(data)

process_tracks()