from datetime import datetime
import numpy as np
import boto3
import os
import shutil

to_rad = np.pi / 180
to_deg = 180 / np.pi

def regionrad(region): 
    return [r*to_rad for r in region]

def sec2Z(t): 
    return "{}Z".format(datetime.utcfromtimestamp(t).isoformat())


start = 31
step = 29

def readCRS(result, bucket, type):
    time = []
    altitude = []
    longitude = []
    latitude = []
    refdata = []
    s3_client = boto3.client('s3')
    for infile in result:
        s3_file = s3_client.get_object(Bucket=bucket, Key=infile)
        print(infile)
        generator_lines = s3_file['Body'].iter_lines()
        lines = []
        for line in generator_lines:
            lines.append(line.decode())
        dateline = lines[7].strip().split()
        line = lines[start::step]
        for i in range(start+1, len(lines), step):
            line1 = lines[i:i+28]
            newList = []
            if(type == 'REF'):
                rad_array = list(map(int,(" ".join(line1)).split()))
                newList = [x/100 for x in rad_array]
            else:
                rad_array = list(map(float,(" ".join(line1)).split()))
                newList = [x for x in rad_array]
            refdata.append(newList)

        # print(refdata)

        for item in line:
            parts = item.strip().split()
            if len(parts) >= 9:
                # Creating timestamp to compare and retrieve heading, pitch, roll from ER2 data
                date = f"{dateline[0]}-{dateline[1]}-{dateline[2]} {int(parts[4]):02d}:{int(parts[5]):02d}:{int(parts[6]):02d}"
                time.append((datetime.strptime(date, "%Y-%m-%d %H:%M:%S")))
                altitude.append(int(parts[2]))
                longitude.append(int(parts[7]))
                latitude.append(int(parts[8]))

    return time, altitude, longitude, latitude, refdata

def down_vector(roll, pitch, head):
    x = np.sin(roll) * np.cos(head) + np.cos(roll) * np.sin(pitch) * np.sin(head)
    y = -np.sin(roll) * np.sin(head) + np.cos(roll) * np.sin(pitch) * np.cos(head)
    z = -np.cos(roll) * np.cos(pitch)
    return (x, y, z)

def create_zarr_dir(fdate):
    """Create a directory to hold zarr file
    """
    tempdir = 'temp/' + fdate + '/zarr'
    if os.path.exists(tempdir):
        shutil.rmtree(tempdir)
    os.makedirs(tempdir)
    return tempdir