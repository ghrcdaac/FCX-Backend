import pandas as pd
import os
import glob
from datetime import datetime, timedelta
from utils.ingest_utils import *
from utils.pclouds import *
from flight_track import process_tracks

tInstr = {'2005-07-27':'T06:27:00Z'}

folder = "/Users/Indhuja/Desktop/tcsp/CRS"
files = glob.glob(os.path.join(folder, "CR050727*__REF.ER2"))
fileVEL = "/Users/Indhuja/Desktop/CR050727_1045__VEL.ER2"
fileER2 = "/Users/Indhuja/Desktop/tcsp/ER2/tcsp_naver2_20050727_9035.txt"

start = 31
step = 29

time = []
altitude = []
longitude = []
latitude = []
refdata = []
def readCRS():
    t1970 = datetime(1970,1,1)
    for fileREF in files:
        with open(fileREF, "r") as file:
            lines = file.readlines()
            dateline = lines[7].strip().split()
            line = lines[start::step]
            # print(line)
            for i in range(start+1, len(lines), step-1):
                print(i)
                line1 = lines[i:i+27]
                with open("/Users/Indhuja/Desktop/crs.txt", "w") as f:
                    for line in line1:
                        f.writelines(f"{line}\n")

                flat_list = [int(number) for sublist in [line.split() for line in line1] for number in sublist]

                # Find the maximum value and its position
                max_value = max(flat_list)
                max_position = flat_list.index(max_value)

                # print("Maximum Value:", max_value)
                # print("Position:", max_position)
                refdata.append(max_value)

            print(refdata)

            # Iterate through the list
            for item in line:
                parts = item.strip().split()
                if len(parts) >= 9:
                    # Extract and format data into appropriate columns
                    date = f"{dateline[0]}-{dateline[1]}-{dateline[2]} {int(parts[4]):02d}:{int(parts[5]):02d}:{int(parts[6]):02d}"
                    time.append((datetime.strptime(date, "%Y-%m-%d %H:%M:%S")))
                    altitude.append(int(parts[2]))
                    longitude.append(int(parts[7]))
                    latitude.append(int(parts[8]))

    return time, altitude, longitude, latitude, refdata

def readER2(fileER2):
    data = process_tracks()
    return data


def mk_RAD(RAD):
        fdate = "2005-07-27"
        t1970 = datetime(1970,1,1)
        tFlight = datetime.strptime(fdate + tInstr[fdate], "%Y-%m-%dT%H:%M:%SZ")
        print(tFlight)

        RAD.loc[:, 'time'] = RAD['time'].apply(lambda x: (x - t1970).total_seconds())

        SecS = (tFlight - t1970).total_seconds()  #to be consistent with across all ER-2 measurements, 
        RAD['timeP'] = RAD['time'] - SecS         #time is counted from SecS in visualization
        # print(SecS)
        print(RAD['timeP'],RAD['time'])

        lonw, lone = RAD['lon'].min()-0.2, RAD['lon'].max()+0.2
        lats, latn = RAD['lat'].min()-0.2, RAD['lat'].max()+0.2
        altb, altu = RAD['alt'].min(),RAD['alt'].max()
        bigbox = [lonw, lats, lone, latn, altb, altu] #*to_rad
        print(bigbox)

        nPoints = len(RAD)
        Tsize = 500000
        nTile = nPoints//Tsize
        if(nPoints%Tsize > 0): nTile += 1
        print(' Valid data points:',nPoints)

        #----Make pointcloud tiles
        # for vname in {Vars}:
        vname = "ref"
        print(' -Making pointcloud tileset for',vname)
        # folder= outDir0+ '/'+bandSel+'_'+vname
        # mkfolder(folder)

        tileset = Tileset(vname,bigbox,SecS)

        for tile in range (nTile):
            if(tile ==0):
                epoch = SecS         #--epoch and end are seconds from (1970,1,1)
            else:
                epoch = RAD['time'][tile*Tsize]   #SecS + tile*Tsize
            end = RAD['time'][min((tile+1)*Tsize, nPoints-1)]
            subset = RAD[(RAD['time'] >= epoch) & (RAD['time'] < end)]
            print(subset)
            make_pcloudTile(vname, tile, tileset, subset, epoch, end, folder)


time, altitude, longitude, latitude, refdata = readCRS()

data = {
    'time': time,
    'alt': altitude,
    'lon': longitude,
    'lat': latitude,
    'ref': refdata
}
df = pd.DataFrame(data)

df['lon'] = df['lon'] / 1000
df['lat'] = df['lat'] / 1000
df['ref'] = df['ref'] / 100
df = df.sort_values(by=['time'])
df = df.reset_index(drop=True)
print(df)

RAD = df[(df['alt'] >= 0) & (df['alt'] <= 20000)] #<--mid_lat winter storm (12000 would do)
RAD = RAD.reset_index(drop=True)
# print(' In range data points:',len(RAD))
dfER2 = readER2(fileER2)


merged_df = pd.merge(dfER2, df, left_on="timestamp",right_on="time", how="inner")
mk_RAD(merged_df)

readCRS()