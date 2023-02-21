#########################################
#----mk_RADpcloud.py
# Only used ATB
#########################################
from glob import glob
from datetime import datetime
from utils.Utils import *
from utils.pcloud_subs import *
from utils.s3 import fileList, uploadFiles



#to_rad = np.pi / 180.0
#to_deg = 180.0 / np.pi

CPLpath= '/Storage/Impacts/data/'
outDir0 = '/Storage/Impacts/VISdata/'   #head dir for outputs

def mk_CPLpcloud(s3FilePath, outputFolder):
    sdate = s3FilePath.split('_')[5].split(".")[0]
    fdate = '{}-{}-{}'.format(sdate[:4], sdate[4:6], sdate[6:])
    
    # Extract necessary data cols as pandas dataframe.
    CPL, _, _ = ER2CPL(s3FilePath)

    ## data preprocessing start ##
    
    # Date-time correction in extracted CPL df.
    t0 = datetime.strptime(fdate,"%Y-%m-%d")
    t1970 = datetime(1970,1,1)
    # tFlight = datetime.strptime(fdate + tInstr[fdate], "%Y-%m-%dT%H:%M:%SZ") #tlflight with both date and time. A hash used to map flight time for each date. TODO: if really needed, device a way
    tFlight = datetime.strptime(fdate+"T00:00:00Z", "%Y-%m-%dT%H:%M:%SZ")

    Sec0 = (t0 - t1970).total_seconds()       #<-- current day's 00Z from (1970,1,1) 00Z
    CPL['Time'] = [Sec0 + s for s in CPL['Secs']]
    
    SecS = (tFlight - t1970).total_seconds()  #to be consistent with RAD, REALLY NEED IT NOW???
    CPL['timeP'] = CPL['Time'] - SecS

    # Necessary coordinate data necessary to create 3d tileset.
    lonw,lone=CPL['lon'].min()-0.2,CPL['lon'].max()+0.2
    lats,latn=CPL['lat'].min()-0.2,CPL['lat'].max()+0.2
    altb,altu=CPL['alt'].min()-0.2,CPL['alt'].max()+0.2
    bigbox=[lonw, lats, lone, latn, altb, altu] #*to_rad

    ## 3dtileset generation start 
    # tileset object is meant to hold all the necessary json information for 3d tile.
    # initially it is loaded with base information, later for each tile, data are added to it.
    tileset=Tileset('CPL_atb',bigbox, SecS)

    Tsize = 50000
    nPoints = len(CPL)
    nTile = int(nPoints/Tsize) + 1

    for tile in range (nTile):
        #--epoch and end are seconds from (1970,1,1)
        if(tile ==0):
            epoch = SecS
        else:
            epoch =  CPL['Time'][tile*Tsize]   #SecS + tile*Tsize
        end = CPL['Time'][min((tile+1)*Tsize, nPoints-1)]
        subCPL = CPL[(CPL['Time'] >= epoch) & (CPL['Time'] < end)] #subset of cpl inside necessary timeframe
        #print(sec2Z(epoch),sec2Z(end),len(subCPL))

        make_pcloudTile('atb',tile, tileset, subCPL, epoch, end, outputFolder)

    ## data preprocessing end ##

def data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name):
    sourcePrefixPath=f"{field_campaign}/{input_data_dir}/{instrument_name}/olympex"
    destinationPrefixPath=f"{field_campaign}/{output_data_dir}/{instrument_name}"
    
    results = fileList(bucket_name, sourcePrefixPath)
    for s3_raw_file_key in results:
        src_s3_path = f"s3://{bucket_name}/{s3_raw_file_key}"

        # Create DIR for Output.        
        sdate = src_s3_path.split('_')[5].split(".")[0]
        fdate = '{}-{}-{}'.format(sdate[:4], sdate[4:6], sdate[6:])
        tempfolder= f'/tmp/cpl_converted/{fdate}/cpl/atb/'
        mkfolder(tempfolder) 

        # prepare pointcloud 3dtileset
        mk_CPLpcloud(src_s3_path, tempfolder)
        
        # upload to s3
        dest_s3_path = f"{destinationPrefixPath}/{sdate}"
        uploadFiles(bucket_name, tempfolder, dest_s3_path)
        return # break after 1 loop for dev.

bucket_name = ""


def cpl():
    # bucket_name = os.getenv('RAW_DATA_BUCKET')
    bucket_name="ghrc-fcx-field-campaigns-szg"
    field_campaign = "Olympex"
    input_data_dir = "instrument-raw-data"
    output_data_dir = "instrument-processed-data"
    instrument_name = "cpl"
    data_pre_process(bucket_name, field_campaign, input_data_dir, output_data_dir, instrument_name)


cpl()