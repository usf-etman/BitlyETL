import pandas as pd
import httpagentparser
import numpy as np
import argparse
from os import listdir
from os.path import join
from subprocess import PIPE, Popen
from datetime import datetime

parser = argparse.ArgumentParser()

#Add positional argument for directory path
parser.add_argument("dir",
                    help="Enter JSON files directory path")
                    
#Add optional -u option to store time in UNIX Epoch format
parser.add_argument("-u", "--unix", action="store_true", dest="unix", default=False,
                    help="Store time in UNIX Epoch format")

#Read directory path
args = parser.parse_args()
json_dir = args.dir

#Get list of JSON files at the specified directory
jsons = [join(json_dir, file) for file in listdir(json_dir) if file[-4:]=="json"]
print(f"Found {len(jsons)} JSON files:")
for file in jsons:
    print(file.split('/')[-1])

#Check for duplicates
checksums = {}
duplicates = []
uniques = []
for filename in jsons:
    # Use Popen to call the md5sum utility
    with Popen(["md5sum", filename], stdout=PIPE) as proc:
        # checksum command return list of two elements
        # value of hash function
        # file name
        # the following expression will retrieve only the value of hash function
        checksum = proc.stdout.read().split()[0]
        # Append duplicate to a list if the checksum is found
        if checksum in checksums:
            duplicates.append(filename)
        else:
            checksums[checksum] = filename
            uniques.append(filename)

print(f"\nFound {len(duplicates)} Duplicate(s):")
for dup in duplicates:
    print(dup.split('/')[-1])

#function to get browser name 
def getBrowser(ua):
    try:
        ret = httpagentparser.detect(ua)['browser']['name']
    except:
        ret = "Unknown Browser"
    return ret

#function to get OS name
def getOS(ua):
    try:
        ret = httpagentparser.detect(ua)['os']['name']
    except:
        ret = "Unknown OS"
    return ret

#function to shorten URL 
def shortenURL(url):
    try:
        ret = url.split('/')[2]
    except:
        ret = url
    return ret 

#function to get longitude
def getLongitude(s):
    s = str(s)
    s = s.replace('[','')
    lst = s.split(',')
    ret = lst[0]
    if ret == "nan":
        ret = np.nan
    return float(ret)

#function to get latitude
def getLatitude(s):
    s = str(s)
    s = s.replace(']','')
    lst = s.split(',')
    try:
        ret = lst[1]
    except:
        ret = np.nan
    return float(ret)
    
def convertEpoch(ts):
    try:
        ret = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    except:
        ret = ts
    return ret
 
#Transformation
for unique in uniques:
    print(f"\nTransforming file: {unique}")
    df1 = pd.read_json(unique, lines=True)
    df1.a.str.split()
    df1['web_browser'] = df1.a.map(getBrowser)
    df1['operating_sys'] = df1.a.map(getOS)
    df1['from_url'] = df1.r.map(shortenURL)
    df1['to_url'] = df1.u.map(shortenURL)
    df1['longitude'] = df1.ll.map(getLongitude)
    df1['latitude'] = df1.ll.map(getLatitude)
    
    if(~args.unix):
        df1['t'] = df1['t'].map(convertEpoch)
        df1['hc'] = df1['hc'].map(convertEpoch)
    
    print(f"Successfully transformed {df1.shape[0]} Rows")
    df1.rename({'cy':'city', 'tz':'time_zone', 't':'time_in', 'hc':'time_out'}, axis=1, inplace=True)
    df1_clean = df1[['web_browser', 'operating_sys', 'from_url', 'to_url', 'city', 'longitude',
                     'latitude', 'time_zone', 'time_in', 'time_out']].copy()
    df1_clean.dropna(inplace=True)
    print(f"Dropped {df1.shape[0] - df1_clean.shape[0]} Rows with null values")
    
    target = f"/mnt/g/Courses/ITI Material/21.Python for Data Management/Task 2/target/{unique[:-4]}csv"
    df1_clean.to_csv(target, index=False)
    print(f"Saved transformed data at {target}")