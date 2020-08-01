import pandas as pd
import numpy as np
import requests
from datetime import date, timedelta
from pathlib import Path

yesterday = date.today() - timedelta(days=1)
yesterday_full = yesterday.strftime("%A %B %m, %Y")
yesterday = yesterday.strftime("%Y%m%d")
bands = ['160m','80m','60m','40m','30m','20m','17m','15m','12m','10m','6m','4m']

def getRawDataRBN(date=yesterday):
    url = 'http://www.reversebeacon.net/raw_data/dl.php?f='+date
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        file = date+'.zip'
        directory_path = Path.cwd() / 'data'

        if directory_path.is_dir() == True:
            pass
        else:
            print('Creating directory',directory_path)
            directory_path.mkdir()

        path2file= directory_path / file
        print('Fetching',file,'from the RBN archive and saving it to',path2file)

        with path2file.open('wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return path2file


file = yesterday + '.zip'
datafile = Path.cwd() / 'data' / file

if datafile.exists() == True:
    print(datafile,'is an existing file')
else:
    print(datafile,'is NOT an existing file')
    datafile = getRawDataRBN()

df = pd.read_csv(datafile,keep_default_na=False,na_values='')
df = df.dropna(subset=['tx_mode'])

print('-------')
print('Summary of the RBN on',yesterday_full)
print('-------\n')
print(len(df.index),'Total Spots')
print(len(df['callsign'].unique()),'Active Skimmers on',len(df['de_cont'].unique()),'Continents and',len(df['de_pfx'].unique()),'DXCC entities' )
print(len(df['dx'].unique()),'Spoted stations on',len(df['dx_cont'].unique()),'Continents and',len(df['dx_pfx'].unique()),'DXCC entities')
cw_df = df[df['tx_mode'] == 'CW']
print(cw_df['speed'].mean().round(1),'WPM Average CW Speed')
print('\n')

# print(df['tx_mode'].unique())
print(df['mode'].unique())
# p rint(df.info())

#Getting the active bands array and sorting according the the bands list
active_bands = df['band'].unique().tolist()
sorted_active_bands = [band for band in bands if band in active_bands]
for band in sorted_active_bands:
    band_df = df[df['band'] == band]
    if len(band_df.index)>0:
        print(band,'-',len(band_df.index),'Total Spots -',len(band_df['dx'].unique()),'Spoted stations on',len(band_df['dx_cont'].unique()),'Continents and',len(band_df['dx_pfx'].unique()),'DXCC entities')


# top10 = df['callsign'].groupby(df['de_cont','band']).value_counts()
# top10 = df.groupby(['de_cont','callsign','band']).agg({'dx':['count'],'speed':['mean']})
# print(top10.to_string())

#top10_pivot = df.pivot(index='callsign',values='dx',columns='band',aggfunc='count')
#print(top10_pivot)
#print('\nBreakdown by Skimmer')

#Change to your callsign is your want to filter. You can also uncomment this line to get the results for all skimmers.  
df = df[df['callsign'] == '9V1RM']

skimmers = sorted(df['callsign'].unique())

for station in skimmers:
    print('\n')
    print('-------')
    print(station)
    station_df = df[df['callsign'] == station]
    print('-------\n')
    print(len(station_df.index),'Total Spots')
    print(len(station_df['dx'].unique()),'Spoted stations on',len(station_df['dx_cont'].unique()),'Continents and',len(station_df['dx_pfx'].unique()),'DXCC entities')
    print('-------\n')
    active_bands = df['band'].unique().tolist()
    sorted_active_bands = [band for band in bands if band in active_bands]
    for band in sorted_active_bands:
    # print(band)
        band_df = station_df[station_df['band'] == band]
        if len(band_df.index)>0:
            print(band,'-',len(band_df.index),'Total Spots -',len(band_df['dx'].unique()),'Spoted stations on',len(band_df['dx_cont'].unique()),'Continents and',len(band_df['dx_pfx'].unique()),'DXCC entities')
    # for mode in sorted_active_bands:
    
    ncdxf_df = station_df[station_df['mode'] == 'NCDXF B']
    if len(ncdxf_df.index) > 0:
        print('\nNorthern DX Californian Club Beacons\n')
        print(len(ncdxf_df['dx'].unique()),'NCDXF Beacons heard on', len(ncdxf_df['dx_cont'].unique()),'Continents')
        print(ncdxf_df.to_string())
        # ncdxf_List = ncdxc_df['dx'].unique()
        
        for beacon in ncdxf_df['dx'].unique():
            print(beacon)
            ncdxf_df = station_df[station_df['dx'] == beacon]
            if len(ncdxf_df.index)>0:
                for band in ncdxf_df['band'].unique():
                    beacon_ncdxf_df = ncdxf_df[ncdxf_df['band'] == band]
                    print(band,'-',len(beacon_ncdxf_df.index))
        
        ncdxf_df = station_df[station_df['mode'] == 'NCDXF B']
        df_pivot = pd.pivot_table(ncdxf_df,values='mode',index=['dx'],columns='band',aggfunc='count')
        print(df_pivot)