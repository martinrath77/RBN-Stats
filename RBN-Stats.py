import pandas as pd
import numpy as np
import requests
from datetime import date, timedelta
from pathlib import Path

yesterday = date.today() - timedelta(days=1)
yesterday = yesterday.strftime("%Y%m%d")

def getRawDataRBN(date=yesterday):
    url = 'http://www.reversebeacon.net/raw_data/dl.php?f='+date
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        file = date+'.zip'
        path2file= Path.cwd() / 'data' / file
        print(path2file)
        with path2file.open('wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        return path2file

datafile = getRawDataRBN()
print(datafile)
df = pd.read_csv(datafile,keep_default_na=False,na_values='')
df = df.dropna(subset=['tx_mode'])
print(df.tail(5))
# print(df.info())
# Skimmers_call = df['callsign'].unique()
# print(Skimmers_call)
print(len(df.index),'Total Spots')
print(len(df['callsign'].unique()),'Active Skimmers on',len(df['de_cont'].unique()),'Continents and',len(df['de_pfx'].unique()),'DXCC entities' )
print(len(df['dx'].unique()),'Spoted stations on',len(df['dx_cont'].unique()),'Continents and',len(df['dx_pfx'].unique()),'DXCC entities')

# print(df.describe())
print(df['tx_mode'].unique())
print(df['mode'].unique())
print(df['band'].unique())

cw_df = df[df['tx_mode'] == 'CW']
print(cw_df['speed'].mean().round(1),'WPM Average CW Speed')

df = df[df['callsign'] == '9V1RM']

for station in df['callsign'].unique():
    print('-------')
    print(station)
    station_df = df[df['callsign'] == station]
    print('-------')
    print(len(station_df.index),'Total Spots')
    print(len(station_df['dx'].unique()),'Spoted stations on',len(station_df['dx_cont'].unique()),'Continents and',len(station_df['dx_pfx'].unique()),'DXCC entities')
    print('-------')
    for mode in station_df['mode'].unique():
        band_df = station_df[station_df['mode'] == mode]
        print(band_df.head(5))
        for band in band_df['band'].unique():
            # print(band)
            band_df = band_df[band_df['band'] == band]
            if len(band_df.index)>0:
                print(mode,'-',band,'-',len(band_df.index),'Total Spots -',len(band_df['dx'].unique()),'Spoted stations on',len(band_df['dx_cont'].unique()),'Continents and',len(band_df['dx_pfx'].unique()),'DXCC entities')
