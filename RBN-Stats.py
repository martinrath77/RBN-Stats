import pandas as pd
import numpy as np
import requests

df = pd.read_csv('20200712.zip',keep_default_na=False,na_values='')
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

cw_df = df[df['tx_mode'] == 'CW']
print(cw_df['speed'].mean().round(1),'WPM Average CW Speed')


# df = df[df['mode'].isnull()]
# print(df)