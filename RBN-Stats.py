import pandas as pd
import requests

df = pd.read_csv('20200712.zip')
print(df.head(5))
print(df.info())
Skimmers_call = df['callsign'].unique()
print(Skimmers_call)
print('They were', len(Skimmers_call),'Skimmer stations online today on the RBN' )