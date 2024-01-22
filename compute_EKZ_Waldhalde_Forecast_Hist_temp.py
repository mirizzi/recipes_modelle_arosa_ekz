import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt, timedelta as td
from  pytz import timezone

# Read recipe inputs
EKZ_Waldhalde_postprocessed = dataiku.Dataset("EKZ_Waldhalde_postprocessed")
df = EKZ_Waldhalde_postprocessed.get_dataframe()

# CET from UTC
utc_tz = timezone('UTC')
cet_tz = timezone('Europe/Zurich')


df = df.rename(columns={'datetime_cet':'Zeitstempel [CET, von]'})
df['Zeitstempel [CET, von]'] = df['Zeitstempel [CET, von]'].apply(lambda x: x.tz_localize(utc_tz))

# rename columns
df = df.rename(columns={'Zeitstempel [CET, von]':'datetime', 'Waldhalde Forecast [MW]':'Prod_FC_MW'})

# drop HIST column
df = df.drop(columns={'Waldhalde Hist [MW]'})

# drop empty rows
df = df.dropna(subset=['Prod_FC_MW'], how='all')

# Write recipe outputs
EKZ_Waldhalde_Forecast_Hist_temp = dataiku.Dataset("EKZ_Waldhalde_Forecast_Hist_temp")
EKZ_Waldhalde_Forecast_Hist_temp.write_with_schema(df)