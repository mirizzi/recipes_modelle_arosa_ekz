# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
import os
from dataiku import pandasutils as pdu
from datetime import datetime as dt, timedelta as td
from  pytz import timezone
from utils import write_to_ftp_ekz


def ffill(df:pd.DataFrame, start_time:dt, end_time:dt, time_resolution:str):

    # Create a range of datetime values from "start_time" to "end_time" and a frequency of "time_resolution"
    dt_range = pd.date_range(start=start_time, end=end_time, freq=time_resolution)

    # Transform dt_range to DataFrame and define the column name as "datetime_column"
    dt_range = pd.DataFrame(index=dt_range, columns=['datetime'])
    dt_range['datetime'] = dt_range.index
    dt_range = dt_range.reset_index(drop=True)

    # Merge dt_range with the DataFrame "df" to be forward filled
    df = pd.merge(dt_range, df, on='datetime', how='left')

    # Forward Fill DataFrame "df"
    #df.fillna(method='ffill', inplace=True)
    df = df.interpolate(method='linear')

    return df

# CET from UTC
utc_tz = timezone('UTC')
cet_tz = timezone('Europe/Zurich')

# Read recipe inputs
EKZ_Waldhalde_Forecast_Hist = dataiku.Dataset("EKZ_Waldhalde_Forecast_Hist")
df = EKZ_Waldhalde_Forecast_Hist.get_dataframe()

# Convert utz to cet
df['datetime'] = df['datetime'].apply(lambda x: x.tz_localize(utc_tz).astimezone(cet_tz))

# define start and end time of export
start_cet = (dt.today()+td(days=-10)).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(cet_tz)
end_cet = max(df['datetime']).astimezone(cet_tz)

# keep only df between start and end:
mask = (df['datetime'] > start_cet) & (df['datetime'] <= end_cet)
df = df.loc[mask]

# extrapolate from 1h to 15Min resolution
df = ffill(df, start_cet, end_cet, '15Min')

# drop empty rows, if any
df = df.dropna().reset_index()

# take away timezone awareness
df['datetime'] = df['datetime'].apply(lambda x: x.tz_localize(None))

# Reformat datetime to string
df['datetime'] = df['datetime'].dt.strftime("%d.%m.%Y %H:%M")

# Round everything to 3 "Nachkommastellen"
df = df.round(3)

### EXPORT FOR PRECIPITATION ###
df_precip = df[['datetime', 'precip_mm']].copy()
df_precip['0'] = 'EKZ_Waldhalde_Niederschlag_mm_p_h_100314'
df_precip['2'] = '15min'
df_precip['3'] = 'REF'
df_precip['4'] = 0
df_precip['6'] = 'mm_p_h'

df_precip = df_precip[['0','datetime','2','3','4','precip_mm','6']]

# Save equations in folder
static_path = dataiku.Folder("fUZIpaNd").get_path()
filename = 'EKZ_Waldhalde_Niederschlag_mm_p_h_100314.csv'
df_precip.to_csv(os.path.join(static_path,filename), header=False, index=False, sep=";")


### EXPORT FOR PRODUCTION ###
df_prod = df[['datetime', 'Prod_FC_MW']].copy()
df_prod['0'] = 'EKZ_Waldhalde_Produktion_MW_100312'
df_prod['2'] = '15min'
df_prod['3'] = 'REF'
df_prod['4'] = 0
df_prod['6'] = 'MW'

df_prod = df_prod[['0','datetime','2','3','4','Prod_FC_MW','6']]
# Save equations in folder
static_path = dataiku.Folder("fUZIpaNd").get_path()
filename = 'EKZ_Waldhalde_Produktion_MW_100312.csv'
df_prod.to_csv(os.path.join(static_path,filename), header=False, index=False, sep=";")


#************************** EXPORT TO FTP SERVER **************************#
# Define remote directory path on the FTP server
directory = '/EKZ_forecasts_axpo'

# Define the list of dictionaries for file exports
exports = [
    {
        'filename': f"EKZ_Waldhalde_Niederschlag_mm_p_h_100314.csv",
        'data': df_precip
    },
    {
        'filename': f"EKZ_Waldhalde_Produktion_MW_100312.csv",
        'data': df_prod
    }
]

# Iterate over the list of exports and perform the file exports
for export in exports:
    filename = export['filename']
    data = export['data']
    write_to_ftp_ekz(directory, filename, data, key_acc="CKW_FTP_READ_ACC", key_pass="CKW_FTP_READ_PASS")


#************************** Write recipe outputs **************************#
EKZ_Waldhalde_FTP_Exports = dataiku.Folder("fUZIpaNd")
EKZ_Waldhalde_FTP_Exports_info = EKZ_Waldhalde_FTP_Exports.get_info()