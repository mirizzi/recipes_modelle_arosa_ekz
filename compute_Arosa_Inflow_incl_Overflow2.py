# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from ftplib import FTP
import json
from utils import ftp_prep, write_to_ftp
from datetime import datetime as dt, timedelta as td
import pytz
from pytz import timezone as tz
from scipy.interpolate import interp1d

# Inputs  !!! to be checked !!!
offset_masl=989.08
prod_shift=4 # number of 15minutes time shit between litzirueti and molinis


# Read recipe inputs
lake_table_isel = dataiku.Dataset("lake_table_isel")
lake_table_isel_df = lake_table_isel.get_dataframe()
isel_table_m3 = lake_table_isel_df['volume [Mm3]'].tolist()
isel_table_masl = lake_table_isel_df['lake_level [masl]'].tolist()

Molinis_Seestand_Uberlauf = dataiku.Dataset("Molinis_Seestand_Uberlauf")
Molinis_Seestand_Uberlauf_df = Molinis_Seestand_Uberlauf.get_dataframe()
Molinis_Seestand_Uberlauf_df['Überlauf Wehr Molinis (m)']=round(Molinis_Seestand_Uberlauf_df['Überlauf Wehr Molinis (m)'],2)

Isel_Ueberlauf_Klappenstellung = dataiku.Dataset("Isel_Ueberlauf_Klappenstellung")
Isel_Ueberlauf_Klappenstellung_df = Isel_Ueberlauf_Klappenstellung.get_dataframe()


# Lake level calculator
def isel_masl_to_m3(masl_value, masl, m3):
    if masl_value > max(masl):
        interpolated_value = max(m3)+0.1
    elif masl_value < min(masl):
        interpolated_value = min(m3)-0.1
    else:
        f = interp1d(masl, m3)
        interpolated_value = f(masl_value)
    return interpolated_value

# Isel overflow calculator
def isel_overflow(masl_value, flap_position,Isel_Ueberlauf_Klappenstellung_df):
    flap_position=round(flap_position,2)
    if flap_position < 0.4:
        flap_position=0.4
    elif flap_position > 1:
        flap_position=1

    masl_value=round(masl_value,2)
    if masl_value > max(Isel_Ueberlauf_Klappenstellung_df['Seestand']):
        masl_value = max(Isel_Ueberlauf_Klappenstellung_df['Seestand'])
    elif masl_value < min(Isel_Ueberlauf_Klappenstellung_df['Seestand']):
        masl_value = min(Isel_Ueberlauf_Klappenstellung_df['Seestand'])

    overflow=Isel_Ueberlauf_Klappenstellung_df[Isel_Ueberlauf_Klappenstellung_df['Seestand']==masl_value]
    overflow=overflow[[str(flap_position)]]

    return overflow.iat[0,0]

def molinis_overflow(height, Molinis_Seestand_Uberlauf_df):
    height=round(height,2)

    if height > max(Molinis_Seestand_Uberlauf_df['Überlauf Wehr Molinis (m)']):
        height = max(Molinis_Seestand_Uberlauf_df['Überlauf Wehr Molinis (m)'])
    elif height < min(Molinis_Seestand_Uberlauf_df['Überlauf Wehr Molinis (m)']):
        height = min(Molinis_Seestand_Uberlauf_df['Überlauf Wehr Molinis (m)'])

    overflow=Molinis_Seestand_Uberlauf_df[Molinis_Seestand_Uberlauf_df['Überlauf Wehr Molinis (m)']==height]/1000 # the table is in l/s, thus the facto 1000 to have m3/s

    return overflow.iat[0,0]


# Access FTP Server
client = dataiku.api_client()
auth_info = client.get_auth_info(with_secrets=True)

secret_value = None
secret_key = ["CKW_FTP_READ_ACC", "CKW_FTP_READ_PASS"]

key_df = pd.DataFrame(index=np.arange(len(secret_key)), columns=["key", "value"])

key_counter = 0
for secret in auth_info["secrets"]:
    if key_counter < len(secret_key) and secret_key[key_counter] == secret['key']:
        key_df.iat[key_counter,0] = secret['key']
        key_df.iat[key_counter,1] = secret["value"]
        key_counter += 1

# Define FTP server details
ftp_host = 'transfer.ckw.ch'
ftp_username = str(key_df.iat[0,1])
ftp_password = str(key_df.iat[1,1])

# Connect to the host
ftp = FTP(ftp_host)
ftp.login(user=ftp_username, passwd=ftp_password)

# List all subfolders (directories) in the current working directory
ftp.cwd('/emsi')

# Specify the filename to download
filename = 'timeseries.json'

# Download the file and store its contents in a local variable
with open(filename, 'wb') as file:
    ftp.retrbinary(f'RETR {filename}', file.write)

# Close the FTP connection
ftp.quit()

# Read the contents of the downloaded JSON file and load it into a Python dictionary
with open(filename, 'r') as json_file:
    emsi_data = json.load(json_file)

### 1. Get data for Arosa
litzirueti_throughput = emsi_data.get('KW.ARO.MW.F_LITZIR:10')
litzirueti_throughput = pd.read_json(json.dumps(litzirueti_throughput), orient='columns')
litzirueti_throughput.rename(columns={"value":"litzirueti_flow"}, inplace=True)
litzirueti_prod = emsi_data.get('KW.ARO.MW.P_LITZIR:10')
litzirueti_prod = pd.read_json(json.dumps(litzirueti_prod), orient='columns')
litzirueti_prod.rename(columns={"value":"litzirueti_prod"}, inplace=True)
lueen_throughput = emsi_data.get('KW.ARO.MW.F_LUEEN:10')
lueen_throughput = pd.read_json(json.dumps(lueen_throughput), orient='columns')
lueen_throughput.rename(columns={"value":"lueen_flow"}, inplace=True)
lueen_m1_prod = emsi_data.get('KW.ARO.MW.P_LUEEN_M1:10')
lueen_m1_prod = pd.read_json(json.dumps(lueen_m1_prod), orient='columns')
lueen_m1_prod.rename(columns={"value":"lueen_m1_prod"}, inplace=True)
lueen_m2_prod = emsi_data.get('KW.ARO.MW.P_LUEEN_M2:10')
lueen_m2_prod = pd.read_json(json.dumps(lueen_m2_prod), orient='columns')
lueen_m2_prod.rename(columns={"value":"lueen_m2_prod"}, inplace=True)
lueen_m3_prod = emsi_data.get('KW.ARO.MW.P_LUEEN_M3:10')
lueen_m3_prod = pd.read_json(json.dumps(lueen_m3_prod), orient='columns')
lueen_m3_prod.rename(columns={"value":"lueen_m3_prod"}, inplace=True)

stauklappe_y = emsi_data.get('KW.ARO.MW.Y_STAUKL:10')
stauklappe_y = pd.read_json(json.dumps(stauklappe_y), orient='columns')
stauklappe_y.rename(columns={"value":"stauklappe"}, inplace=True)

isel_masl = emsi_data.get('KW.ARO.MW.L_ISEL:10')
isel_masl = pd.read_json(json.dumps(isel_masl), orient='columns')
isel_masl.rename(columns={"value":"isel_masl"}, inplace=True)

molinis_masl = emsi_data.get('KW.ARO.MW.L_MOLINIS:10')
molinis_masl = pd.read_json(json.dumps(molinis_masl), orient='columns')
molinis_masl.rename(columns={"value":"molinis_masl"}, inplace=True)

arosa_df = pd.merge(litzirueti_throughput, litzirueti_prod, on='time', how='inner')

arosa_df = pd.merge(arosa_df, lueen_throughput, on='time', how='inner')
arosa_df = pd.merge(arosa_df, lueen_m1_prod, on='time', how='inner')
arosa_df = pd.merge(arosa_df, lueen_m2_prod, on='time', how='inner')
arosa_df = pd.merge(arosa_df, lueen_m3_prod, on='time', how='inner')
arosa_df = pd.merge(arosa_df, stauklappe_y, on='time', how='inner')
arosa_df = pd.merge(arosa_df, isel_masl, on='time', how='inner')
arosa_df = pd.merge(arosa_df, molinis_masl, on='time', how='inner')


### 2. calculate water balance equation
for i in range(1, len(arosa_df)):
    lake_level_i = isel_masl_to_m3(arosa_df["isel_masl"].iloc[i], isel_table_masl, isel_table_m3)*10**6 # the volume in the isel table is in Mm3, thus the factor 10**6
    lake_level_i_min_1 = isel_masl_to_m3(arosa_df["isel_masl"].iloc[i - 1], isel_table_masl, isel_table_m3)*10**6
    throughput_15min = arosa_df['litzirueti_flow'].iloc[i-1] * 900 # 60 s/min * 15 min
    overflow = isel_overflow(arosa_df["isel_masl"].iloc[i-1], arosa_df["stauklappe"].iloc[i-1],Isel_Ueberlauf_Klappenstellung_df)
    arosa_df.loc[i - 1, "isel_inflow [m3/s]"] = (lake_level_i - lake_level_i_min_1 + throughput_15min)/900+overflow
    
for i in range(prod_shift, len(arosa_df)):
    overflow = molinis_overflow(arosa_df["molinis_masl"].iloc[i]-offset_masl, Molinis_Seestand_Uberlauf_df)
    arosa_df.loc[i, "molinis_inflow [m3/s]"] =  -arosa_df['litzirueti_flow'].iloc[i-prod_shift] + arosa_df['lueen_flow'].iloc[i]+overflow
arosa_df['time'] = pd.to_datetime(arosa_df['time'], utc=True)
arosa_df=arosa_df.rename(columns={'time': 'datetime'})
# Write recipe outputs
Arosa_Inflow_incl_Overflow = dataiku.Dataset("Arosa_Inflow_incl_Overflow")
Arosa_Inflow_incl_Overflow.write_with_schema(arosa_df)