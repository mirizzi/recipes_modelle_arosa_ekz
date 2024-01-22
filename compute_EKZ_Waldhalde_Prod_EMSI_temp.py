# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from ftplib import FTP
import json
from utils import ftp_prep, write_to_ftp
from datetime import datetime as dt, timedelta as td
import pytz
from pytz import timezone as tz

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

EKZ_Waldhalde = emsi_data.get('KW.EKZ_WAL.SRL.PIST:10')

# Load the data into a DataFrame using the appropriate orientation
EKZ_Waldhalde_df = pd.read_json(json.dumps(EKZ_Waldhalde), orient='columns')

# convert "time"-column to  datetime format
EKZ_Waldhalde_df['time'] = pd.to_datetime(EKZ_Waldhalde_df['time'], utc=True)

# Round the values in the value column to 4 decimal places
EKZ_Waldhalde_df['value'] = EKZ_Waldhalde_df['value'].round(3)

# Keep only dataframe before(!) the forecasting timerange
glob_var = dataiku.get_custom_variables()
start_utc = pytz.utc.localize(dt.strptime(glob_var['start_utc'], '%Y-%m-%dT%H:%M:%S'))
# Only keep dates required for the forecasting
EKZ_Waldhalde_df = EKZ_Waldhalde_df[EKZ_Waldhalde_df['time'] < start_utc]

# Rename columns
EKZ_Waldhalde_df = EKZ_Waldhalde_df.rename(columns={'time': 'datetime', 'value': 'Production_MW'})

# Write recipe outputs
EKZ_Waldhalde_Prod_EMSI = dataiku.Dataset("EKZ_Waldhalde_Prod_EMSI")
EKZ_Waldhalde_Prod_EMSI.write_with_schema(EKZ_Waldhalde_df)