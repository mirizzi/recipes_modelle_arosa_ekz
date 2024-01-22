# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt
import csv
from ftplib import FTP
from utils import ftp_prep, write_to_ftp

# Read recipe inputs
AROSA_Isel_Forecast_postprocessed = dataiku.Dataset("AROSA_Isel_Forecast_postprocessed")
Isel_df = AROSA_Isel_Forecast_postprocessed.get_dataframe()
AROSA_Pradapunt_Forecast_postprocessed = dataiku.Dataset("AROSA_Pradapunt_Forecast_postprocessed")
Pradapunt_df = AROSA_Pradapunt_Forecast_postprocessed.get_dataframe()

#************************** PREPARE DFs TO FTP EXPORT FORMAT **************************#
Isel_df = ftp_prep(Isel_df)
Isel_df['prediction'] = Isel_df['prediction'].multiply(3600)
Isel_df = Isel_df.rename(columns={'prediction': 'Inflow (m3perh)'})

Pradapunt_df = ftp_prep(Pradapunt_df)
Pradapunt_df['prediction'] = Pradapunt_df['prediction'].multiply(3600)
Pradapunt_df = Pradapunt_df.rename(columns={'prediction': 'Inflow (m3perh)'})

#************************** EXPORT TO FTP SERVER **************************#
# Define remote directory path on the FTP server
directory = '/TSE_optimization/inputs/inflows/'

# Define the list of dictionaries for file exports
exports = [
    {
        'filename': f"ARO_Isel_Inflow_Shortterm.csv",
        'data': Isel_df
    },
    {
        'filename': f"ARO_Pradapunt_Inflow_Shortterm.csv",
        'data': Pradapunt_df
    }
]

# Iterate over the list of exports and perform the file exports
for export in exports:
    filename = export['filename']
    data = export['data']
    write_to_ftp(directory, filename, data, key_acc="CKW_FTP_WRITE_ACC", key_pass="CKW_FTP_WRITE_PASS")


#************************** Write recipe outputs **************************#
AROSA_Isel_Inflow_Export = dataiku.Dataset("AROSA_Isel_Inflow_Export")
AROSA_Isel_Inflow_Export.write_with_schema(Isel_df)
AROSA_Pradapunt_Inflow_Export = dataiku.Dataset("AROSA_Pradapunt_Inflow_Export")
AROSA_Pradapunt_Inflow_Export.write_with_schema(Pradapunt_df)