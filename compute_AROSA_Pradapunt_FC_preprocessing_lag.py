# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from features_create import *
# Read recipe inputs
AROSA_Pradapunt_FC_preprocessing = dataiku.Dataset("AROSA_Pradapunt_FC_preprocessing")
df = AROSA_Pradapunt_FC_preprocessing.get_dataframe()


# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df['pradapunt_inflow'][df['pradapunt_inflow']<.0000001]=0.0001

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
col_configs = [
    ('pradapunt_inflow', 24, 48),
    ('precip_mm', 24, 48),
    ('soil_moist_15cm',24,48)
]
df=roll_shift_m(df,col_configs)
df['pradapunt_inflow_log']=np.log(df['pradapunt_inflow'])
df['pradapunt_inflow_sqrt']=np.sqrt(df['pradapunt_inflow'])

# -----------------------

# Write recipe outputs
AROSA_Pradapunt_FC_preprocessing_lag = dataiku.Dataset("AROSA_Pradapunt_FC_preprocessing_lag")
AROSA_Pradapunt_FC_preprocessing_lag.write_with_schema(df)