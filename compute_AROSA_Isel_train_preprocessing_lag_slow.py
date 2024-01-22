# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from features_create import *

# Read recipe inputs
AROSA_Isel_train_preprocessing = dataiku.Dataset("AROSA_Isel_train_preprocessing")
df = AROSA_Isel_train_preprocessing.get_dataframe()

df['isel_inflow'][df['isel_inflow']<.0000001]=0.0001

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
col_configs = [
    ('isel_inflow', 72, 48), #(create rolling mean(24) and shift(48))
    ('precip_mm', 72, 48),
    ('soil_moist_15cm',72,48)
]
df=roll_shift_m(df,col_configs)
df['isel_inflow_log']=np.log(df['isel_inflow'])
df['isel_inflow_sqrt']=np.sqrt(df['isel_inflow'])

# Write recipe outputs
AROSA_Isel_train_preprocessing_lag_slow = dataiku.Dataset("AROSA_Isel_train_preprocessing_lag_slow")
AROSA_Isel_train_preprocessing_lag_slow.write_with_schema(df)
