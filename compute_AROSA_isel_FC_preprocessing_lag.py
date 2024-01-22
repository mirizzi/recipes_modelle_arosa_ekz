# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from features_create import *
# Read recipe inputs
# Dataset AROSA_Pradapunt_FC_preprocessing renamed to AROSA_Isel_FC_preprocessing by michele.rizzi@axpo.com on 2024-01-10 13:46:58
AROSA_Isel_FC_preprocessing = dataiku.Dataset("AROSA_Isel_FC_preprocessing")
df = AROSA_Isel_FC_preprocessing.get_dataframe()


# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
col_configs = [
    ('isel_inflow', 24, 48),
    ('precip_mm', 24, 48),
    ('soil_moist_15cm',24,48)
]
df=roll_shift_m(df,col_configs)


# -----------------------

# Write recipe outputs
# Dataset AROSA_Pradapunt_FC_preprocessing_lag renamed to AROSA_isel_FC_preprocessing_lag by michele.rizzi@axpo.com on 2024-01-10 13:46:58
AROSA_Isel_FC_preprocessing_lag = dataiku.Dataset("AROSA_isel_FC_preprocessing_lag")
AROSA_Isel_FC_preprocessing_lag.write_with_schema(df)