# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt

from utils_prep import arosa_isel_preproc

# Read recipe inputs
AROSA_ISEL_Hist = dataiku.Dataset("AROSA_ISEL_Hist")
df = AROSA_ISEL_Hist.get_dataframe()
# Sort the DataFrame by the 'datetime' column
df.sort_values(by='datetime', inplace=True)

# Filter out all values which are not suitable for the training data
df = df[df['isel_inflow'].notnull()]

# Preprocess df
df = arosa_isel_preproc(df)

# Write recipe outputs
AROSA_Isel_train_preprocessing = dataiku.Dataset("AROSA_Isel_train_preprocessing")
AROSA_Isel_train_preprocessing.write_with_schema(df)
