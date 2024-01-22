# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from utils_prep import arosa_pradapunt_preproc

# Read recipe inputs
AROSA_PRADAPUNT_Hist = dataiku.Dataset("AROSA_PRADAPUNT_Hist")
df = AROSA_PRADAPUNT_Hist.get_dataframe()
# Sort the DataFrame by the 'datetime' column
df.sort_values(by='datetime', inplace=True)

# Filter out all values which are not suitable for the training data
df = df[df['pradapunt_inflow'].notnull()]

# Preprocess df
df = arosa_pradapunt_preproc(df)


# Write recipe outputs
AROSA_Pradapunt_train_preprocessing = dataiku.Dataset("AROSA_Pradapunt_train_preprocessing")
AROSA_Pradapunt_train_preprocessing.write_with_schema(df)
