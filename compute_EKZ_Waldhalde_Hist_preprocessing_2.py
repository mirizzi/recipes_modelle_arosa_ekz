# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt

from utils_prep import *


# Read recipe inputs
EKZ_Waldhalde_Hist = dataiku.Dataset("EKZ_Waldhalde_Hist")
df = EKZ_Waldhalde_Hist.get_dataframe()
# Sort the DataFrame by the 'datetime' column
df.sort_values(by='datetime', inplace=True)

# Filter out all values which are not suitable for the training data
df["Production_MW"] = low_cap(df["Production_MW"],1.1)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE


# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
EKZ_Waldhalde_Hist_preprocessing_2 = dataiku.Dataset("EKZ_Waldhalde_Hist_preprocessing_2")
EKZ_Waldhalde_Hist_preprocessing_2.write_with_schema(df)