# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
# #%config Completer.use_jedi = False
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
EKZ_Waldhalde_Hist_preprocessing_2_lag = dataiku.Dataset("EKZ_Waldhalde_Hist_preprocessing_2_lag")
df = EKZ_Waldhalde_Hist_preprocessing_2_lag.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df.index= pd.to_datetime(df['datetime'])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df=df[:"2023"]# only train till 2022, todo clean 2023 prod data

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE

# Write recipe outputs
EKZ_Meteo_train_preprocessing = dataiku.Dataset("EKZ_Meteo_train_preprocessing")
EKZ_Meteo_train_preprocessing.write_with_schema(df)