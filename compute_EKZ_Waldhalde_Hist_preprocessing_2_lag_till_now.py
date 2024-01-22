# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
#%config Completer.use_jedi = False 
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
EKZ_Waldhalde_Hist_preprocessing_2_lag = dataiku.Dataset("EKZ_Waldhalde_Hist_preprocessing_2_lag")
df = EKZ_Waldhalde_Hist_preprocessing_2_lag.get_dataframe()
df.index= pd.to_datetime(df['datetime'])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
pd.Timestamp.now(tz='UCT')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df=df[df.index < pd.Timestamp.now(tz='UCT')]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df




# Write recipe outputs
EKZ_Waldhalde_Hist_preprocessing_2_lag_till_now = dataiku.Dataset("EKZ_Waldhalde_Hist_preprocessing_2_lag_till_now")
EKZ_Waldhalde_Hist_preprocessing_2_lag_till_now.write_with_schema(df)