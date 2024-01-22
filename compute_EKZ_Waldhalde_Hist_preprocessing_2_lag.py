# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from features_create import *
# Read recipe inputs
EKZ_Waldhalde_Hist_preprocessing_2 = dataiku.Dataset("EKZ_Waldhalde_Hist_preprocessing_2")
df = EKZ_Waldhalde_Hist_preprocessing_2.get_dataframe()
df.index= pd.to_datetime(df["datetime"])
df= df[df.index.year!=2023]
#df['Production_MW'][df['Production_MW']<.0000001]=0.0001
df['q3_Production'] = pd.qcut(df['Production_MW'], q=3, labels=[1,2,3])

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
col_configs = [
    ('q3_Production', 24,48),
    ('Production_MW', 24, 48), #(create rolling mean(24) and shift(48))
    ('precip_mm', 24, 48),
    ('soil_moist_15cm',24,48)
]
df=roll_shift_m(df,col_configs)

#df['Production_MW_log']=np.log(df['Production_MW'])
#df['Production_MW_sqrt']=np.sqrt(df['Production_MW'])
#df['q2_Production'] = pd.qcut(df['Production_MW'], q=2, labels=[1,2])

# Write recipe outputs
EKZ_Waldhalde_Hist_preprocessing_2_lag = dataiku.Dataset("EKZ_Waldhalde_Hist_preprocessing_2_lag")
EKZ_Waldhalde_Hist_preprocessing_2_lag.write_with_schema(df)