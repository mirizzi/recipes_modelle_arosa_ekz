# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt, timedelta as td
import pytz
from pytz import timezone


# Define the timezones for UTC and Europe/Zurich
utc = timezone('UTC')
cet = timezone('Europe/Zurich')

# Read historic dataset
EKZ_Waldhalde_Hist = dataiku.Dataset("EKZ_Waldhalde_Hist")
hist_df = EKZ_Waldhalde_Hist.get_dataframe()

# Convert UTC to Europe/Zurich
hist_df['datetime'] = pd.to_datetime(hist_df['datetime']).dt.tz_convert(cet)

# Calculate `end_cet` as yesterday midnight in CET
# Keep only dataframe before(!) the forecasting timerange
glob_var = dataiku.get_custom_variables()
end_cet = pytz.utc.localize(dt.strptime(glob_var['start_utc'], '%Y-%m-%dT%H:%M:%S')).replace(tzinfo=None).astimezone(cet)
# Calculate `start_cet` as 45 days before `end_cet`
start_cet = end_cet - td(days=45)
# Only keep dates required for the forecasting
hist_df = hist_df[(hist_df['datetime'] >= start_cet) & (hist_df['datetime'] < end_cet)]

# Read forecasted dataset
EKZ_Prod_FC = dataiku.Dataset("EKZ_Prod_FC")
fc_df = EKZ_Prod_FC.get_dataframe()

# Convert UTC to Europe/Zurich
fc_df['datetime'] = pd.to_datetime(fc_df['datetime']).dt.tz_convert(cet)

# Concatenate them vertically
stacked_df = hist_df.append(fc_df, ignore_index=True)

# sort dataset
stacked_df.sort_values(by='datetime', inplace=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
stacked_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#filtering out the extra features
# Identify common columns
common_columns = list(hist_df.columns.intersection(fc_df.columns)) + ["prediction"]+["Production_MW"]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Select only the common columns
stacked_df = stacked_df[common_columns]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
stacked_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
EKZ_Waldhalde_Hist_FC_combined = dataiku.Dataset("EKZ_Waldhalde_Hist_FC_combined")
EKZ_Waldhalde_Hist_FC_combined.write_with_schema(stacked_df)