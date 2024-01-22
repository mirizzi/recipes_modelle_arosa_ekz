# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt, timedelta as td
from pytz import UTC

from utils_prep import ekz_waldhalde_preproc

# Read recipe inputs
EKZ_Waldhalde_Hist = dataiku.Dataset("EKZ_Waldhalde_Hist_preprocessing_2_lag")
df = EKZ_Waldhalde_Hist.get_dataframe()
# Sort the DataFrame by the 'datetime' column
df.sort_values(by='datetime', inplace=True)

# Get project variables
client = dataiku.api_client()
project = client.get_project(dataiku.get_custom_variables()['projectKey'])
project_variables = project.get_variables()

# get train start and end
start = dt.strptime(project_variables["standard"]["start_utc"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
start_prep = dt.strptime(project_variables["standard"]["ekz_load_start"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
end = dt.strptime(project_variables["standard"]["end_utc"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)

# Filter out all values which are not suitable for the data preprocessing
df = df[(df['datetime'] >= start_prep) & (df['datetime'] <= end)]

# Preprocess df
df = ekz_waldhalde_preproc(df)

# Only keep dates required for the forecasting
df = df[(df['datetime'] >= start) & (df['datetime'] <= end)]

# Delete the column "Production_MW", as you want to forecast that
df.drop(['Production_MW',"q3_Production"], axis=1, inplace=True)
# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
EKZ_Meteo_FC_preprocessing = dataiku.Dataset("EKZ_Meteo_FC_preprocessing")
EKZ_Meteo_FC_preprocessing.write_with_schema(df)