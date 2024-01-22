# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt, timedelta as td
from pytz import UTC

from utils_prep import arosa_pradapunt_preproc

# Read recipe inputs
AROSA_PRADAPUNT_Hist = dataiku.Dataset("AROSA_PRADAPUNT_Hist")
df = AROSA_PRADAPUNT_Hist.get_dataframe()
# Sort the DataFrame by the 'datetime' column
df.sort_values(by='datetime', inplace=True)

# Get project variables
client = dataiku.api_client()
project = client.get_project(dataiku.get_custom_variables()['projectKey'])
project_variables = project.get_variables()

# get train start and end
start = dt.strptime(project_variables["standard"]["start_utc"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
start_prep = dt.strptime(project_variables["standard"]["arosa_pradapunt_load_start"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)
end = dt.strptime(project_variables["standard"]["end_utc"], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=UTC)

# Filter out all values which are not suitable for the training data
df = df[(df['datetime'] >= start_prep) & (df['datetime'] <= end)]

# Preprocess df
df = arosa_pradapunt_preproc(df)

# Write recipe outputs
AROSA_Pradapunt_FC_preprocessing = dataiku.Dataset("AROSA_Pradapunt_FC_preprocessing")
AROSA_Pradapunt_FC_preprocessing.write_with_schema(df)
