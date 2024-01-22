# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
Q3 = dataiku.Dataset("Q3")
df = Q3.get_dataframe()


# Compute recipe outputs from inputs
# TODO: Replace this part by your actual code that computes the output, as a Pandas dataframe
# NB: DSS also supports other kinds of APIs for reading and writing data. Please see doc.
midpoints = []
for category in df['q3_Production'].unique():
    category_data = data[quantile_categories == category]
    midpoint = (max(category_data) + min(category_data)) / 2
    midpoints.append(midpoint)
    
for i in range(len(df)):
    category = df['q3_Production'].iloc[i]
    category_data = data[quantile_categories == category]
    
    # Calculate the midpoint of the quantile's range
    midpoint = (max(category_data) + min(category_data)) / 2
Q3_decateg_df = Q3_df # For this sample code, simply copy input to output


# Write recipe outputs
Q3_decateg = dataiku.Dataset("Q3_decateg")
Q3_decateg.write_with_schema(Q3_decateg_df)
