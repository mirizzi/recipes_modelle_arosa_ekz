# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

### 1) Manipulation on Production Data
EKZ_Waldhalde_Prod_EMSI = dataiku.Dataset("EKZ_Waldhalde_Prod_EMSI")
prod_newest_df = EKZ_Waldhalde_Prod_EMSI.get_dataframe()
# Set 'datetime' column as the index
prod_newest_df.set_index('datetime', inplace=True)
# Resample the data to hourly intervals and calculate the mean for each hour
prod_newest_df = prod_newest_df.resample('1H').mean()
# Reset the index to make 'datetime' a column again if needed
prod_newest_df.reset_index(inplace=True)
# Round values to 3 Nachkommastellen
prod_newest_df['Production_MW'] = prod_newest_df['Production_MW'].round(3)

EKZ_Prod_Hist_0 = dataiku.Dataset("EKZ_Prod_Hist_0_prepared")
prod_hist_df = EKZ_Prod_Hist_0.get_dataframe()
# Set 'datetime' column as the index
prod_hist_df.set_index('datetime', inplace=True)
# Resample the data to hourly intervals and calculate the mean for each hour
prod_hist_df = prod_hist_df.resample('1H').mean()
# Reset the index to make 'datetime' a column again if needed
prod_hist_df.reset_index(inplace=True)
# Round values to 3 Nachkommastellen
prod_hist_df['Production_MW'] = prod_hist_df['Production_MW'].round(3)

# Crop oldest data if current and historic data is overlapping
min_date = min(prod_newest_df['datetime'])
prod_hist_df = prod_hist_df[prod_hist_df['datetime'] < min_date]

# Stack newest data on top of oldest data
prod_df = pd.concat([prod_hist_df, prod_newest_df], axis=0)
prod_df.reset_index(drop=True, inplace=True)

# Sort the DataFrame by the 'datetime' column
prod_df.sort_values(by='datetime', inplace=True)

### 2) Manipulation on Meteo Data
# Read recipe inputs
EKZ_Waldhalde_Meteo_Hist = dataiku.Dataset("EKZ_Waldhalde_Meteo_Hist_temp0")
meteo_df = EKZ_Waldhalde_Meteo_Hist.get_dataframe()
meteo_df = meteo_df.rename(columns={'validdate':'datetime', 't_2m:C': 'temp_2m', 'precip_1h:mm': 'precip_mm', 'snow_depth:cm':'snow_depth_cm','snow_melt_1h:mm':'snow_melt_mm', 'soil_moisture_index_-15cm:idx':'soil_moist_15cm'})
meteo_df['datetime'] = pd.to_datetime(meteo_df['datetime'])

### 3) Merge Meteo Data with Production Data
df = pd.merge(meteo_df, prod_df, on='datetime', how='left')

# Write recipe outputs
# Dataset EKZ_Meteo_Hist_temp renamed to EKZ_Waldhalde_Hist_temp by dario.panzuto@axpo.com on 2023-12-05 14:04:48
EKZ_Meteo_Hist_temp = dataiku.Dataset("EKZ_Waldhalde_Hist_temp")
EKZ_Meteo_Hist_temp.write_with_schema(df)