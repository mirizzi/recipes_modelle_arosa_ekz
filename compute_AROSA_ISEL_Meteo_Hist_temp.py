# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
### 0) Manipulation on Production Data
Prod_EMSI = dataiku.Dataset("Arosa_Inflow_incl_Overflow")
prod_newest_df = Prod_EMSI.get_dataframe()
prod_newest_df=prod_newest_df[["datetime","isel_inflow [m3/s]"]]
# Set 'datetime' column as the index
prod_newest_df.set_index('datetime', inplace=True)
# Resample the data to hourly intervals and calculate the mean for each hour
prod_newest_df = prod_newest_df.resample('1H').mean()
# Reset the index to make 'datetime' a column again if needed
prod_newest_df.reset_index(inplace=True)
# Round values to 3 Nachkommastellen
prod_newest_df.rename(columns={"isel_inflow [m3/s]": "isel_inflow"}, inplace=True)
prod_newest_df=prod_newest_df.round(3)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# 1) Read historical inflows of isel
Isel_Inflows_Hist = dataiku.Dataset("Arosa_Inflows_Hist")
isel_df = Isel_Inflows_Hist.get_dataframe()
# Select necessary columns
isel_df = isel_df[['datetime', 'Isel Inflow (m3/s)']]
# rename column
isel_df.rename(columns={"Isel Inflow (m3/s)": "isel_inflow"}, inplace=True)

# Convert 'datetime' column to datetime format and set it as the index
isel_df['datetime'] = pd.to_datetime(isel_df['datetime'], format='%d.%m.%Y %H:%M')
isel_df.set_index('datetime', inplace=True)

# Resample the data to hourly intervals and calculate the mean for each hour. Then round value to 3 digits.
isel_df = isel_df.resample('1H').mean().round(3)

# Reset the index to make 'datetime' a column again if needed
isel_df.reset_index(inplace=True)
isel_df['datetime'] = isel_df['datetime'].dt.tz_localize('utc')

# Sort the DataFrame by the 'datetime' column
isel_df.sort_values(by='datetime', inplace=True)

# Crop oldest data if current and historic data is overlapping
min_date = min(prod_newest_df['datetime'])
isel_df = isel_df[isel_df['datetime'] < min_date]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Stack newest data on top of oldest data
isel_df = pd.concat([isel_df, prod_newest_df], axis=0)
isel_df.reset_index(drop=True, inplace=True)

# Sort the DataFrame by the 'datetime' column
isel_df.sort_values(by='datetime', inplace=True)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# 2) Read historical inflows of isel
AROSA_Isel_Meteo_Forecast = dataiku.Dataset("AROSA_Isel_Meteo_Forecast")
meteo_df = AROSA_Isel_Meteo_Forecast.get_dataframe()
meteo_df = meteo_df.rename(columns={'validdate':'datetime', 't_2m:C': 'temp_2m', 'precip_1h:mm': 'precip_mm', 'snow_depth:cm':'snow_depth_cm','snow_melt_1h:mm':'snow_melt_mm', 'soil_moisture_index_-15cm:idx':'soil_moist_15cm'})
meteo_df['datetime'] = pd.to_datetime(meteo_df['datetime'])


### 3) Merge Meteo Data with Production Data
df = pd.merge(meteo_df, isel_df, on='datetime', how='left')

# Write recipe outputs
AROSA_ISEL_Meteo_Hist_temp = dataiku.Dataset("AROSA_ISEL_Hist_temp")
AROSA_ISEL_Meteo_Hist_temp.write_with_schema(df)