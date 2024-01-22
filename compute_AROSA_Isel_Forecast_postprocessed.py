import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
AROSA_Isel_Forecast = dataiku.Dataset("AROSA_Isel_Forecast")
Isel_df = AROSA_Isel_Forecast.get_dataframe()
AROSA_Pradapunt_Forecast = dataiku.Dataset("AROSA_Pradapunt_Forecast")
Pradapunt_df = AROSA_Pradapunt_Forecast.get_dataframe()

def clean_df(df:pd.DataFrame):
    # Delete columns by name
    columns_to_delete = ['Seasonality', 't_2m:C', 'precip_1h:mm', 'snow_depth:cm','soil_moisture_index_-15cm:idx','global_rad:W', 'Label']
    df = df.drop(columns=columns_to_delete)

    return df

def map_dotation(df:pd.DataFrame):
    df['dotation'] = df['datetime'].dt.month.map({
        1: 0.06,  # January
        2: 0.06,  # February
        3: 0.06,  # March
        4: 0.06,  # April
        5: 0.1,   # May
        6: 0.1,   # June
        7: 0.1,   # July
        8: 0.08,  # August
        9: 0.08,  # September
       10: 0.06,  # October
       11: 0.06,  # November
       12: 0.06   # December
    })
    
    return df
#************************** Postprocessing Arosa Isel **************************#
Isel_df = clean_df(Isel_df)
# Dotierung Isel
Isel_df = map_dotation(Isel_df)
Isel_df['prediction'] = Isel_df['prediction'] - Isel_df['dotation']
Isel_df = Isel_df.drop(columns={'dotation'})

#************************** Postprocessing Arosa Pradapunt **************************#
Pradapunt_df = clean_df(Pradapunt_df)
# Dotierung Pradapunt
Pradapunt_df = map_dotation(Pradapunt_df)
Pradapunt_df['prediction'] = Pradapunt_df['prediction'] + Pradapunt_df['dotation']
Pradapunt_df = Pradapunt_df.drop(columns={'dotation'})


# Write recipe outputs
AROSA_Isel_Forecast_postprocessed = dataiku.Dataset("AROSA_Isel_Forecast_postprocessed")
AROSA_Isel_Forecast_postprocessed.write_with_schema(Isel_df)
AROSA_Pradapunt_Forecast_postprocessed = dataiku.Dataset("AROSA_Pradapunt_Forecast_postprocessed")
AROSA_Pradapunt_Forecast_postprocessed.write_with_schema(Pradapunt_df)