# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt, timedelta as td
from  pytz import timezone

# CET from UTC
utc_tz = timezone('UTC')
cet_tz = timezone('Europe/Zurich')

# Set time tomorrow
glob_var = dataiku.get_custom_variables()
start_cet = dt.strptime(glob_var['start_cet'], '%Y-%m-%dT%H:%M:%S')
start_cet = start_cet.astimezone(cet_tz)

# Read recipe inputs
EKZ_Waldhalde_FC_sorted = dataiku.Dataset("EKZ_Waldhalde_FC_sorted")
df_cet = EKZ_Waldhalde_FC_sorted.get_dataframe()

df_cet = df_cet.rename(columns={'datetime_cet':'Zeitstempel [CET, von]'})
df_cet['Zeitstempel [CET, von]'] = df_cet['Zeitstempel [CET, von]'].apply(lambda x: x.tz_localize(utc_tz).astimezone(cet_tz))


hist_df = df_cet[df_cet["Zeitstempel [CET, von]"]<start_cet]
forecast_df = df_cet[df_cet["Zeitstempel [CET, von]"]>=start_cet]

# Group the historical inflow data by day and calculate the average of 'Waldhalde Hist [MW]'
Hist_avg_df = hist_df.groupby(hist_df['Zeitstempel [CET, von]'].dt.date)['Waldhalde Hist [MW]'].mean()
# extract yesterday's averaged inflow
yesterday = (dt.today().date() + td(days=-1))
avg_yesterday = Hist_avg_df.loc[yesterday]

# Calculate yesterday's standard deviation.
Hist_std_dev = hist_df.groupby(hist_df['Zeitstempel [CET, von]'].dt.date)['Waldhalde Hist [MW]'].std()
std_dev_yesterday = Hist_std_dev[yesterday]

if std_dev_yesterday <= 0.05:
    # Group the forecasted inflow data by day and calculate the average of 'Waldhalde Forecast [MW]'
    Forecast_avg_df = forecast_df.groupby(forecast_df['Zeitstempel [CET, von]'].dt.date)['Waldhalde Forecast [MW]'].mean()

    # This for-loop calculates the average forecasted inflow and its scaling factor with regards to yesterdays inflow average
    for i in range(1):#len(Forecast_avg_df)):
        # extract future averaged inflow, day by day
        date_fcst = (dt.today().date() + td(days=+i+1))
        avg_date_fcst = Forecast_avg_df.loc[date_fcst]
        ## Calculates the scaling factor for the first forecasted day.
        # This scaling factor is linearly interpolated for 24 h to the value 1
        scaling_fct = avg_yesterday / avg_date_fcst
        ## Now, interpolate the first forecast day from scaling_fct to 1
        # Calculate the number of rows to interpolate
        num_rows = len(df_cet[df_cet['Zeitstempel [CET, von]'].dt.date == date_fcst])
        # Create an array of linearly interpolated values from scaling_fct to 1
        interpolated_values = np.linspace(scaling_fct, 1, num_rows)
        # Write the scaling factor in a new column
        df_cet.loc[df_cet['Zeitstempel [CET, von]'].dt.date == date_fcst, 'scaling_fct'] = interpolated_values
        # Set all NaN values in the 'scaling_fct' column to 1
        df_cet['scaling_fct'].fillna(1, inplace=True)


    # Multiply the Forecast column with the scaling column
    df_cet['Waldhalde Forecast [MW]'] = df_cet['Waldhalde Forecast [MW]'] * df_cet['scaling_fct']

    #drop the scaling column
    df_cet = df_cet.drop(columns={'scaling_fct'})

# Finally, round all values
df_cet = df_cet.round(3)
    
# Write recipe outputs
EKZ_Waldhalde_postprocessed = dataiku.Dataset("EKZ_Waldhalde_postprocessed")
EKZ_Waldhalde_postprocessed.write_with_schema(df_cet)