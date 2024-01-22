# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from datetime import datetime as dt, timedelta as td
import pytz
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import io

# CET from UTC
utc_tz = pytz.timezone('UTC')
cet_tz = pytz.timezone('Europe/Zurich')

# Set time tomorrow
glob_var = dataiku.get_custom_variables()
start_utc = dt.strptime(glob_var['start_utc'], '%Y-%m-%dT%H:%M:%S')
# Convert start_utc to the same timezone as Zeitstempel [CEST, von]
start_cet = start_utc.replace(tzinfo=utc_tz).astimezone(cet_tz)

# Read recipe inputs
EKZ_Waldhalde_Forecast = dataiku.Dataset("EKZ_Waldhalde_postprocessed")
df = EKZ_Waldhalde_Forecast.get_dataframe()

# Rename column names
df = df.rename(columns={"Zeitstempel [CET, von]":"Zeitstempel [CEST, von]", "precip_mm" :"Niederschlag [mm/h]"})

df_hist = df[df["Zeitstempel [CEST, von]"]<start_cet]
df_fcst = df[df["Zeitstempel [CEST, von]"]>=start_cet]

# Make plot
fig, ax1 = plt.subplots()

# Rotate x-axis tick labels for better readability (optional)
plt.xticks(rotation=45, ha='right')

# Plot Waldhalde [MW] on the left axis in blue
ax1.plot(df_hist["Zeitstempel [CEST, von]"], df_hist["Waldhalde Hist [MW]"], color="blue")
ax1.plot(df_fcst["Zeitstempel [CEST, von]"], df_fcst["Waldhalde Forecast [MW]"], color="red")
ax1.set_xlabel("Zeitstempel")
ax1.set_ylabel("Waldhalde [MW]", color="blue")
ax1.tick_params(axis="y", colors="blue")

# Format x-axis ticks to show only every 7th day and rotate labels
ax1.xaxis.set_major_locator(mdates.DayLocator(interval=3))
ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

# Create a twin axis for Niederschlag [mm/h] on the right side
ax2 = ax1.twinx()
# Plot Niederschlag [mm/h] on the right axis in yellow
ax2.plot(df["Zeitstempel [CEST, von]"], df["Niederschlag [mm/h]"], color= (0.8, 0.6, 0))
ax2.set_ylabel("Niederschlag [mm/h]", color= (0.8, 0.6, 0))
ax2.tick_params(axis="y", colors= (0.8, 0.6, 0))

# Format x-axis ticks to show only every 7th day and rotate labels
ax2.xaxis.set_major_locator(mdates.DayLocator(interval=7))
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))

# Set the scaling of left and right y-axes
ylim_precip = df["Niederschlag [mm/h]"].max()+1
ax1.set_ylim(0, 3.1)
ax2.set_ylim(0, ylim_precip)

ax = plt.gca()
ax.grid(which='major', axis='both', linestyle='--')
plt.title("Zuflussprognose Waldhalde", fontweight="bold", fontsize=11)

# Delete columns by name
columns_to_delete = ['Waldhalde Hist [MW]', 'temp_2m', 'snow_depth_cm', 'snow_melt_mm', 'soil_moist_15cm']
df_fcst = df_fcst.drop(columns=columns_to_delete)

# Write dataframe outputs
EKZ_Waldhalde_Zuflussprognose = dataiku.Dataset("EKZ_Waldhalde_Zuflussprognose")
EKZ_Waldhalde_Zuflussprognose.write_with_schema(df_fcst)

# Save plot in folder
EKZ_Waldhalde_Plots = dataiku.Folder("zs97Zu0X")
EKZ_Waldhalde_Plots_info = EKZ_Waldhalde_Plots.get_info()

bs = io.BytesIO()
filename = 'Waldhalde_Zuflussprognose.png'
plt.savefig(bs, format="png", bbox_inches='tight')
EKZ_Waldhalde_Plots.upload_stream(filename, bs.getvalue())