# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE_MAGIC_CELL
# Automatically replaced inline charts by "no-op" charts
# %pylab inline
import matplotlib
matplotlib.use("Agg")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import dataiku
from dataiku import pandasutils as pdu
import pandas as pd

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Example: load a DSS dataset as a Pandas dataframe
mydataset = dataiku.Dataset("AROSA_Isel_train_preprocessing")
df = mydataset.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Recipe outputs
isel_norm = dataiku.Dataset("isel_norm")
isel_norm.write_with_schema(pandas_dataframe)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import pandas as pd
from statsmodels.gam.api import GLMGam, BSplines
from statsmodels.genmod.families import Gaussian

# Select the 'isel_inflow' column and use DataFrame index as the predictor
df['index'] = df.index

# Define the spline basis
# Customize the knots, degree, and df (degrees of freedom) as needed
x_spline = BSplines(df[['index']], df=[12], degree=[3], include_intercept=True)

# Define and fit the model
gam = GLMGam.from_formula('isel_inflow ~ 1', data=df, smoother=x_spline, family=Gaussian())
gam_result = gam.fit()

# Output the results
print(gam_result.summary())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
import pandas as pd
import numpy as np
from statsmodels.gam.api import GLMGam, BSplines
from statsmodels.genmod.families import Gaussian
import matplotlib.pyplot as plt

# Load your data
df = pd.read_csv('your_data.csv')  # replace with your data source

# Convert 'datetime' to datetime object and extract week of the year
df['datetime'] = pd.to_datetime(df['datetime'])
df['week_of_year'] = df['datetime'].dt.isocalendar().week

# Calculate weekly averages
weekly_avg = df.groupby('week_of_year')['isel_inflow'].mean().reset_index()

# Fit GAM model - using week of the year as the predictor
x_spline = BSplines(weekly_avg[['week_of_year']], df=[53], degree=[3], include_intercept=True)
gam = GLMGam.from_formula('isel_inflow ~ 1', data=weekly_avg, smoother=x_spline, family=Gaussian())
gam_result = gam.fit()

# Predict using the model for plotting
week_range = np.arange(1, 54)  # weeks 1 to 53
predicted = gam_result.predict({'week_of_year': week_range})

# Plotting
plt.figure(figsize=(12, 6))
plt.plot(week_range, predicted, label='GAM Model')
plt.scatter(weekly_avg['week_of_year'], weekly_avg['isel_inflow'], color='red', label='Weekly Averages')
plt.xlabel('Week of the Year')
plt.ylabel('Average Inflow')
plt.title('Weekly Average Inflow and GAM Model Prediction')
plt.legend()
plt.show()