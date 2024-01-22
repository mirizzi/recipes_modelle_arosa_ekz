 DECLARE @delete_from_utc AS datetime
DECLARE @delete_to_utc AS datetime

SELECT @delete_from_utc = MIN(datetime) FROM dataiku.arosa_isel_forecast_postprocessed
SELECT @delete_to_utc = MAX(datetime) FROM dataiku.arosa_isel_forecast_postprocessed

DELETE FROM dataiku.arosa_isel_forecast_hist WHERE datetime >= @delete_from_utc AND datetime <= @delete_to_utc

INSERT INTO dataiku.arosa_isel_forecast_hist
SELECT a.datetime, a.prediction
FROM dataiku.arosa_isel_forecast_postprocessed a