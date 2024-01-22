DECLARE @delete_from_utc AS datetime
DECLARE @delete_to_utc AS datetime

SELECT @delete_from_utc = MIN(datetime) FROM dataiku.arosa_isel_hist_temp
SELECT @delete_to_utc = MAX(datetime) FROM dataiku.arosa_isel_hist_temp

DELETE FROM dataiku.arosa_isel_hist WHERE datetime >= @delete_from_utc AND datetime <= @delete_to_utc

INSERT INTO dataiku.arosa_isel_hist
SELECT *
FROM dataiku.arosa_isel_hist_temp a

TRUNCATE TABLE dataiku.arosa_isel_hist_temp