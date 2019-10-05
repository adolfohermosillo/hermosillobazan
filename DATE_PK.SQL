CREATE TABLE storm_events_modeled.Date AS
SELECT *
FROM 
(SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2013
UNION DISTINCT
SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2012
UNION DISTINCT
SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2011
UNION DISTINCT
SELECT distinct (safe_cast ( begin_date_time = DATE)) as date, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2010)
ORDER BY date
