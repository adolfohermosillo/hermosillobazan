CREATE TABLE storm_events_modeled.Date AS
SELECT *
FROM 
(SELECT begin_date_time, begin_yearmonth, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2013
UNION DISTINCT
SELECT begin_date_time, begin_yearmonth, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2012
UNION DISTINCT
SELECT begin_date_time, begin_yearmonth, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2011
UNION DISTINCT
SELECT begin_date_time, begin_yearmonth, year, month_name, begin_day
FROM storm_events_staging.StormEvents_2010)
ORDER BY begin_date_time
