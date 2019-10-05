-- Creates a table of events and where and when they happened as well as interesting facts such as deaths and injuries they caused.
CREATE TABLE storm_events_modeled.StormEvents AS
SELECT *
FROM 
(SELECT 
event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2013
UNION DISTINCT
SELECT event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2012
UNION DISTINCT
SELECT event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2011
UNION DISTINCT
SELECT event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
FROM storm_events_staging.StormEvents_2010)
ORDER BY event_id, event_type, year, month_name, begin_day, state_fips, cz_fips, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_crops
