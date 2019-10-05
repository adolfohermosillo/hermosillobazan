--Creates a table with locations as primary keys and can be mapped to county name given state fips and county fips.

CREATE TABLE storm_events_modeled.Locations AS
SELECT *
FROM 
(SELECT  distinct( begin_lat_begin_lon_appended)  as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2013
WHERE begin_lat_begin_lon_appended is not null
UNION DISTINCT
SELECT distinct( begin_lat_begin_lon_appended)  as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2012
WHERE begin_lat_begin_lon_appended is not null
UNION DISTINCT
SELECT distinct( begin_lat_begin_lon_appended)  as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2011
WHERE begin_lat_begin_lon_appended is not null
UNION DISTINCT
SELECT distinct( begin_lat_begin_lon_appended) as location,  cz_name, state_fips, cz_fips
FROM storm_events_staging.StormEvents_2010 
WHERE begin_lat_begin_lon_appended is not null)
ORDER BY location,cz_name, state_fips, cz_fips
