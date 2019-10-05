--Creates a table with all unique state FIPS code along with the name of the state or territory.
--The state_fips being the primary key
CREATE TABLE storm_events_modeled.State AS
SELECT *
FROM 
(SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2013
WHERE state_fips IS NOT null OR state IS NOT null
UNION DISTINCT
SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2012
WHERE state_fips IS NOT null OR state IS NOT null
UNION DISTINCT
SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2011
WHERE state_fips IS NOT null OR state IS NOT null
UNION DISTINCT
SELECT DISTINCT state_fips, state
FROM storm_events_staging.StormEvents_2010
WHERE state_fips IS NOT null OR state IS NOT null)
ORDER BY state_fips
