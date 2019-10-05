--Events joined to see tornados happened in different counties within states
SELECT
year, month_name, cz_name, location
FROM storm_events_modeled.StormEvents a
FULL OUTER JOIN storm_events_modeled.Locations b
ON a.cz_fips = b.cz_fips AND a.state_fips = b.state_fips
WHERE event_type = 'Tornado'
ORDER BY year, month_name
