--Events joined to see tornados happened in different counties within states ordered chronologically
SELECT
a.year, a.month_name, cz_name, location
FROM storm_events_modeled.StormEvents a
FULL OUTER JOIN storm_events_modeled.Locations b
ON a.cz_fips = b.cz_fips AND a.state_fips = b.state_fips
JOIN storm_events_modeled.Date c
ON a.year = c.year AND a.month_name = c.month_name AND a.begin_day = c.begin_day
WHERE event_type = 'Tornado'
ORDER BY date
