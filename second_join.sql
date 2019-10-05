--StormEvents joined to State table to display all avalanches that caused at least one death
SELECT
state, year, month_name, begin_day, deaths_direct
FROM storm_events_modeled.StormEvents a
JOIN storm_events_modeled.State b
ON a.state_fips = b.state_fips 
WHERE event_type = 'Avalanche' AND deaths_direct >= 1
ORDER BY year, month_name, begin_day, state
