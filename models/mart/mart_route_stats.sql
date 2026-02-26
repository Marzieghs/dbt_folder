{{ config(materialized='table') }}

WITH route_stats AS (
    SELECT 
        -- These are the names that MUST stay consistent
        origin AS airport_code,      -- We use your original name 'airport_code'
        dest AS dest_airport_code,   -- We use your original name 'dest_airport_code'
        COUNT(*) AS total_flights,
        COUNT(DISTINCT tail_number) AS unique_plane, 
        COUNT(DISTINCT airline) AS unique_airline,
        ROUND(AVG(actual_elapsed_time), 2) AS avg_elapsed,
        ROUND(AVG(arr_delay), 2) AS avg_arr_delay,
        MAX(arr_delay) AS max_delay,
        MIN(arr_delay) AS min_delay,
        SUM(cancelled) AS cancelled_depratures, -- Matching your original spelling
        SUM(diverted) AS diverted_departures
    FROM {{ ref('prep_flights') }}
    GROUP BY airport_code, dest_airport_code
)

SELECT 
    -- Now we pull exactly what we named above
    rs.airport_code,
    rs.dest_airport_code,
    rs.total_flights,
    rs.unique_plane,
    rs.unique_airline,
    rs.cancelled_depratures,
    rs.diverted_departures,
    rs.avg_elapsed,
    rs.avg_arr_delay,
    rs.max_delay,
    rs.min_delay,

    -- Origin Airport Info
    orig.city AS origin_city,
    orig.country AS origin_country,
    orig.name AS origin_name,

    -- Destination Airport Info
    dest.city AS dest_city,
    dest.country AS dest_country,
    dest.name AS dest_name

FROM route_stats AS rs
-- JOIN #1: For the Origin
LEFT JOIN {{ ref('prep_airports') }} AS orig 
    ON rs.airport_code = orig.faa

-- JOIN #2: For the Destination
LEFT JOIN {{ ref('prep_airports') }} AS dest 
    ON rs.dest_airport_code = dest.faa
