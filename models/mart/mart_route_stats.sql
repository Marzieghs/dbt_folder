{{ config(materialized='table') }}
with route_stats as(
select origin as airport_code,
dest as dest_airport_code,
count(*) as total_flights,
count(distinct pf.tail_number) as unique_plane, 
count(distinct pf.airline) as unique_airline,
count (case when cancelled= 1 then 1 end) as cancelled_depratures,
count (case when diverted = 1 then 1 end) as diverted_departures,
round(avg(pf.actual_elapsed_time),2) as avg_elapsed,
round(avg(pf.arr_delay),2) as avg_arr_delay,
max(pf.arr_delay) as max_delay,
min(pf.arr_delay) as min_delay
from {{ ref('prep_flights') }} pf
group by airport_code, dest_airport_code
) select -- Route Identifiers
    rs.origin_code,
    rs.dest_code,
    
    -- Origin Airport Details (from the 'orig' join)
    orig.name as origin_airport_name,
    orig.city as origin_city,
    orig.country as origin_country,
    
    -- Destination Airport Details (from the 'dest' join)
    dest.name as dest_airport_name,
    dest.city as dest_city,
    dest.country as dest_country,
    
    -- Metrics
    rs.total_flights,
    rs.unique_plane,
    rs.unique_airline,
    rs.avg_elapsed,
    rs.avg_arr_delay,
    rs.max_delay,
    rs.min_delay,
    rs.total_cancelled,
    rs.total_diverted
from route_stats as rs
left join {{ ref('prep_airports') }} as orig 
on orig.faa = rs.airport_code
LEFT JOIN {{ ref('prep_airports') }} as dest 
ON rs.dest_airport_code = dest.faa