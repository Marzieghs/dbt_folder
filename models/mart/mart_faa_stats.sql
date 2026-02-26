SELECT 
    faa, 
    name, 
    city 
FROM {{ ref('prep_airports') }}