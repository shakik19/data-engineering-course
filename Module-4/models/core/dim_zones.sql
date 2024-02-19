SELECT
    LocationID as location_id,
    Borough as borough,
    Zone AS zone,
    replace(service_zone, 'Boro', 'Green') as service_zone
FROM {{ ref('taxi_zone_lookup') }}
WHERE Borough != 'Unknown'