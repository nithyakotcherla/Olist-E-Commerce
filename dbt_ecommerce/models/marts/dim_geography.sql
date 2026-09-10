{{ config(materialized='table') }}

-- Raw geolocation carries many rows per zip prefix (one per captured coordinate).
-- Collapse to one row per prefix so it can be joined as a proper dimension.
SELECT
    zip_code,
    ROUND(AVG(lat)::numeric, 6) AS lat,
    ROUND(AVG(lng)::numeric, 6) AS lng,
    MODE() WITHIN GROUP (ORDER BY city)  AS city,
    MODE() WITHIN GROUP (ORDER BY state) AS state,
    COUNT(*) AS coordinate_count
FROM {{ ref('stg_geolocation') }}
GROUP BY zip_code
