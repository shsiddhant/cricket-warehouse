
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key='venue_id'
    )
}}

------------------------------------------------

WITH

venue_city AS (

    SELECT * FROM {{ ref('venue_city') }} -- seed

),

city_country AS (

    SELECT * FROM {{ ref('city_country') }} -- seed

),

venues_with_hash_id AS (
    
    SELECT 

        {{ dbt_utils.generate_surrogate_key(["venue_name"]) }} AS venue_id,

        venue_city.venue_name,
        venue_city.city,
        city_country.country,

        {{ dbt.current_timestamp() }} AS last_update

    FROM venue_city

    JOIN city_country
        USING (city)
)

SELECT * FROM venues_with_hash_id

