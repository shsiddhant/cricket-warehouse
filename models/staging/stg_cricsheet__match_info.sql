-- Configuration

{{
    config(
        materialized='incremental'
    )
}}

----------------------------------------------------

WITH 

matches_json AS (
    
    SELECT * FROM {{ source('cricsheet', 'matches_json') }}

),


match_info_staging AS (
    
    SELECT
        DISTINCT ON (match_id)

        (data->'match_id')::INTEGER AS match_id,
        {{ dbt_utils.generate_surrogate_key(["data"]) }} AS hash_id,
        data->'info' AS info,
        {{ dbt.current_timestamp() }} AS last_update
    
    FROM matches_json

)

SELECT * FROM match_info_staging

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT hash_id FROM {{ this }})

{% endif %}
