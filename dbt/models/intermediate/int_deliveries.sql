

-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number', 'over_number', 'ball_in_over']
    )
}}

------------------------------------------------


WITH

deliveries_staging AS (

    SELECT * FROM {{ ref('stg_cricsheet__deliveries') }}

)

SELECT * FROM deliveries_staging

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
