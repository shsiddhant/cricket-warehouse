
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key='match_id'
    )
}}

------------------------------------------------

WITH

staging AS (

    SELECT * FROM {{ ref('stg_cricsheet__match_info') }}

),

venues AS (

    SELECT * FROM {{ ref('int_venues') }}

),

matches AS (

    SELECT
        DISTINCT ON (match_id)
        
        staging.match_id,
        staging.hash_id,
        venues.venue_id,

        TO_DATE(staging.info->'dates'->> 0, 'YYYY-MM-DD') AS start_date,

        staging.info->>'match_type' AS format,
        staging.info->>'gender' AS gender,

        staging.info->'event'->>'name' AS event_name,
        (staging.info->'event'->>'match_number')::INTEGER AS event_match_number,
        staging.info->'event'->>'stage' AS event_stage,


        staging.info->'toss'->>'winner' AS toss_winner,
        staging.info->'toss'->>'decision' AS toss_decision,
        
        CASE
            -- If 'winner' key is there, then put winner,
            WHEN staging.info->'outcome'->>'winner' IS NOT NULL
                THEN 'winner'
            -- If no winner and 'result' is present, then put that ('tie' / 'no result'
            WHEN staging.info->'outcome'->>'result' IS NOT NULL
                THEN staging.info->'outcome'->>'result' END
        AS outcome_type,
        
        staging.info->'outcome'->'by' AS outcome_margin,
        staging.info->'outcome'->>'winner' AS winner,
        staging.info->>'player_of_match' AS player_of_match,
        
        {{ dbt.current_timestamp() }} AS last_update
    
    FROM staging

    LEFT JOIN venues
        ON staging.info->>'venue' = venues.venue_name

)

SELECT * FROM matches

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
