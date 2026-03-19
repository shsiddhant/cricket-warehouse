
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key='team_id'
    )
}}

------------------------------------------------

WITH

match_teams AS (
    SELECT * FROM {{ ref('int_match_teams') }}
),

teams AS (

    SELECT
        DISTINCT ON (team, format)

        {{ dbt_utils.generate_surrogate_key([
            'team',
            'format'
            ])
        }} AS team_id,

        hash_id,
        team,
        format,

        {{ dbt.current_timestamp() }} AS last_update
    
    FROM match_teams
    
)

SELECT * FROM teams

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
