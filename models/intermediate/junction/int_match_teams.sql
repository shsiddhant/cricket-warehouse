
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'team']
    )
}}

------------------------------------------------

WITH

staging AS (

    SELECT * FROM {{ ref('stg_cricsheet__match_info') }}

),

matches AS (

    SELECT * FROM {{ ref('int_matches') }}

),
venues AS (

    SELECT * FROM {{ ref('int_venues') }}

),

match_teams AS (

    SELECT
        DISTINCT ON (match_id, team)
    
        {{ dbt_utils.generate_surrogate_key([
            'matches.match_id',
            't.team'
            ])
        }} AS match_team_id,

        staging.hash_id,
        matches.match_id AS match_id,

        staging.info->>'match_type' AS format,
        t.team AS team,
        staging.info->'teams'->>((2-t.position)::int) AS opponent,
 
        CASE
            WHEN matches.toss_winner = t.team
                THEN matches.toss_decision = 'bat'
            ELSE
                matches.toss_decision = 'field'
            END
        AS batted_first,

        (matches.winner = t.team) AS won_match
    
    FROM staging

    JOIN venues
        ON venues.venue_name = staging.info->>'venue'
    
    JOIN matches
        ON staging.match_id = matches.match_id
    
    CROSS JOIN
        LATERAL jsonb_array_elements_text(staging.info->'teams')
        
        WITH ORDINALITY AS t(team, position)

)

SELECT * FROM match_teams

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
