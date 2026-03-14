
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id']
    )
}}

------------------------------------------------

WITH


matches AS (

    SELECT * FROM {{ ref('int_matches') }}

),

innings AS (

    SELECT * FROM {{ ref('int_innings') }}

),

innings_with_overs_display AS (
    
    SELECT
        
        *,

        DIV(legal_deliveries, 6)::text || '.' || MOD(legal_deliveries, 6) AS overs_display

    FROM innings

),

team_scores AS (

    SELECT

        match_id,
        team,
        MIN(innings_number) AS ordering,

        STRING_AGG(

            runs_scored::text || '/' || wickets_lost::text || ' (' || overs_display || ')',
            ' & '
            ORDER BY innings_number

        ) AS scores

    FROM innings_with_overs_display

    GROUP BY match_id, team

),

match_scores AS (

    SELECT

        match_id,

        STRING_AGG(

            team || ' ' || scores,
            ' vs '
            ORDER BY ordering

        ) AS scoreline

    FROM team_scores

    GROUP BY match_id
),

match_summary AS (

    SELECT

        matches.match_id,
        matches.start_date,
        match_scores.scoreline,

        matches.winner ||
            CASE
                WHEN matches.winner IS NOT NULL
                    THEN ' won by '

                ELSE ''

            END ||

            CASE
                WHEN matches.outcome_margin->'runs' IS NOT NULL
                    THEN matches.outcome_margin->>'runs' || ' runs'

                WHEN matches.outcome_margin->'wickets' IS NOT NULL
                    THEN matches.outcome_margin->>'wickets' || ' wickets'

                ELSE ''
            END
            AS result

        FROM matches

        LEFT JOIN match_scores
            ON matches.match_id = match_scores.match_id

)

SELECT * FROM match_summary
