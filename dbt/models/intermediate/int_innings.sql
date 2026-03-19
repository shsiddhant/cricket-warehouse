
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number']
    )
}}

------------------------------------------------


WITH

deliveries AS (

    SELECT * FROM {{ ref('int_deliveries') }}

),

innings AS (

    SELECT

        match_id,
        innings_number,

        ANY_VALUE(hash_id) AS hash_id,
        ANY_VALUE(team) AS team,
        ANY_VALUE(super_over) AS super_over,

        SUM(runs) AS runs_scored,
        COUNT(player_out) AS wickets_lost,
        COUNT(*) FILTER (WHERE is_legal) AS legal_deliveries,
        SUM(wides) AS wides,
        SUM(noballs) AS noballs,
        SUM(byes) AS byes,
        SUM(legbyes) AS legbyes,
        SUM(extras) AS extras

    FROM deliveries

    GROUP BY (match_id, innings_number)

)

SELECT * FROM innings

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
