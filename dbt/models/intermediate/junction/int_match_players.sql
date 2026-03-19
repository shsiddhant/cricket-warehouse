
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'player_id']
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

match_players AS (

    SELECT
        DISTINCT ON (match_id, player_id)

        {{ dbt_utils.generate_surrogate_key([
            'matches.match_id',
            'match_players_names.player_name'
            ])
        }} AS match_player_id,

        matches.match_id,
        staging.hash_id,

        staging.info->'registry'->'people'->>match_players_names.player_name AS player_id,

        match_players_names.player_name,
        match_players.team,
        matches.format

    FROM staging

    CROSS JOIN
        LATERAL jsonb_each(info->'players') AS match_players(team, players)
    
    CROSS JOIN
        LATERAL jsonb_array_elements_text(match_players.players) AS match_players_names(player_name)

    JOIN matches
        ON staging.match_id = matches.match_id

)

SELECT * FROM match_players

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
