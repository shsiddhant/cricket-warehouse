
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['team_id', 'player_id']
    )
}}

------------------------------------------------
WITH

teams as (

    select * from {{ ref('int_teams') }}

),

match_players as (

    select * from {{ ref('int_match_players') }}

),

players AS (
    
    SELECT
        DISTINCT ON (team_id, player_id)
        
        teams.team_id AS team_id,
        match_players.player_id,
        match_players.hash_id,
        match_players.player_name,
        match_players.team,
        match_players.format

    FROM match_players
    
    
    JOIN teams
        ON match_players.team = teams.team
        AND match_players.format = teams.format

)

SELECT * FROM players

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
