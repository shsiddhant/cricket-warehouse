-- Configuration

{{
    config(
        materialized='incremental'
    )
}}

------------------------------------------------

WITH 

deliveries_json AS (
    
    SELECT * FROM {{ source('cricsheet', 'deliveries_json') }}

),

deliveries_staging AS (

    SELECT

        match_id::int AS match_id,
        hash_id,
        n_innings AS innings_number,
        team,
        n_over AS over_number,
        n_delivery AS ball_in_over,
        super_over,
     
        COALESCE ((delivery->'runs'->'total')::integer, 0) AS runs,
        COALESCE ((delivery->'runs'->'batter')::integer, 0) AS batter_runs,
        COALESCE ((delivery->'extras'->'wides')::integer, 0) AS wides,
        COALESCE ((delivery->'extras'->'noballs')::integer, 0) AS noballs,
        COALESCE ((delivery->'extras'->'byes')::integer, 0) AS byes,
        COALESCE ((delivery->'extras'->'legbyes')::integer, 0) AS legbyes,
        COALESCE ((delivery->'runs'->'extras')::integer, 0) AS extras,
        
        delivery->>'batter' AS batter,
        delivery->>'non_striker' AS non_striker,
        delivery->>'bowler' AS bowler,
        
        ((delivery->'extras'->'wides') IS NULL) AND
        ((delivery->'extras'->'noballs') IS NULL)
        AS is_legal,
        
        delivery->'wickets'->0->>'player_out' AS player_out,
        delivery->'wickets'->0->>'kind' AS dismissal_mode

        FROM deliveries_json

)

SELECT * FROM deliveries_staging

{% if is_incremental() %}

    WHERE hash_id NOT IN (SELECT DISTINCT hash_id FROM {{ this }})

{% endif %}
