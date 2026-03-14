
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number', 'batting_position']
    )
}}

------------------------------------------------

WITH

dismissed_players AS (

	SELECT * FROM {{ ref('fct_dismissed_players') }}

),

deliveries AS (

    SELECT * FROM {{ ref('int_deliveries') }}

),

batting_order AS (

    SELECT * FROM {{ ref('fct_batting_order') }}

),

batter_stats_without_dismissal AS (

    SELECT

        batting_order.match_id,
        batting_order.innings_number,
        batting_order.batting_position,

        COALESCE(

            ANY_VALUE(deliveries.super_over), FALSE

        ) AS is_super_over,
        
        ANY_VALUE(deliveries.team) AS team,

        ANY_VALUE(batting_order.player_name) AS player_name,

        COALESCE(

            SUM(deliveries.batter_runs), 0

        ) AS runs,

        SUM(
            CASE
                WHEN deliveries.wides = 0 THEN 1
               
                ELSE 0

            END

        ) AS balls

    FROM batting_order

    JOIN deliveries
        ON deliveries.match_id = batting_order.match_id
        AND deliveries.innings_number = batting_order.innings_number
        AND deliveries.batter = batting_order.player_name

    GROUP BY
        batting_order.match_id,
        batting_order.innings_number,
        batting_order.batting_position

),

batting_scorecard AS (

    SELECT

				batter_stats_without_dismissal.*,
    		(dismissed_players.player_out IS NOT NULL) AS is_dismissed

		FROM batter_stats_without_dismissal

		LEFT JOIN dismissed_players
				ON batter_stats_without_dismissal.match_id = dismissed_players.match_id
        AND batter_stats_without_dismissal.innings_number = dismissed_players.innings_number
        AND batter_stats_without_dismissal.player_name = dismissed_players.player_out

)

SELECT * FROM batting_scorecard

