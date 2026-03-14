
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number', 'batting_position']
    )
}}

------------------------------------------------


WITH

int_deliveries AS (

    SELECT * FROM {{ ref('int_deliveries') }}

),

deliveries_sequence AS (

    SELECT
        match_id,
        innings_number,
        batter,
        non_striker,
        COUNT(*) OVER w AS ball_in_innings

    FROM int_deliveries

    WINDOW w AS (
        PARTITION BY match_id, innings_number
        ORDER BY over_number, ball_in_over
    )

),

first_appearance_in_innings AS (

    SELECT

        match_id,
        innings_number,
        batter AS player,

        1 AS priority,

        MIN(ball_in_innings) AS first_appearance

    FROM deliveries_sequence

    GROUP BY match_id, innings_number, batter

    UNION

    SELECT

        match_id,
        innings_number,
        non_striker AS player,

        2 AS priority,

        MIN(ball_in_innings) AS first_appearance

    FROM deliveries_sequence

    GROUP BY match_id, innings_number, non_striker

),

fct_batting_order_duplicated AS (

    SELECT

        match_id,
        innings_number,
        player,
        priority,
        first_appearance,
        FIRST_VALUE(first_appearance) OVER w AS min_first_appearance

    FROM first_appearance_in_innings

    WINDOW w AS (
        PARTITION BY match_id, innings_number, player
        ORDER BY first_appearance
    )

),

fct_batting_order AS (
     
    SELECT

        match_id,
        innings_number,
        player AS player_name,
        COUNT(*) OVER w AS batting_position
        
    FROM fct_batting_order_duplicated

    WHERE first_appearance = min_first_appearance

    WINDOW w AS (
        PARTITION BY match_id, innings_number
        ORDER BY min_first_appearance, priority
    )

)

SELECT * FROM fct_batting_order

