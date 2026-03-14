
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number', 'bowling_position']
    )
}}

------------------------------------------------


WITH

int_deliveries AS (

    SELECT * FROM {{ ref('int_deliveries') }}

),

deliveries_sequence AS (

    SELECT * FROM {{ ref('fct_deliveries_sequence') }}

),

first_appearance_in_innings AS (

    SELECT

        match_id,
        innings_number,
        bowler,

        MIN(ball_in_innings) AS first_appearance

    FROM deliveries_sequence

    GROUP BY match_id, innings_number, bowler

),

bowling_order AS (

    SELECT

        match_id,
        innings_number,
        bowler,
        COUNT(*) OVER w AS bowling_position

    FROM first_appearance_in_innings

    WINDOW w AS (
        PARTITION BY match_id, innings_number
        ORDER BY first_appearance
    )
)

SELECT * FROM bowling_order

