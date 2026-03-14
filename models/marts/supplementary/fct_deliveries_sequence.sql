-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number', 'ball_in_innings']
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
        COUNT(*) OVER w AS ball_in_innings,
        batter,
        non_striker,
        bowler

    FROM int_deliveries

    WINDOW w AS (
        PARTITION BY match_id, innings_number
        ORDER BY over_number, ball_in_over
    )
)

SELECT * FROM deliveries_sequence

