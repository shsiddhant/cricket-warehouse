
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number', 'bowling_position']
    )
}}

------------------------------------------------

{%
    set dismissal_modes = [
        'lbw',
        'bowled',
        'caught',
        'caught and bowled',
        'stumped',
        'hit wicket'
    ]

%}

WITH

deliveries AS (

    SELECT * FROM {{ ref('int_deliveries') }}

),

bowling_order AS (

    SELECT * FROM {{ ref('fct_bowling_order') }}

),

bowling_scorecard_without_economy AS (

    SELECT

        bowling_order.match_id,
        bowling_order.innings_number,
        bowling_order.bowling_position,

        COALESCE(

            ANY_VALUE(deliveries.super_over), FALSE

        ) AS is_super_over,
        
        ANY_VALUE(deliveries.bowler) AS bowler,

        SUM(
            CASE
                WHEN is_legal THEN 1
                ELSE 0
            END
        ) AS balls,
        
        SUM(batter_runs) + SUM(wides) + SUM(noballs) AS runs,

        SUM(
            CASE
                WHEN
                    player_out IS NOT NULL
                    AND dismissal_mode IN ( 
                        {% for dismissal_mode in dismissal_modes %}
                        '{{ dismissal_mode }}'
                        {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                THEN 1

                ELSE 0

            END

        ) AS wickets,

        SUM(
            CASE
                WHEN is_legal AND batter_runs = 0 THEN 1

                ELSE 0

            END

        ) AS dots,
        
        SUM(
            CASE
                WHEN wides > 0 THEN 1
                
                ELSE 0

            END

        ) AS wides,

        SUM(
            CASE
                WHEN noballs > 0 THEN 1
                
                ELSE 0

            END

        ) AS noballs

    FROM bowling_order

    JOIN deliveries
        ON deliveries.match_id = bowling_order.match_id
        AND deliveries.innings_number = bowling_order.innings_number
        AND deliveries.bowler = bowling_order.bowler

    GROUP BY
        bowling_order.match_id,
        bowling_order.innings_number,
        bowling_order.bowling_position

),

bowling_scorecard AS (

    SELECT

        match_id,
        innings_number,
        is_super_over,
        bowling_position,
        bowler,
        balls,
        runs,
        wickets,
        dots,
        CASE

            WHEN balls > 0
                THEN ROUND(6.0 * runs / balls, 2)

            ELSE NULL
        END
        AS economy,
        wides,
        noballs

    FROM bowling_scorecard_without_economy

)

SELECT * FROM bowling_scorecard

