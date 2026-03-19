
-- Configuration

{{
    config(
        materialized='incremental',
        unique_key=['match_id', 'innings_number', 'player_out']
    )
}}

------------------------------------------------

WITH

deliveries AS (

	SELECT * FROM {{ ref('int_deliveries') }}

),

dismissed_players AS (

	SELECT
			DISTINCT 
			
			match_id,
			innings_number,
			player_out

	FROM deliveries

	WHERE player_out IS NOT NULL

)


SELECT * FROM dismissed_players

