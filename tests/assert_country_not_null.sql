-- country must not be equal to null or "null".

SELECT

    venue_id,
		country

FROM {{ ref('int_venues') }}

WHERE country IS NULL

