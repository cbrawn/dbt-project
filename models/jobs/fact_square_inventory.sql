{{
	config(
		materialized='incremental',
		unique_key='LOCATION_ID'
	)
}}
SELECT DISTINCT LOCATION_ID, QUANTITY_ON_HAND, VARIATION_ID
	FROM PC_STITCH_DB.SQUAREBI.SQUARE_INVENTORY SI
