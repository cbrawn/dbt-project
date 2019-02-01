{{
	config(
		materialized='incremental',
		unique_key='EMPLOYEE_ID'
	)
}}
SELECT FL.VALUE AS AUTHORIZED_LOCATION_ID, SE.ID AS EMPLOYEE_ID
	FROM PC_STITCH_DB.SQUAREBI.SQUARE_EMPLOYEES SE
	,lateral flatten (input => AUTHORIZED_LOCATION_IDS, path => '') fl

