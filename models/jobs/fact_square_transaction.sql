{{
	config(
		materialized='incremental',
		unique_key='PAYMENT_ID'
	)
}}
SELECT DISTINCT row_number() over (order by SP.ID asc) AS TRANSACTION_ID, CAST(CREATED_AT AS DATE) AS date_of_transaction, discount_money:amount::NUMBER AS DISCOUNT_AMOUNT, NULL AS REFUND_REASON, SL.ID AS LOCATION_ID, merchant_ID, LATFLAPAY.ITEM_ID AS ITEM_ID, LATFLAPAY.COST_PER_ITEM AS VALUE_AMOUNT, LATFLAPAY.EMPLOYEE_ID AS EMPLOYEE_ID, SP.ID AS PAYMENT_ID
from PC_STITCH_DB.SQUAREBI.SQUARE_PAYMENTS SP
LEFT JOIN PC_STITCH_DB.SQUAREBI.SQUARE_LOCATION SL
   ON SP.DEVICE:name = SL.NAME
  LEFT JOIN (
    SELECT DISTINCT ID, fl.value:item_detail:item_id::string as ITEM_ID, fl.value:single_quantity_money:amount::number as COST_PER_ITEM, f.value:employee_id as EMPLOYEE_ID
     from PC_STITCH_DB.SQUAREBI.SQUARE_PAYMENTS
  , lateral flatten( input => tender ) f
  , lateral flatten( input => itemizations ) fl
  ) LATFLAPAY ON SP.ID = LATFLAPAY.ID

{% if is_incremental() %}
	WHERE date_of_transaction > (select max(date_of_transaction) from {{ this }})
{% endif %}  
  
  
UNION  
  
SELECT DISTINCT row_number() over (order by CREATED_AT DESC) AS TRANSACTION_ID, CAST(CREATED_AT AS DATE) AS date_of_transaction, NULL, REASON AS REFUND_REASON, LOCATION_ID, MERCHANT_ID, NULL, REFUNDED_MONEY:amount::NUMBER AS REFUNDED_AMOUNT, NULL, PAYMENT_ID
	from PC_STITCH_DB.SQUAREBI.SQUARE_REFUNDS

	
	
	{% if is_incremental() %}
	WHERE date_of_transaction > (select max(date_of_transaction) from {{ this }})
{% endif %}
	
	
	
	
