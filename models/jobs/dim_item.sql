{{
	config(
		materialized='incremental',
		unique_key='ITEM_NAME'
	)
}}
SELECT DISTINCT BI.MATERIAL AS ITEM_ID, BI.EUROPEAN_ARTICLE_NUMBERS_UNIVERSAL_PRODUCT_CODE AS BARCODE_NUMBER ,BI.MATERIAL_DESCRIPTION AS ITEM_NAME, BI.PRODUCT_HIERARCHY AS CATEGORY, BI.PRODUCT_HIERACHY_DESCRIPTION AS CATEGORY_NAME, MAX(BI.PACK_SIZE) AS PACK_SIZE, SALES_GROUP_NAME AS BRAND
	FROM BILLING_STG BI
    GROUP BY BI.MATERIAL, BI.EUROPEAN_ARTICLE_NUMBERS_UNIVERSAL_PRODUCT_CODE, BI.PRODUCT_HIERARCHY ,BI.PRODUCT_HIERACHY_DESCRIPTION, BI.MATERIAL_DESCRIPTION, BI.SALES_GROUP_NAME
	
