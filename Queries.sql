-- Query for factOlist

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.factOlist
	WITH (
	LOCATION = 'data-after-etl-sql-database/factOlist/',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
    AS
    SELECT  
    price,
    freight_value,
    payment_installments,
    payment_value,
    review_score,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,	
    product_height_cm,	
    product_width_cm
    FROM Olistecom.olist_orders o
    JOIN Olistecom.olist_customers c
    on o.customer_id=c.customer_id
    JOIN Olistecom.olist_geolocation g
    on c.customer_zip_code_prefix=g.geolocation_zip_code_prefix
    JOIN Olistecom.olist_sellers s
    on g.geolocation_zip_code_prefix=s.seller_zip_code_prefix
    JOIN Olistecom.olist_order_items oi
    on o.order_id=oi.order_id
    JOIN Olistecom.olist_order_payments op
    on oi.order_id=op.order_id
    and o.order_id=op.order_id
    JOIN Olistecom.olist_products p
    on oi.product_id=p.product_id
    JOIN Olistecom.olist_order_reviews ore
    on o.order_id=ore.order_id
GO


SELECT TOP 100 * FROM Olistecom.factOlist
GO






-- Query for sellers

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_sellers (
	[seller_id] nvarchar(4000),
	[seller_zip_code_prefix] bigint,
	[seller_city] nvarchar(4000),
	[seller_state] nvarchar(4000)
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/sellers/part-00000-tid-5987832571858376508-05be1a48-b022-4eba-8736-d13676b7b282-299-1-c000.csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_sellers
GO






-- Query for products

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_products (
	[product_id] nvarchar(4000),
	[product_category_name] nvarchar(4000),
	[product_name_length] bigint,
	[product_description_length] bigint,
	[product_photos_qty] bigint,
	[product_weight_g] bigint,
	[product_length_cm] bigint,
	[product_height_cm] bigint,
	[product_width_cm] bigint
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/products/part-00000-tid-91177716132913510-9d6d6e01-eb43-4cb8-a29e-230647413451-297-1-c000.csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_products
GO






-- Query for orders

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_orders (
	[order_id] nvarchar(4000),
	[customer_id] nvarchar(4000),
	[order_status] nvarchar(4000),
	[order_purchase_timestamp] datetime2(0),
	[order_approved_at] datetime2(0),
	[order_delivered_carrier_date] datetime2(0),
	[order_delivered_customer_date] datetime2(0),
	[order_estimated_delivery_date] datetime2(0)
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/orders/part-00000-tid-7957317315142628117-e57f2fd0-5c0a-4726-8cc4-038523c1c014-295-1-c000.csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_orders
GO






-- Query for order_reviews

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_order_reviews (
	[review_id] nvarchar(4000),
	[order_id] nvarchar(4000),
	[review_score] bigint,
	[review_creation_date] nvarchar(4000),
	[review_answer_timestamp] nvarchar(4000)
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/order_reviews/part-00000-tid-6720113690828659362-bdf09a91-1b2b-4897-a6c9-9eb4f97f1059-360-1-c000 (1).csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_order_reviews
GO






-- Query for order_payments

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_order_payments (
	[order_id] nvarchar(4000),
	[payment_sequential] bigint,
	[payment_type] nvarchar(4000),
	[payment_installments] bigint,
	[payment_value] float
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/order_payments/part-00000-tid-2316200056330485633-af47ce34-f640-4e54-a831-ae83c8d54f3e-285-1-c000 (1).csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_order_payments
GO






-- Query for order_items

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_order_items (
	[order_id] nvarchar(4000),
	[order_item_id] bigint,
	[product_id] nvarchar(4000),
	[seller_id] nvarchar(4000),
	[shipping_limit_date] datetime2(0),
	[price] float,
	[freight_value] float
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/order_items/part-00000-tid-6846895615314642463-4e186285-49aa-4f56-a881-e9074bdeeba9-282-1-c000.csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_order_items
GO






-- Queries for geolocation

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_geolocation (
	[geolocation_zip_code_prefix] bigint,
	[geolocation_lat] float,
	[geolocation_lng] float,
	[geolocation_city] nvarchar(4000),
	[geolocation_state] nvarchar(4000)
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/geolocation/part-00000-tid-6291182367145135655-53e1eb31-bbc6-4585-b38f-b4445cff645b-277-1-c000 (1).csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_geolocation
GO






-- Query for customers

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_customers (
	[customer_id] nvarchar(4000),
	[customer_unique_id] nvarchar(4000),
	[customer_zip_code_prefix] bigint,
	[customer_city] nvarchar(4000),
	[customer_state] nvarchar(4000)
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/customers/part-00000-tid-5285133680287256689-3ab6fceb-8f75-4206-8172-f45b3d2bd35a-272-1-c000 (1).csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_customers
GO






-- Query for product_category_name_translation

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDelimitedTextFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDelimitedTextFormat] 
	WITH ( FORMAT_TYPE = DELIMITEDTEXT ,
	       FORMAT_OPTIONS (
			 FIELD_TERMINATOR = ',',
			 FIRST_ROW = 2,
			 USE_TYPE_DEFAULT = FALSE
			))
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'olist-ecommerce_olistecommercedataset_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [olist-ecommerce_olistecommercedataset_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://olist-ecommerce@olistecommercedataset.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE Olistecom.olist_product_category_name_translation (
	[product_category_name] nvarchar(4000),
	[product_category_name_english] nvarchar(4000)
	)
	WITH (
	LOCATION = 'data-after-etl-sql-database/product_category_name_translation/part-00000-tid-7555851371885829816-bd8425ed-dd4f-437d-9f5a-3849e7b350a5-301-1-c000.csv',
	DATA_SOURCE = [olist-ecommerce_olistecommercedataset_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDelimitedTextFormat]
	)
GO


SELECT TOP 100 * FROM Olistecom.olist_product_category_name_translation
GO