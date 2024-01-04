MILESTONE 3

#TASK 1

ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(12),
ALTER COLUMN product_code TYPE VARCHAR(11),
ALTER COLUMN product_quantity TYPE SMALLINT;

SELECT MAX(LENGTH(product_code)) FROM orders_table;
SELECT MAX(LENGTH(store_code)) FROM orders_table;
SELECT MAX(LENGTH(card_number)) FROM orders_table;

#TASK 2

SELECT MAX(LENGTH(country_code)) FROM dim_users;

ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(255),
ALTER COLUMN last_name TYPE VARCHAR(255),
ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
ALTER COLUMN join_date TYPE DATE USING join_date::DATE;

#TASK 3

SELECT MAX(LENGTH(store_code)) FROM dim_store_details;
SELECT MAX(LENGTH(country_code)) FROM dim_store_details;

UPDATE dim_store_details
SET latitude = COALESCE(latitude, lat)
WHERE latitude IS NULL;

-- After merging, we don't need the second latitude column anymore:
ALTER TABLE dim_store_details
DROP COLUMN lat;

ALTER TABLE dim_store_details
ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT,
ALTER COLUMN locality TYPE VARCHAR(255),
ALTER COLUMN store_code TYPE VARCHAR(11),
ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
ALTER COLUMN store_type TYPE VARCHAR(255),
ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT,
ALTER COLUMN country_code TYPE VARCHAR(2),
ALTER COLUMN continent TYPE VARCHAR(255);

#TASK 4

SELECT * FROM dim_products -- From here we can see that the name of the column is price_£

SELECT price_£, pg_typeof(price_£)
FROM dim_products; -- Type is double precision

ALTER TABLE dim_products
RENAME COLUMN price_£ TO price;

ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(255);

UPDATE dim_products
SET weight_class = CASE
    WHEN weight_kg < 2 THEN 'Light'
    WHEN weight_kg >= 2 AND weight_kg < 40 THEN 'Mid_Sized'
    WHEN weight_kg >= 40 AND weight_kg < 140 THEN 'Heavy'
    WHEN weight_kg >= 140 THEN 'Truck_Required'
    ELSE 'Unknown'  -- Optional, to handle NULL or unexpected values
END;


#TASK 5

ALTER TABLE dim_products
RENAME COLUMN weight_kg TO weight;

ALTER TABLE dim_products
RENAME COLUMN price TO product_price;

ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

SELECT MAX(LENGTH("EAN")) FROM dim_products;
SELECT MAX(LENGTH(product_code)) FROM dim_products;
SELECT MAX(LENGTH(weight_class)) FROM dim_products;



ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT,
ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT,
ALTER COLUMN "EAN" TYPE VARCHAR(13), 
ALTER COLUMN product_code TYPE VARCHAR(11),  
ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
ALTER COLUMN still_available TYPE BOOL USING CASE WHEN still_available = 'true' THEN TRUE ELSE FALSE END,
ALTER COLUMN weight_class TYPE VARCHAR(14);

#TASK 6

SELECT MAX(LENGTH(month)) FROM dim_date_times;
SELECT MAX(LENGTH(year)) FROM dim_date_times;
SELECT MAX(LENGTH(day)) FROM dim_date_times;
SELECT MAX(LENGTH(time_period)) FROM dim_date_times;

ALTER TABLE dim_date_times
ALTER COLUMN month TYPE VARCHAR(2),
ALTER COLUMN year TYPE VARCHAR(4),
ALTER COLUMN day TYPE VARCHAR(2),
ALTER COLUMN time_period TYPE VARCHAR(10),
ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;

#TASK 7

SELECT MAX(LENGTH(card_number::TEXT)) FROM dim_card_details;
SELECT MAX(LENGTH(expiry_date)) FROM dim_card_details;

ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;


#TASK 8

SELECT card_number, COUNT(*) FROM dim_card_details GROUP BY card_number HAVING COUNT(*) > 1 OR card_number IS NULL;
SELECT date_uuid, COUNT(*) FROM dim_date_times GROUP BY date_uuid HAVING COUNT(*) > 1 OR date_uuid IS NULL;
SELECT product_code, COUNT(*) FROM dim_products GROUP BY product_code HAVING COUNT(*) > 1 OR product_code IS NULL;
SELECT store_code, COUNT(*) FROM dim_store_details GROUP BY store_code HAVING COUNT(*) > 1 OR store_code IS NULL;
SELECT user_uuid, COUNT(*) FROM dim_users GROUP BY user_uuid HAVING COUNT(*) > 1 OR user_uuid IS NULL;

-- All above queries returned "no data" which indicates there are no duplicates or null values in the key columns of our dimension tables

ALTER TABLE dim_card_details ADD PRIMARY KEY (card_number);
ALTER TABLE dim_date_times ADD PRIMARY KEY (date_uuid);
ALTER TABLE dim_products ADD PRIMARY KEY (product_code);
ALTER TABLE dim_store_details ADD PRIMARY KEY (store_code);
ALTER TABLE dim_users ADD PRIMARY KEY (user_uuid);

#TASK 9

SELECT DISTINCT card_number 
FROM orders_table 
WHERE card_number NOT IN (SELECT card_number FROM dim_card_details);


ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_card_number
FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);


ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_date_uuid
FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid);


ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_product_code
FOREIGN KEY (product_code) REFERENCES dim_products(product_code);


ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_store_code
FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code);


ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_user_uuid
FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid);

MILESTONE 4

-- How many stores does the business have and in which countries?
SELECT country_code AS country, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;

-- Which locations currently have the most stores?
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;

-- Which months produce the most sales?
SELECT SUM(orders_table.product_quantity * dim_products.product_price) as total_sales, dim_date_times.month 
FROM dim_date_times 
JOIN orders_table ON dim_date_times.date_uuid = orders_table.date_uuid 
JOIN dim_products ON orders_table.product_code = dim_products.product_code 
GROUP BY dim_date_times.month 
ORDER BY total_sales DESC;

-- How many sales are coming from online?

SELECT COUNT(*) as number_of_sales, SUM(product_quantity) as product_quantity_count,
CASE 
    WHEN store_code LIKE 'WEB%' THEN 'Web'
    ELSE 'Offline'
END AS location    
FROM orders_table
GROUP BY location

-- What percentage of sales comes through from each type of store?
SELECT
	store_type,
	SUM(product_quantity * product_price) AS total_sales,
	ROUND((SUM(product_quantity * product_price)::numeric / SUM(SUM(product_quantity * product_price)::numeric) OVER ()) * 100.0, 2) AS percentage_total
FROM
	orders_table
	JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY store_type
ORDER BY total_sales DESC;


-- Which month in which year produced the most sales?
SELECT
    SUM(product_quantity * product_price) AS total_sales,
    year, month 
FROM orders_table
    JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
	JOIN dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 5;

-- What is our staff headcount?
SELECT 
	SUM(staff_numbers) as total_staff_numbers, country_code
FROM
	dim_store_details
GROUP BY
	country_code
ORDER BY
	total_staff_numbers DESC

-- Which German Store Type is selling the most?
SELECT 
	SUM(product_quantity * product_price) AS total_sales, 
	store_type, 
	country_code 
FROM orders_table
	JOIN dim_products on orders_table.product_code = dim_products.product_code 
	JOIN dim_store_details on orders_table.store_code = dim_store_details.store_code AND dim_store_details.country_code = 'DE' 
GROUP BY store_type, country_code
ORDER BY total_sales 

-- How quickly is the company making sales?
WITH cte AS(
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') as datetimes, year FROM dim_date_times
    ORDER BY datetimes DESC
), cte2 AS(
    SELECT 
        year, 
        datetimes, 
        LEAD(datetimes, 1) OVER (ORDER BY datetimes DESC) as time_difference 
        FROM cte
) SELECT year, AVG((datetimes - time_difference)) as actual_time_taken FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC