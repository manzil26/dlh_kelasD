-- =======================
-- DROP TABEL FAKTA
-- =======================
DROP TABLE IF EXISTS fact_inventory_conditions CASCADE;
DROP TABLE IF EXISTS fact_financial_report CASCADE;
DROP TABLE IF EXISTS fact_social_sentiment CASCADE;

select *from  dim_platform dp 


-- =======================
-- DROP TABEL DIMENSI
-- =======================

DROP TABLE IF EXISTS dim_time_warehouse CASCADE;
DROP TABLE IF EXISTS dim_warehouse CASCADE;
DROP TABLE IF EXISTS dim_segment CASCADE;
DROP TABLE IF EXISTS dim_location CASCADE;
DROP TABLE IF EXISTS dim_product_competitor CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_company CASCADE;
DROP TABLE IF EXISTS dim_sentiment CASCADE;
DROP TABLE IF EXISTS dim_platform CASCADE;
DROP TABLE IF EXISTS dim_time_txt CASCADE;

select * from  dim_platform dp 
-- =======================
-- TRUNCATE FAKTA
-- =======================

TRUNCATE TABLE fact_inventory_conditions CASCADE;
TRUNCATE TABLE fact_financial_report CASCADE;
TRUNCATE TABLE fact_social_sentiment CASCADE;

-- =======================
-- TRUNCATE DIMENSI
-- =======================

TRUNCATE TABLE dim_time_warehouse CASCADE;
TRUNCATE TABLE dim_warehouse CASCADE;
TRUNCATE TABLE dim_segment CASCADE;
TRUNCATE TABLE dim_location CASCADE;
TRUNCATE TABLE dim_product_competitor CASCADE;
TRUNCATE TABLE dim_date CASCADE;
TRUNCATE TABLE dim_company CASCADE;
TRUNCATE TABLE dim_sentiment CASCADE;
TRUNCATE TABLE dim_platform CASCADE;
TRUNCATE TABLE dim_time_txt CASCADE;
select * from  dim_sentiment ds 

select * from  dim_time_txt
-- =======================
-- TABEL DIMENSI
-- =======================

-- dim_time_txt
CREATE TABLE IF NOT EXISTS dim_time_txt (
    time_id SERIAL PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    UNIQUE (day, month, year)
);

-- dim_platform
CREATE TABLE IF NOT EXISTS dim_platform (
    platform_key SERIAL PRIMARY KEY,
    platform_name VARCHAR(255) UNIQUE
);

-- dim_sentiment
CREATE TABLE IF NOT EXISTS dim_sentiment (
    sentiment_key SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    product_subcategory VARCHAR(255),
    sentiment_label VARCHAR(20),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE (product_name, product_subcategory, is_current)
);

-- dim_company
CREATE TABLE IF NOT EXISTS dim_company (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255),
    notes TEXT,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE (company_name, is_current)
);

-- dim_date
CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    UNIQUE (day, month, year)
);

-- dim_product_competitor
CREATE TABLE IF NOT EXISTS dim_product_competitor (
    product_id SERIAL PRIMARY KEY,
    best_selling_product VARCHAR(255),
    product_category VARCHAR(255),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE (best_selling_product, is_current)
);

-- dim_location
CREATE TABLE IF NOT EXISTS dim_location (
    location_id SERIAL PRIMARY KEY,
    country_region VARCHAR(255),
    province_name VARCHAR(255),
    UNIQUE (country_region, province_name)
);

-- dim_segment
CREATE TABLE IF NOT EXISTS dim_segment (
    segment_id SERIAL PRIMARY KEY,
    market_segment VARCHAR(255) UNIQUE
);

-- dim_warehouse
CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_sk SERIAL PRIMARY KEY,
    warehouse_id VARCHAR(10),
    warehouse_name VARCHAR(255),
    location VARCHAR(255),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE,
    UNIQUE (warehouse_id, is_current)
);

SELECT table_name FROM information_schema.tables
WHERE table_schema = 'public';


-- dim_time_warehouse
CREATE TABLE IF NOT EXISTS dim_time_warehouse (
    time_id SERIAL PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    UNIQUE (day, month, year)
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key           INT PRIMARY KEY,
    productid             INT,
    productsubcategory    VARCHAR,
    productname           VARCHAR
);  

CREATE TABLE IF NOT EXISTS dim_product (
    product_key           INT PRIMARY KEY,
    productid             INT,
    productsubcategory    VARCHAR,
    productname           VARCHAR
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_cleaned_productname
ON dim_product (LOWER(TRIM(productname)));

-- =======================
-- TABEL FAKTA
-- =======================

-- fact_social_sentiment
CREATE TABLE IF NOT EXISTS fact_social_sentiment (
    fact_id SERIAL PRIMARY KEY,
    time_id INT REFERENCES dim_time_txt(time_id),
    platform_key INT REFERENCES dim_platform(platform_key),
    sentiment_key INT REFERENCES dim_sentiment(sentiment_key),
    post_count INT,
    UNIQUE (time_id, platform_key, sentiment_key)
);

-- fact_financial_report
CREATE TABLE IF NOT EXISTS fact_financial_report (
    fact_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES dim_company(company_id),
    date_id INT REFERENCES dim_date(date_id),
    product_id INT REFERENCES dim_product_competitor(product_id),
    location_id INT REFERENCES dim_location(location_id),
    segment_id INT REFERENCES dim_segment(segment_id),
    revenue FLOAT,
    expense FLOAT,
    profit FLOAT
);

-- fact_inventory_conditions
CREATE TABLE IF NOT EXISTS fact_inventory_conditions (
    fact_id SERIAL PRIMARY KEY,
    warehouse_sk INT REFERENCES dim_warehouse(warehouse_sk),
    time_id INT REFERENCES dim_time_warehouse(time_id),
    product_key INT REFERENCES dim_product(product_key),
    damaged_units INT,
    total_units INT,
    storage_temp_c FLOAT
);
