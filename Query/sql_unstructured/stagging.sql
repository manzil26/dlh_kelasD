-- ======================================
-- DROP TABEL STAGING (Jalankan dulu)
-- ======================================

DROP TABLE IF EXISTS public.stg_social_posts;
DROP TABLE IF EXISTS public.financial_report;
DROP TABLE IF EXISTS public.stg_temperature_readings;
DROP TABLE IF EXISTS public.stg_warehouse_inventory_conditions;
DROP TABLE IF EXISTS public.stg_energy_consumption;




select * from stg_social_posts
-- =======================
-- TRUNCATE STAGING DAN OPERASIONAL
-- =======================

TRUNCATE TABLE public.stg_social_posts;
TRUNCATE TABLE public.financial_report;
TRUNCATE TABLE public.stg_temperature_readings;
TRUNCATE TABLE public.stg_warehouse_inventory_conditions;
TRUNCATE TABLE public.stg_energy_consumption;

-- ======================================
-- CREATE TABEL STAGING
-- ======================================

-- 1. Tabel Staging untuk Posting Sosial Media
CREATE TABLE public.stg_social_posts (
    posted_at DATE,
    platform VARCHAR(100),
    username VARCHAR(100),
    message TEXT,
    product_mention VARCHAR(255),
    productsubcategory VARCHAR(100),
    sentiment_label VARCHAR(20),
    post_count INT
);

select * from public.stg_social_posts
-- 2. Tabel Staging untuk Laporan Keuangan dari PDF
CREATE TABLE public.financial_report (
    company_name VARCHAR(255),
    report_date DATE,
    revenue NUMERIC(15,2),
    expense NUMERIC(15,2),
    market_segment VARCHAR(100),
    notes TEXT,
    country_region VARCHAR(100),
    province_name VARCHAR(100),
    best_selling_product VARCHAR(255),
    product_category VARCHAR(100),
    profit NUMERIC(15,2)  -- akan dihitung setelah insert
);

-- 3. Tabel Staging untuk Pembacaan Suhu Gudang
CREATE TABLE public.stg_temperature_readings (
    date DATE,
    warehouse_id VARCHAR(50),
    warehouse_name VARCHAR(255),
    temperature_c NUMERIC(5,2),
    humidity NUMERIC(5,2),
    city VARCHAR(100)
);

-- 4. Tabel Staging untuk Kondisi Inventori Gudang
CREATE TABLE public.stg_warehouse_inventory_conditions (
    warehouse_id VARCHAR(50),
    date DATE,
    item_type VARCHAR(100),
    damaged_units INT,
    total_units INT,
    storage_temp_c NUMERIC(5,2)
);

-- 5. Tabel Staging untuk Konsumsi Energi
CREATE TABLE public.stg_energy_consumption (
    date DATE,
    warehouse_id VARCHAR(50),
    energy_used_kwh NUMERIC(10,2),
    avg_temp_c NUMERIC(5,2)
);






