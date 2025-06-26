
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key     INT PRIMARY KEY,
    customerid       INT,
    customername     VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key           INT PRIMARY KEY,
    productid             INT,
    productsubcategory    VARCHAR,
    productname           VARCHAR
);



CREATE TABLE IF NOT EXISTS fact_sales (
    sales_id        INT PRIMARY KEY,
    customer_key    INT REFERENCES dim_customer(customer_key),
    product_key     INT REFERENCES dim_product(product_key),
    territory_key   INT REFERENCES dim_territory(territory_key),
    time_key        INT REFERENCES dim_time(time_key),
    total_quantity  INT,
    average_amount  NUMERIC,
    total_revenue   NUMERIC
);


CREATE TABLE IF NOT EXISTS dim_territory (
    territory_key    INT PRIMARY KEY,
    territoryid      INT,
    provincename     VARCHAR,
    countryregion    VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_key     INT PRIMARY KEY,
    year         INT,
    month        INT,
    day          INT
);

