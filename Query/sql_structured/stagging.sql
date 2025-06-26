-- Buat tabel customer
DROP TABLE IF EXISTS customer;
CREATE TABLE customer AS
SELECT * FROM sales.customer;

-- Buat tabel person
DROP TABLE IF EXISTS person;
CREATE TABLE person AS
SELECT * FROM person.person;

-- Buat tabel product
DROP TABLE IF EXISTS product;
CREATE TABLE product AS
SELECT * FROM production.product;

-- Buat tabel productcategory
DROP TABLE IF EXISTS productcategory;
CREATE TABLE productcategory AS
SELECT * FROM production.productcategory;

-- Buat tabel productsubcategory
DROP TABLE IF EXISTS productsubcategory;
CREATE TABLE productsubcategory AS
SELECT * FROM production.productsubcategory;

-- Buat tabel salesorderheader
DROP TABLE IF EXISTS salesorderheader;
CREATE TABLE salesorderheader AS
SELECT * FROM sales.salesorderheader;

-- Buat tabel salesorderdetail
DROP TABLE IF EXISTS salesorderdetail;
CREATE TABLE salesorderdetail AS
SELECT * FROM sales.salesorderdetail;

-- Buat tabel salesterritory
DROP TABLE IF EXISTS salesterritory;
CREATE TABLE salesterritory AS
SELECT * FROM sales.salesterritory;

-- Buat tabel stateprovince
DROP TABLE IF EXISTS stateprovince;
CREATE TABLE stateprovince AS
SELECT * FROM person.stateprovince;

-- Buat tabel countryregion
DROP TABLE IF EXISTS countryregion;
CREATE TABLE countryregion AS
SELECT * FROM person.countryregion;
