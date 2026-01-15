/*

Bronze Layer:

1) Analyzing: Understand the Source System by Interviewing Source System Experts, etc.
2) Coding: Data Ingestion; Loading the Data from the Source into the Data Warehouse.
3) Data Validation: Quality Control; Checking for Data Completeness (i.e., Comparing the 
number of Records in the SourceSystem to those in the Bronze layer to ensure we are not 
losing any data) and implementing Schema Checks to ensure all data are placed in the right 
positions.
4) Data Documentation & Committing all work in Git.

*/

/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- DDL (Data Definition Language) for Tables
-- Use Data Profiling to Understand the Data Tables better and then Design Scripts

-- For all CSV files

DROP TABLE IF EXISTS bronze.crm_cust_info; -- T-SQL Logic to Check if Table already Exists
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key VARCHAR(50),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_marital_status VARCHAR(50),
	cst_gender VARCHAR(50),
	cst_create_date DATE
); -- Naming according to our Rules/Naming Conventions

DROP TABLE IF EXISTS bronze.crm_prod_info;
CREATE TABLE bronze.crm_prod_info (
	prod_id INT,
	prod_key VARCHAR(50),
	prod_nm VARCHAR(50),
	prod_cost INT,
	prod_line VARCHAR(50),
	prod_start_dt DATE,
	prod_end_dt DATE
);

DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
	sls_ord_num VARCHAR(50),
	sls_prod_key VARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);

DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
	cid VARCHAR(50),
	cntry VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
	cid VARCHAR(50),
	bdate DATE,
	gen VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
	id VARCHAR(50),
	cat VARCHAR(50),
	subcat VARCHAR(50),
	maintenance VARCHAR(50)
);