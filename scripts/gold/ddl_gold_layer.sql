-- DDL: Gold Layer

/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

CREATE VIEW gold.dim_customers AS 
SELECT -- Checked for Duplicate Customer IDs after Joining too, using GROUP BY and then HAVING COUNT(*) > 1
	ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Generating a "Surrogate Key" for each Record in the Table
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	ci.cst_marital_status AS marital_status,
	CASE WHEN ci.cst_gender != 'n/a' THEN ci.cst_gender
		ELSE COALESCE(ca.gen, 'n/a')
	END AS gender,
	ci.cst_create_date AS create_date,
	ca.bdate AS birth_date,
	la.cntry AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
	ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
	ON ci.cst_key = la.cid;


-- Creating Dimension Products

CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER (ORDER BY pn.prod_start_dt, pn.prod_key) AS product_key,
	pn.prod_id AS product_id,
	pn.prod_key AS product_number,
	pn.prod_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prod_cost AS cost,
	pn.prod_line AS product_line,
	pn.prod_start_dt AS start_date
FROM silver.crm_prod_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
WHERE prod_end_dt IS NULL;

-- SELECT * FROM gold.dim_products;

-- Creating Fact Sales
-- Building Fact: Using the Dimension's Surrogate Keys, instead of IDs, to Connect Facts with Dimensions

CREATE VIEW gold.fact_sales AS
SELECT
	sd.sls_ord_num AS order_number, -- Start: Dimension Keys
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date, -- Start: Dates
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount, -- Start: Measures
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
	ON sd.sls_prod_key = pr.product_number
LEFT JOIN gold.dim_customers cu
	ON sd.sls_cust_id = cu.customer_id
WHERE product_key IS NOT NULL;

-- Foreign Key Integrity (Dimensions)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c -- Fact Check: Are all Dimension Tables Successfully Joining to the Fact Table?
	ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
	ON p.product_key = f.product_key
WHERE c.customer_key IS NULL;


-- Star Schema: Creating the 2 Dimensions "Customers" and "Products" with all Columns





























































































