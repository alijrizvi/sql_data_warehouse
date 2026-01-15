/*

-- Cleaning & Loading all Data from the Bronze Layer on to the Silver Layer

- Removing Duplicate Primary Keys (Customer IDs): We only need Customer IDs with their Latest Date of Purchase
- Quality Check: Removing Unwanted Spaces
- Quality Check: Checking the Consistency of Values in Low-Cardinality Columns 
(i.e., Columns with a Limited number of Possible values)

- Will also be Transforming Date such that the Primary Keys and other Columns we intend to Join on, based
on our Data Flow diagram, align well and will be doable

- The Checks performed for Data Transformation plans were also done here, but much of their experimental
code was removed and simply Implemented in the "SELECT" clauses for the Final code

*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time   TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time   TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();

    BEGIN  -- TRY BLOCK

        RAISE NOTICE '=============================================';
        RAISE NOTICE 'Loading Silver Layer';
        RAISE NOTICE '=============================================';

        ----------------------------------------------------------------
        -- Table #1: silver.crm_cust_info
        ----------------------------------------------------------------
        start_time := clock_timestamp();

        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gender,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'S'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'M'
                ELSE 'N/A'
            END,
            CASE
                WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'F'
                WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'M'
                ELSE 'N/A'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY cst_id
                       ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
        ) f
        WHERE flag_last = 1;

        end_time := clock_timestamp();
        RAISE NOTICE 'Table #1 (crm_cust_info) load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        ----------------------------------------------------------------
        -- Table #2: silver.crm_prod_info
        ----------------------------------------------------------------
        start_time := clock_timestamp();

        INSERT INTO silver.crm_prod_info (
            prod_id,
            cat_id,
            prod_key,
            prod_nm,
            prod_cost,
            prod_line,
            prod_start_dt,
            prod_end_dt
        )
        SELECT
            prod_id,
            REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prod_key, 7) AS prod_key,
            prod_nm,
            COALESCE(prod_cost, 0),
            CASE
                WHEN UPPER(TRIM(prod_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prod_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prod_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prod_line)) = 'T' THEN 'Touring'
                ELSE 'N/A'
            END,
            prod_start_dt,
            prod_end_dt
        FROM bronze.crm_prod_info
        WHERE
        (
            REPLACE(SUBSTRING(prod_key, 1, 5), '-', '_') IN
                (SELECT id FROM bronze.erp_px_cat_g1v2)
            OR
            SUBSTRING(prod_key, 7) IN
                (SELECT DISTINCT sls_prod_key FROM bronze.crm_sales_details)
        )
        AND prod_cost >= 0
        AND prod_end_dt > prod_start_dt;

        end_time := clock_timestamp();
        RAISE NOTICE 'Table #2 (crm_prod_info) load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        ----------------------------------------------------------------
        -- Table #3: silver.crm_sales_details
        ----------------------------------------------------------------
        start_time := clock_timestamp();

        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prod_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prod_key,
            sls_cust_id,
            CASE
                WHEN sls_order_dt = 0 THEN NULL
                WHEN LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
            END,
            CASE
                WHEN sls_ship_dt = 0 THEN NULL
                WHEN LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
            END,
            CASE
                WHEN sls_due_dt = 0 THEN NULL
                WHEN LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
                ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
            END,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0
                THEN ABS(sls_quantity) * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        end_time := clock_timestamp();
        RAISE NOTICE 'Table #3 (crm_sales_details) load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        ----------------------------------------------------------------
        -- Table #4: silver.erp_cust_az12
        ----------------------------------------------------------------
        start_time := clock_timestamp();

        INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
                ELSE cid
            END,
            CASE
                WHEN bdate > NOW() THEN NULL
                WHEN bdate < DATE '1900-01-01' THEN NULL
                ELSE bdate
            END,
            CASE
                WHEN TRIM(UPPER(gen)) LIKE 'M%' THEN 'M'
                WHEN TRIM(UPPER(gen)) LIKE 'F%' THEN 'F'
                ELSE NULL
            END
        FROM bronze.erp_cust_az12;

        end_time := clock_timestamp();
        RAISE NOTICE 'Table #4 (erp_cust_az12) load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        ----------------------------------------------------------------
        -- Table #5: silver.erp_loc_a101
        ----------------------------------------------------------------
        start_time := clock_timestamp();

        INSERT INTO silver.erp_loc_a101 (cid, cntry)
        SELECT
            REPLACE(cid, '-', ''),
            CASE
                WHEN TRIM(cntry) IS NULL OR TRIM(cntry) = '' THEN NULL
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                ELSE TRIM(cntry)
            END
        FROM bronze.erp_loc_a101
        WHERE REPLACE(cid, '-', '') IN (
            SELECT cst_key FROM silver.crm_cust_info
        );

        end_time := clock_timestamp();
        RAISE NOTICE 'Table #5 (erp_loc_a101) load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        ----------------------------------------------------------------
        -- Table #6: silver.erp_px_cat_g1v2
        ----------------------------------------------------------------
        start_time := clock_timestamp();

        INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        SELECT id, cat, subcat, maintenance
        FROM bronze.erp_px_cat_g1v2;

        end_time := clock_timestamp();
        RAISE NOTICE 'Table #6 (erp_px_cat_g1v2) load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        ----------------------------------------------------------------
        -- Batch End
        ----------------------------------------------------------------
        batch_end_time := clock_timestamp();

        RAISE NOTICE '=============================================';
        RAISE NOTICE 'Total Silver Load Duration: % seconds',
            EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        RAISE NOTICE '=============================================';

    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE '❌ ERROR: Error occurred during Silver layer load';
        RAISE NOTICE 'Message: %', SQLERRM;
    END;

END;
$$;

-- Executing the Stored Procedure for the Silver Layer

CALL silver.load_silver();