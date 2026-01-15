/*

Creating and Saving a "Stored Procedure" to Run the Same SQL Script Repeatedly in the Future
There we go! It is in the "Procedures" Tab and we can access and Run it using "EXEC bronze.load_bronze"
to Load in the Needed CSV files whenever we want to


When building our Stored Procedure, it will be Crucial to Focus not just on getting the Files in,
but also to get them Properly, Monitor how much Time each Table is taking to load in, Ensure
there are no Errors popping up and, if so; Rectify them, etc.

*/

-- Defining our Stored Procedure:

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
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
        RAISE NOTICE 'Loading Bronze Layer:';
        RAISE NOTICE '=============================================';

        RAISE NOTICE '---------------- CRM TABLES ----------------';

        -- crm_cust_info
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_cust_info;
        COPY bronze.crm_cust_info
        FROM '/Users/alijazibrizvi/Documents/Data Analytics/SQL Data Warehouse Project - w Baraa Khatib/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER true);
        end_time := clock_timestamp();
        RAISE NOTICE 'crm_cust_info load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        -- crm_prod_info
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_prod_info;
        COPY bronze.crm_prod_info
        FROM '/Users/alijazibrizvi/Documents/Data Analytics/SQL Data Warehouse Project - w Baraa Khatib/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER true);
        end_time := clock_timestamp();
        RAISE NOTICE 'crm_prod_info load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        -- crm_sales_details
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.crm_sales_details;
        COPY bronze.crm_sales_details
        FROM '/Users/alijazibrizvi/Documents/Data Analytics/SQL Data Warehouse Project - w Baraa Khatib/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER true);
        end_time := clock_timestamp();
        RAISE NOTICE 'crm_sales_details load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        RAISE NOTICE '---------------- ERP TABLES ----------------';

        -- erp_loc_a101
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_loc_a101;
        COPY bronze.erp_loc_a101
        FROM '/Users/alijazibrizvi/Documents/Data Analytics/SQL Data Warehouse Project - w Baraa Khatib/source_erp/loc_a101.csv'
        WITH (FORMAT csv, HEADER true);
        end_time := clock_timestamp();
        RAISE NOTICE 'erp_loc_a101 load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        -- erp_cust_az12
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_cust_az12;
        COPY bronze.erp_cust_az12
        FROM '/Users/alijazibrizvi/Documents/Data Analytics/SQL Data Warehouse Project - w Baraa Khatib/source_erp/cust_az12.csv'
        WITH (FORMAT csv, HEADER true);
        end_time := clock_timestamp();
        RAISE NOTICE 'erp_cust_az12 load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        -- erp_px_cat_g1v2
        start_time := clock_timestamp();
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        COPY bronze.erp_px_cat_g1v2
        FROM '/Users/alijazibrizvi/Documents/Data Analytics/SQL Data Warehouse Project - w Baraa Khatib/source_erp/px_cat_g1v2.csv'
        WITH (FORMAT csv, HEADER true);
        end_time := clock_timestamp();
        RAISE NOTICE 'erp_px_cat_g1v2 load time: % seconds',
            EXTRACT(EPOCH FROM (end_time - start_time));

        batch_end_time := clock_timestamp();

        RAISE NOTICE '=============================================';
        RAISE NOTICE 'Total Bronze Load Duration: % seconds',
            EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
        RAISE NOTICE '=============================================';

    EXCEPTION WHEN OTHERS THEN  -- CATCH BLOCK
        RAISE NOTICE '❌ ERROR: An error occurred during loading Bronze layer';
        RAISE NOTICE 'Error message: %', SQLERRM;
        RAISE NOTICE '=============================================';
        RAISE NOTICE 'Bronze Load FAILED';
        RAISE NOTICE '=============================================';
    END;

END;
$$;

-- Calling and Executing our Stored Procedure!

CALL bronze.load_bronze(); -- "Run ETL"
