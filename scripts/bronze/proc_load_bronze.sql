CREATE OR REPLACE PROCEDURE bronze.load_all_data()
LANGUAGE plpgsql
AS $$
DECLARE
    start_ts TIMESTAMP;
    end_ts TIMESTAMP;
BEGIN
    RAISE NOTICE 'STARTING DATA LOAD at %', clock_timestamp();

    -- Step 1: Clear all tables
    DELETE FROM bronze.crm_sales_details;
    DELETE FROM bronze.crm_prd_info;
    DELETE FROM bronze.crm_cust_info;
    DELETE FROM bronze.erp_cust_az12;
    DELETE FROM bronze.erp_loc_a101;
    DELETE FROM bronze.erp_px_cat_g1v2;

    -- Optional log
    INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
    VALUES ('ALL TABLES', 'DATA CLEARED', clock_timestamp(), clock_timestamp());

    -- Step 2: Load Data from CSVs into each table

    -- CRM_CUST_INFO
    BEGIN
        start_ts := clock_timestamp();
        COPY bronze.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname,
            cst_marital_status, cst_gndr, cst_create_date
        )
        FROM 'D:/SQL-Projects/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        DELIMITER ',' CSV HEADER;
        end_ts := clock_timestamp();
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('crm_cust_info', 'DATA LOADED', start_ts, end_ts);
        RAISE NOTICE 'crm_cust_info loaded successfully at %', end_ts;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('crm_cust_info', 'FAILED', start_ts, clock_timestamp());
    END;

    -- CRM_PRD_INFO
    BEGIN
        start_ts := clock_timestamp();
        COPY bronze.crm_prd_info (
            prd_id, prd_key, prd_nm, prd_cost,
            prd_line, prd_start_dt, prd_end_dt
        )
        FROM 'D:/SQL-Projects/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        DELIMITER ',' CSV HEADER;
        end_ts := clock_timestamp();
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('crm_prd_info', 'DATA LOADED', start_ts, end_ts);
        RAISE NOTICE 'crm_prd_info loaded successfully at %', end_ts;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('crm_prd_info', 'FAILED', start_ts, clock_timestamp());
    END;

    -- CRM_SALES_DETAILS
    BEGIN
        start_ts := clock_timestamp();
        COPY bronze.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id,
            sls_order_dt, sls_ship_dt, sls_due_dt,
            sls_sales, sls_quantity, sls_price
        )
        FROM 'D:/SQL-Projects/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        DELIMITER ',' CSV HEADER;
        end_ts := clock_timestamp();
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('crm_sales_details', 'DATA LOADED', start_ts, end_ts);
        RAISE NOTICE 'crm_sales_details loaded successfully at %', end_ts;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('crm_sales_details', 'FAILED', start_ts, clock_timestamp());
    END;

    -- ERP_CUST_AZ12
    BEGIN
        start_ts := clock_timestamp();
        COPY bronze.erp_cust_az12 (cid, bdate, gen)
        FROM 'D:/SQL-Projects/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
        DELIMITER ',' CSV HEADER;
        end_ts := clock_timestamp();
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('erp_cust_az12', 'DATA LOADED', start_ts, end_ts);
        RAISE NOTICE 'erp_cust_az12 loaded successfully at %', end_ts;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('erp_cust_az12', 'FAILED', start_ts, clock_timestamp());
    END;

    -- ERP_LOC_A101
    BEGIN
        start_ts := clock_timestamp();
        COPY bronze.erp_loc_a101 (cid, cntry)
        FROM 'D:/SQL-Projects/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
        DELIMITER ',' CSV HEADER;
        end_ts := clock_timestamp();
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('erp_loc_a101', 'DATA LOADED', start_ts, end_ts);
        RAISE NOTICE 'erp_loc_a101 loaded successfully at %', end_ts;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('erp_loc_a101', 'FAILED', start_ts, clock_timestamp());
    END;

    -- ERP_PX_CAT_G1V2
    BEGIN
        start_ts := clock_timestamp();
        COPY bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
        FROM 'D:/SQL-Projects/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
        DELIMITER ',' CSV HEADER;
        end_ts := clock_timestamp();
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('erp_px_cat_g1v2', 'DATA LOADED', start_ts, end_ts);
        RAISE NOTICE 'erp_px_cat_g1v2 loaded successfully at %', end_ts;
    EXCEPTION WHEN OTHERS THEN
        INSERT INTO bronze.load_log(table_name, status, start_time, end_time)
        VALUES ('erp_px_cat_g1v2', 'FAILED', start_ts, clock_timestamp());
    END;

    RAISE NOTICE 'ALL DATA LOADED SUCCESSFULLY by %', clock_timestamp();
END;
$$;
