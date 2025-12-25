/* ============================================================
   Script Name : Load Bronze Layer – CRM & ERP Data
   Purpose     : Load raw data from CSV files into Bronze schema
                 tables using BULK INSERT.
                 
                 This procedure:
                 - Truncates existing Bronze tables
                 - Loads data from source CSV files
                 - Captures load duration for each table
                 - Logs overall batch execution time

   Data Sources:
     - CRM system:
         • Customer Information
         • Product Information
         • Sales Details
     - ERP system:
         • Customer
         • Location
         • Product Category

   Notes :
     - This is the raw ingestion (Bronze) layer
     - No transformations or business logic applied
     - Data is loaded as-is from source files
     - Data cleansing and transformations will be
       handled in the Silver layer

  Usage Example:
    EXEC bronze.load_bronze;

   Author      : Anubhav Saxena
   Created On  : 2025-12-25
   ============================================================ */

-- Loading the data from CSV files into Bronze schema tables

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME,
            @end_time   DATETIME,
			@batch_start_time DATETIME,
			@batch_end_time DATETIME;
	
    BEGIN TRY
		SET @batch_start_time = GETDATE();
        PRINT '======================================';
        PRINT 'Loading Bronze Layer';
        PRINT '======================================';

        /* ================= CRM TABLES ================= */

        PRINT '--------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '--------------------------------------';

        /* CRM CUSTOMER INFO */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'D:\Data Analyst\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_cust_info): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ----------------------------------';

        /* CRM PRODUCT INFO */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'D:\Data Analyst\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_prd_info): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ----------------------------------';

        /* CRM SALES DETAILS */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'D:\Data Analyst\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_sales_details): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ----------------------------------';

        /* ================= ERP TABLES ================= */

        PRINT '--------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '--------------------------------------';

        /* ERP CUSTOMER */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'D:\Data Analyst\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_cust_az12): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ----------------------------------';

        /* ERP LOCATION */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'D:\Data Analyst\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_loc_a101): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ----------------------------------';

        /* ERP PRODUCT CATEGORY */
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'D:\Data Analyst\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_px_cat_g1v2): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> ----------------------------------';

		SET @batch_end_time = GETDATE()
		PRINT '======================================';
		PRINT '>> Loading Bronze Later is Completed';
		PRINT 'Total Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
        PRINT '======================================';

    END TRY
    BEGIN CATCH
        PRINT '===================================================';
        PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===================================================';
    END CATCH
	
END;
