-- ===============================
-- Creating stored procedure 
-- ===============================

--EXEC silver.load_silver

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	-- ====================================================================
	-- Loading Data From bronze.crm_cust_info Into Silver.crm_cust_info
	-- ====================================================================

	DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		PRINT '==================================================';
		PRINT '               Loading Silver Layer               ';
		PRINT '==================================================';

		PRINT '--------------------------------------------------';
		PRINT '               Loading CRM Tables                 ';
		PRINT '--------------------------------------------------';

		-- Loading silver.crm_cust_info
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: Silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)
		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE 
			WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
			WHEN UPPER(cst_marital_status) = 'S' THEN 'Single'
			ELSE 'Unknown'
		END cst_marital_status, -- Normalize marital status values to readable format
		CASE 
			WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
			WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
			ELSE 'Unknown'
		END cst_gndr,-- Normalize gender status values to readable format
		cst_create_date
		FROM(
		SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		)t WHERE flag_last = 1
		

		-- ====================================================================
		-- Loading Data From bronze.crm_prd_info Into Silver.crm_prd_info
		-- ====================================================================

		-- Updating the DDL of silver schema table as per the requirement

		/* IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
		DROP TABLE silver.crm_prd_info;
		CREATE TABLE silver.crm_prd_info (
		prd_id INT,
		cat_id NVARCHAR(50),
		prd_key NVARCHAR(50),
		prd_nm NVARCHAR(50),
		prd_cost INT,
		prd_line NVARCHAR(50),
		prd_start_dt DATE,
		prd_end_dt DATE,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
		); */

		-- ===============================================================

		-- Now inserting into it
		PRINT '>> Truncating Table: silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)

		SELECT 
			prd_id,
			REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- Replace (-) with (_) for joining
			SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,-- Replace NULLs with 0s
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'Unknown'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
		FROM bronze.crm_prd_info


		/* WHERE SUBSTRING(prd_key,7,LEN(prd_key))  IN 
		(SELECT sls_prd_key FROM bronze.crm_sales_details) */

		/* WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN
		(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2) */


		-- Joining the table bronze.erp_px_cat_g1v2 and bronze.crm_prd_info
		-- ISSUE : Delimeters of (bronze.crm_prd_info) table contains (_) and (bronze.erp_px_cat_g1v2) table contains (-)

		-- SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2




		-- ==========================================================================
		-- Loading Data From bronze.crm_sales_details Into Silver.crm_sales_details
		-- ==========================================================================


		-- Checking the DDL as per the requirement 
		/*
		IF OBJECT_ID ('silver.crm_sales_details', 'U') IS NOT NULL
		DROP TABLE silver.crm_sales_details;
		CREATE TABLE silver.crm_sales_details (
		sls_ord_num NVARCHAR(50),
		sls_prd_key NVARCHAR(50),
		sls_cust_id INT,
		sls_order_dt DATE,
		sls_ship_dt DATE,
		sls_due_dt DATE,
		sls_sales INT,
		sls_quantity INT,
		sls_price INT,
		dwh_create_date DATETIME2 DEFAULT GETDATE()
		);
		*/
		PRINT '>> Truncating Table: silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
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
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE)
		END sls_order_dt,

		CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR)AS DATE)
		END sls_ship_dt,

		CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR)AS DATE)
		END sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_price != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <=0 
			THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price 
		END AS sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration Of CRM Tables: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)
		AS NVARCHAR)+' seconds'
		-- WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)

		-- WHERE sls_ord_num != TRIM(sls_ord_num) -- check unwanted spaces 

		-- According to business rule 
		-- Expectation : no result 
		/*
		SELECT sls_sales,
		sls_price,
		sls_quantity
		FROM silver.crm_sales_details
		WHERE sls_price != sls_sales * sls_quantity 
		*/


		-- ====================================================================
		-- Loading Data From bronze.erp_cust_az12 Into Silver.erp_cust_az12
		-- ====================================================================

		SET @start_time=GETDATE();
		PRINT '--------------------------------------------------';
		PRINT '               Loading ERP Tables                 ';
		PRINT '--------------------------------------------------';
		PRINT '>> Truncating Table: silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12'
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT
		CASE
			WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid
		END AS cid,
		CASE 
			WHEN bdate > GETDATE() THEN NULL 
			ELSE bdate
		END AS bdate,

		CASE 
			WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			ELSE 'Unknown'
		END AS gen
		FROM bronze.erp_cust_az12


		-- ====================================================================
		-- Loading Data From bronze.erp_loc_a101 Into Silver.erp_loc_a101
		-- ====================================================================


		PRINT '>> Truncating Table: silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE 
			WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
			ELSE TRIM(cntry)
		END AS cntry
		FROM bronze.erp_loc_a101

		-- ====================================================================
		-- Loading Data From bronze.erp_px_cat_g1v2 Into Silver.erp_px_cat_g1v2
		-- ====================================================================

		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2 (id,
			cat,
			subcat,
			maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration Of ERP Tables: ' + CAST(DATEDIFF(SECOND,@start_time,@end_time)
		AS NVARCHAR)+' seconds'
		SET @batch_end_time = SYSDATETIME();
		PRINT '==================================================';
		PRINT '>> Loading Silver Layer Completed Successfully';
		PRINT 'Load Duration Of Silver Layer: '
			+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR)
			+ ' seconds';
		PRINT '==================================================';
			END TRY 
			BEGIN CATCH 
				PRINT '============================================'
				PRINT 'ERROR OCCURED DURING LOADING Silver LAYER'
				PRINT 'Error Message: '+ERROR_MESSAGE();
				PRINT 'Error Message: '+CAST(ERROR_NUMBER() AS NVARCHAR);
				PRINT 'Error Message: '+CAST(ERROR_STATE() AS NVARCHAR);
				PRINT '============================================'
			END CATCH
END



