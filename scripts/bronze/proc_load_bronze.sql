/*
====================================================================================
Stored Procedure
====================================================================================
Script purpose:
This stored procedure loads data into the 'bronze' schema from external csv files.
It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the 'BULK INSERT' commant to load data from csv files to bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage example:
EXEC bronze.load_bronze
=====================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time =	GETDATE();
		PRINT '============================';
		PRINT 'Loading Bronze Layer';
		PRINT '============================';

		PRINT '----------------------------';
		PRINT 'Loading CRM tables';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_cust_info ';
		truncate table bronze.crm_cust_info

		PRINT '>> Inserting data into table: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\tobos\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '-------------------------------------------------';
		PRINT '>> Load time: ' + CAST( DATEDIFF( second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_prd_info ';
		truncate table bronze.crm_prd_info

		PRINT '>> Inserting data into table:  bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info

		from 'C:\Users\tobos\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '-------------------------------------------------';
		PRINT '>> Load time: ' + CAST( DATEDIFF( second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.crm_sales_details ';
		truncate table bronze.crm_sales_details

		PRINT '>> Inserting data into table: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details

		from 'C:\Users\tobos\Desktop\Projects\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '-------------------------------------------------';
		PRINT '>> Load time: ' + CAST( DATEDIFF( second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		PRINT '----------------------------';
		PRINT 'Loading ERP tables';
		PRINT '----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_custaz12 ';
		truncate table bronze.erp_custaz12

		PRINT '>> Inserting data into table: bronze.erp_custaz12';
		bulk insert bronze.erp_custaz12

		from 'C:\Users\tobos\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '-------------------------------------------------';
		PRINT '>> Load time: ' + CAST( DATEDIFF( second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_loc_ac101 ';
		truncate table bronze.erp_loc_ac101

		PRINT '>> Inserting data into table: bronze.erp_loc_ac101';
		bulk insert bronze.erp_loc_ac101

		from 'C:\Users\tobos\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '-------------------------------------------------';
		PRINT '>> Load time: ' + CAST( DATEDIFF( second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating table: bronze.erp_px_cat_g1v2 ';
		truncate table bronze.erp_px_cat_g1v2

		PRINT '>> Inserting data into table: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2

		from 'C:\Users\tobos\Desktop\Projects\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		SET @end_time = GETDATE();
		PRINT '-------------------------------------------------';
		PRINT '>> Load time: ' + CAST( DATEDIFF( second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '-------------------------------------------------';
		
		SET @batch_end_time = GETDATE();
		PRINT '======================================';
		PRINT 'Loading Bronze Layer is completed!';
		PRINT '  - Total load time: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
		PRINT '======================================';
	END TRY
	BEGIN CATCH
		PRINT '===========================================';
		PRINT 'SOME ERROR OCCURED: ' + ERROR_MESSAGE();
		PRINT '===========================================';
	END CATCH
END
