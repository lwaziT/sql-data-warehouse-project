/*
This script creates tables in the 'Bronze' schema, dropping existing tables.
Run this script to re-define the DDL structure of 'Bronze' tables.
*/

if OBJECT_ID ('bronze.crm_cust_info', 'U') is not null
	  drop table bronze.crm_cust_info;
GO
create table bronze.crm_cust_info (
	cst_id				int,
	cst_key				nvarchar(50),
	cst_firstname		nvarchar(50),
	cst_lastname		nvarchar(50),
	cst_marital_status	nvarchar(50),
	cst_gndr			nvarchar(50),
	cst_create_date		datetime
);
GO
  
if OBJECT_ID ('bronze.crm_prd_info', 'U') is not null
	  drop table bronze.crm_prd_info;
GO
create table bronze.crm_prd_info (
	prd_id			int,
	prd_key			nvarchar(50),
	prd_nm			nvarchar(50),
	prd_cost		int,
	prd_line		nvarchar(50),
	prd_start_dt	datetime,
	prd_end_dt		datetime
);
GO
if OBJECT_ID ('bronze.crm_sales_details', 'U') is not null
	  drop table bronze.crm_sales_details;
GO
create table bronze.crm_sales_details (
	sls_prd_num		nvarchar(50),
	sls_prd_key		nvarchar(50),
	sls_cust_id		int,
	sls_order_dt	int,
	sls_ship_dt		int,
	sls_due_dt		int,
	sls_sales		int,
	sls_quantity	int,
	sls_price		int
);
GO
if OBJECT_ID ('bronze.erp_custaz12', 'U') is not null
	  drop table bronze.erp_custaz12;
GO
create table bronze.erp_custaz12 (
	cid				nvarchar(50),
	bdate			date,
	gen				nvarchar(50)
);
GO
if OBJECT_ID ('bronze.erp_loc_ac101', 'U') is not null
	  drop table bronze.erp_loc_ac101;
GO
create table bronze.erp_loc_ac101 (
	cid				nvarchar(50),
	cntry			nvarchar(50)
);
GO
if OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') is not null
	  drop table bronze.erp_px_cat_g1v2;
GO
create table bronze.erp_px_cat_g1v2 (
	id				nvarchar(50),
	cat				nvarchar(50),
	subcat			nvarchar(50),
	mainatanace		nvarchar(50)
);



