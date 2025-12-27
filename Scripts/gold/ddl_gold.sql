/* =========================================================
   CUSTOMER DIMENSION VIEW
   Purpose:
   - Creates a conformed customer dimension for analytics
   - Combines CRM (master) and ERP data
   - Generates a surrogate customer key
   ========================================================= */

CREATE VIEW gold.dim_customers AS
SELECT 
    -- Surrogate key for the customer dimension
    ROW_NUMBER() OVER (ORDER BY cst_id) AS Customer_key,

    -- Business / natural keys from CRM
    ci.cst_id  AS customer_id,
    ci.cst_key AS customer_number,

    -- Customer personal details
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,

    -- Location information from ERP
    la.cntry AS country,

    -- Standardized marital status from CRM
    ci.cst_marital_status AS marital_status,

    -- Gender logic:
    -- CRM is considered the master source
    -- If CRM gender is Unknown, fallback to ERP data
    CASE 
        WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'Unknown')
    END AS gender,

    -- Additional customer attributes from ERP
    ca.bdate AS birthdate,

    -- Record creation date from CRM
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci

-- Join ERP customer attributes
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid

-- Join ERP location data
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO

/* =========================================================
   PRODUCT DIMENSION VIEW
   Purpose:
   - Creates product dimension with category hierarchy
   - Includes only active products
   - Generates a surrogate product key
   ========================================================= */

CREATE VIEW gold.dim_products AS
SELECT
    -- Surrogate key for the product dimension
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,

    -- Business / natural keys
    pn.prd_id  AS product_id,
    pn.prd_key AS product_number,

    -- Product descriptive attributes
    pn.prd_nm AS product_name,

    -- Category hierarchy
    pn.cat_id AS category_id,
    pc.cat    AS category,
    pc.subcat AS subcategory,
    pc.maintenance,

    -- Cost and classification details
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,

    -- Product lifecycle information
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn

-- Join ERP product category reference data
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id

-- Keep only active products (exclude historical records)
WHERE pn.prd_end_dt IS NULL;
GO
/* =========================================================
   SALES FACT VIEW
   Purpose:
   - Central fact table for sales analysis
   - Connects customers and products via surrogate keys
   - Stores transactional and measurable data
   ========================================================= */

CREATE VIEW gold.fact_sales AS 
SELECT
    -- Order identifiers
    sd.sls_ord_num AS order_number,

    -- Dimension surrogate keys
    pr.product_key,
    cu.Customer_key,

    -- Order lifecycle dates
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,

    -- Measures / facts
    sd.sls_sales   AS sales_amount,
    sd.sls_quantity,
    sd.sls_price   AS price
FROM silver.crm_sales_details sd

-- Join product dimension
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number

-- Join customer dimension
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
