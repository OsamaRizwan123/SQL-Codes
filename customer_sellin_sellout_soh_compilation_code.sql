WITH hpc_fixed AS (
    SELECT 
        barcode,
        "E-Com Price" AS cost
    FROM hpc_all_masterfile_updated
    WHERE barcode IS NOT NULL AND UOM = 'EA' AND "E-Com Price" IS NOT NULL 
    GROUP BY 1, 2
    ORDER BY 1
),

region_mapping AS (
    SELECT 
        "TALABAT DARK STORE NAME" AS DARK_STORE_NAME,
        "PROPER DARK STORE NAME" AS PROPER_DARK_STORE_NAME,
        REGION
    FROM dark_store_mapping
),

sellin_data AS (
    SELECT * FROM VIEW_ALL_SELLIN_COST_SKU_LEVEL_MAPPING_TRANSFORMED_DATA
),

calendar AS (
    SELECT YEAR, MONTH_NO, MONTH_NAME, TRUNCATED_MONTH
    FROM calendar_table
    WHERE YEAR IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

cost_using_sellin AS (
    SELECT * FROM TABLE_ALL_HPC_SELLIN_COST_MAPPING_TRANSFORMED_DATA
),

noon_barcode_mapping_cte AS (
    SELECT NOON_SKU, BARCODE, "SKU NUMBER" AS PART_NUMBER 
    FROM VIEW_ALL_BARCODE_MAPPING_DATA 
    WHERE NOON_SKU IS NOT NULL AND BARCODE IS NOT NULL 
    GROUP BY 1, 2, 3
),

sellin_barcode_mapping_cte AS (
    SELECT NOON_SKU, BARCODE, "SKU NUMBER" AS PART_NUMBER 
    FROM VIEW_ALL_BARCODE_MAPPING_DATA 
    WHERE BARCODE IS NOT NULL AND "SKU NUMBER" IS NOT NULL 
    GROUP BY 1, 2, 3
),

TALABAT_HISTORIC_SELLOUT AS (
    SELECT 
        DATE(SUBSTR(A.DATE, 7, 4) || '-' || SUBSTR(A.DATE, 4, 2) || '-' || SUBSTR(A.DATE, 1, 2)) AS DATE, 
        CAST(A.SKUS AS TEXT) AS SKU,
        CAST(B."SKU NUMBER" AS TEXT) AS TRANSMED_SKU_ID,
        CAST(A.BARCODE AS TEXT) AS BARCODE,
        CAST(A.BARCODE AS TEXT) AS UNIQUE_IDENTIFIER,
        A.SUPPLIER,
        B."SKU NAME" AS PRODUCT_NAME,
        D."NEW BRAND" AS BRAND,
        D."NEW CATEGORY" AS CATEGORY,
        CAST(A.COST AS NUMERIC) AS COST,
        CAST(A.COST*A.QUANTITY AS NUMERIC) AS SALE_VALUE,
        CAST(NULL AS NUMERIC) AS SOH_VALUE,
        CAST(NULL AS NUMERIC) AS SELLIN_VALUE,
        CAST(A.QUANTITY AS NUMERIC) AS SALES_QUANTITY,
        CAST(NULL AS NUMERIC) AS SOH_QUANTITY,
        CAST(NULL AS NUMERIC) AS SELLIN_QUANTITY,
        CAST('Sell Out' AS TEXT) AS TYPE,
        CAST(R.PROPER_DARK_STORE_NAME AS TEXT) AS DARK_STORE_NAME,
        CAST(A.REGION AS TEXT) AS REGION,
        CAST(A.CUSTOMER AS TEXT) AS CUSTOMER
    FROM talabat_transmed_historic_sellout AS A 
    LEFT JOIN COST_USING_SELLIN AS B ON A.BARCODE = B.BARCODE
    LEFT JOIN brand_mapping_sql AS D ON B.BRAND = D."EXISTING BRAND"
    LEFT JOIN region_mapping R ON A."DARK STORE" = R.DARK_STORE_NAME
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
),

TALABAT_NEW_SELLOUT AS (
    SELECT 
        DATE(SUBSTR(A.DATE, 7, 4) || '-' || SUBSTR(A.DATE, 4, 2) || '-' || SUBSTR(A.DATE, 1, 2)) AS DATE,
        CAST(A.SKU AS TEXT) AS SKU,
        CAST(B."SKU NUMBER" AS TEXT) AS TRANSMED_SKU_ID,        
        CAST(A.BARCODE AS TEXT) AS BARCODE,
        CAST(A.BARCODE AS TEXT) AS UNIQUE_IDENTIFIER,
        B.SUPPLIER,
        B."Sku Name" AS PRODUCT_NAME,
        C."NEW BRAND" AS BRAND,
        C."NEW CATEGORY" AS CATEGORY,
        CAST(B.COST AS NUMERIC) AS COST,
        CAST(B.COST*A."SOLD QUANTITY" AS NUMERIC) AS SALE_VALUE,
        CAST(NULL AS NUMERIC) AS SOH_VALUE,
        CAST(NULL AS NUMERIC) AS SELLIN_VALUE,
        CAST(A."SOLD QUANTITY" AS NUMERIC) AS SALES_QUANTITY,
        CAST(NULL AS NUMERIC) AS SOH_QUANTITY,
        CAST(NULL AS NUMERIC) AS SELLIN_QUANTITY,
        CAST('Sell Out' AS TEXT) AS TYPE,
        CAST(D.PROPER_DARK_STORE_NAME AS TEXT) AS DARK_STORE_NAME,
        CAST(D.REGION AS TEXT) AS REGION,
        CAST('Talabat' AS TEXT) AS CUSTOMER
    FROM merge_talabat_sellout_data_unpivoted AS A
    LEFT JOIN cost_using_sellin B ON A.BARCODE = B.BARCODE
    LEFT JOIN brand_mapping_sql C ON B.BRAND = C."EXISTING BRAND"
    LEFT JOIN region_mapping D ON A."DARK STORE NAME" = D.DARK_STORE_NAME
    WHERE A.BRAND IN ("Always", "Ariel", "Aussie", "Clorox", "Crest", "Downy", "Duracell", "Fairy", "Gillette",
                    "Glad", "Head & Shoulders", "Herbal Essence", "Herbal Essences", "Olay", "Old Spice", "Pampers", "Pantene",
                    "Selpak", "Tampax", "Tide", "Venus", "Vicks")
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20
),

-- Additional CTEs follow the same format.

final_result AS (
    SELECT * 
    FROM TALABAT_HISTORIC_SELLOUT
    UNION ALL
    SELECT * 
    FROM TALABAT_NEW_SELLOUT
    UNION ALL
    SELECT * 
    FROM CAREEM_SELLOUT
    UNION ALL
    SELECT * 
    FROM NOON_SELLOUT
    -- Add other required UNIONs or join logic here.
)

SELECT * FROM final_result;
