/*
================================================================================
  FILE        : models/bronze/brz_warehouse_stock.sql
  PURPOSE     : Bronze layer model for raw warehouse stock snapshots
  LAYER       : Bronze (raw ingestion pass through with audit columns)
  DESCRIPTION : Passes through the raw warehouse stock records written by the
                PySpark ingestion script with light filtering only. The Bronze
                layer never transforms business logic. Its sole responsibility
                is to make the raw Delta Lake data accessible to dbt lineage
                and to add a row number for downstream deduplication in Silver.
                Any cleaning, normalization, or business rule application
                happens in the Silver layer, not here.
  MATERIALIZATION : incremental (Delta merge on warehouse_id, sku_id, date)
================================================================================
*/

{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'merge',
        unique_key          = ['warehouse_id', 'sku_id', 'stock_snapshot_date'],
        file_format         = 'delta',
        schema              = 'bronze',
        tags                = ['bronze', 'inventory', 'daily'],
        on_schema_change    = 'sync_all_columns'
    )
}}


WITH source AS (

    /*
    Read directly from the Delta table written by the PySpark ingestion script.
    The source() macro connects this to the sources.yml freshness declaration
    so dbt warns if the PySpark job did not run before the dbt job starts.
    */

    SELECT *
    FROM {{ source('fabric_bronze', 'raw_warehouse_stock') }}

    /*
    On incremental runs only process records from the last 3 days to capture
    any late arriving or corrected stock counts from the WMS without
    reprocessing the full historical table on every daily run.
    */

    {% if is_incremental() %}
        WHERE _ingestion_date >= DATEADD(DAY, -3, CURRENT_DATE())
    {% endif %}

),

with_row_number AS (

    SELECT
        warehouse_id,
        sku_id,
        product_name,
        category,
        supplier_id,
        units_on_hand,
        units_reserved,
        units_on_order,
        reorder_point,
        reorder_quantity,
        unit_cost,
        unit_price,
        warehouse_zone,
        last_counted_at,
        stock_snapshot_date,
        _ingestion_date,
        _ingested_at,
        _source_name,
        _pipeline_run_id,

        /*
        Row number within each warehouse and SKU partition ordered by ingestion
        timestamp. Silver layer uses this to keep only the most recent snapshot
        when multiple corrections arrive for the same SKU in the same day.
        */
        ROW_NUMBER() OVER (
            PARTITION BY warehouse_id, sku_id, stock_snapshot_date
            ORDER BY _ingested_at DESC
        ) AS _row_num

    FROM source

)

SELECT * FROM with_row_number
