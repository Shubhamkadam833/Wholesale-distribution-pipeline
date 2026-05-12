/*
================================================================================
  FILE        : models/silver/slv_warehouse_stock.sql
  PURPOSE     : Silver layer cleaned and normalized warehouse stock snapshots
  LAYER       : Silver (cleansed, validated, business ready)
  DESCRIPTION : Reads the Bronze warehouse stock model, deduplicates to one
                row per warehouse and SKU per snapshot date, applies null
                handling and business rule normalization, computes available
                stock and stock coverage metrics, and flags records with
                data quality issues for downstream monitoring.
                The Silver layer is the single source of truth for all
                inventory queries. Gold models never read Bronze directly.
  MATERIALIZATION : incremental (Delta merge on warehouse_id, sku_id, date)
================================================================================
*/

{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'merge',
        unique_key          = ['warehouse_id', 'sku_id', 'stock_snapshot_date'],
        file_format         = 'delta',
        schema              = 'silver',
        tags                = ['silver', 'inventory', 'daily'],
        on_schema_change    = 'sync_all_columns'
    )
}}


WITH bronze_deduplicated AS (

    /*
    Keep only the most recent ingestion record per warehouse, SKU, and date.
    The _row_num column was assigned in the Bronze model using the same logic.
    */

    SELECT *
    FROM {{ ref('brz_warehouse_stock') }}
    WHERE _row_num = 1

    {% if is_incremental() %}
        AND stock_snapshot_date >= DATEADD(DAY, -3, CURRENT_DATE())
    {% endif %}

),

normalized AS (

    SELECT

        /* IDENTITY KEYS */
        UPPER(TRIM(warehouse_id))               AS warehouse_id,
        UPPER(TRIM(sku_id))                     AS sku_id,
        stock_snapshot_date,

        /* PRODUCT ATTRIBUTES */
        TRIM(product_name)                      AS product_name,
        UPPER(TRIM(
            COALESCE(category, 'UNCATEGORIZED')
        ))                                      AS category,
        UPPER(TRIM(supplier_id))                AS supplier_id,
        UPPER(TRIM(
            COALESCE(warehouse_zone, 'UNKNOWN')
        ))                                      AS warehouse_zone,

        /* STOCK QUANTITIES
           Null quantities from the WMS are treated as zero rather than NULL
           because a NULL in a quantity column means the data is missing,
           not that the actual stock is zero. We flag it separately below. */
        COALESCE(units_on_hand,    0)           AS units_on_hand,
        COALESCE(units_reserved,   0)           AS units_reserved,
        COALESCE(units_on_order,   0)           AS units_on_order,
        COALESCE(reorder_point,    0)           AS reorder_point,
        COALESCE(reorder_quantity, 0)           AS reorder_quantity,

        /* PRICING */
        COALESCE(unit_cost,  0.0)               AS unit_cost,
        COALESCE(unit_price, 0.0)               AS unit_price,

        /* DERIVED METRICS
           Available units are on hand minus reserved. Cannot go below zero
           because a negative available count indicates a fulfillment system
           error rather than a genuine overcommit state. */
        GREATEST(
            COALESCE(units_on_hand, 0) - COALESCE(units_reserved, 0),
            0
        )                                       AS units_available,

        /* Total inventory value at cost for the current snapshot */
        COALESCE(units_on_hand, 0) * COALESCE(unit_cost, 0.0)
                                                AS total_inventory_value,

        /* TIMESTAMP */
        last_counted_at,

        /* AUDIT METADATA */
        _ingestion_date,
        _ingested_at,
        _source_name,
        _pipeline_run_id,

        /* DATA QUALITY FLAGS
           These flags identify records that passed schema validation but have
           suspicious values that finance or supply chain teams should review.
           Flags do not exclude records from Silver. They are surfaced in Gold. */

        CASE
            WHEN units_on_hand IS NULL
                THEN TRUE
            ELSE FALSE
        END                                     AS dq_flag_null_units_on_hand,

        CASE
            WHEN unit_cost IS NULL OR unit_cost = 0
                THEN TRUE
            ELSE FALSE
        END                                     AS dq_flag_missing_cost,

        CASE
            WHEN reorder_point IS NULL OR reorder_point = 0
                THEN TRUE
            ELSE FALSE
        END                                     AS dq_flag_missing_reorder_point,

        CURRENT_TIMESTAMP()                     AS slv_updated_at

    FROM bronze_deduplicated

)

SELECT * FROM normalized
