/*
================================================================================
  FILE        : models/gold/gld_low_stock_alerts.sql
  PURPOSE     : Gold layer low stock alert aggregations for supply chain reporting
  LAYER       : Gold (business ready, reporting optimized)
  DESCRIPTION : Reads the Silver warehouse stock model and produces a daily
                low stock alert table covering three alert tiers:
                  CRITICAL  : units available are zero or below reorder point
                              with no units on order. Immediate action required.
                  WARNING   : units available are at or below reorder point
                              but units on order exist. Monitor closely.
                  WATCH     : units available are within 20 percent above
                              reorder point. Plan replenishment soon.
                This table powers the supply chain operations dashboard in
                Microsoft Fabric Power BI and is the primary input for the
                automated purchase order suggestion report used by
                wholesale procurement teams.
  MATERIALIZATION : incremental (Delta merge on warehouse_id, sku_id, date)
================================================================================
*/

{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'merge',
        unique_key          = ['warehouse_id', 'sku_id', 'alert_date'],
        file_format         = 'delta',
        schema              = 'gold',
        tags                = ['gold', 'inventory', 'alerts', 'supply_chain'],
        on_schema_change    = 'sync_all_columns'
    )
}}


WITH silver_stock AS (

    SELECT *
    FROM {{ ref('slv_warehouse_stock') }}

    /*
    Only evaluate alerts for today's snapshot. Historical alert tiers
    are preserved from prior runs due to the incremental merge strategy.
    */
    {% if is_incremental() %}
        WHERE stock_snapshot_date >= DATEADD(DAY, -1, CURRENT_DATE())
    {% else %}
        WHERE stock_snapshot_date >= DATEADD(DAY, -90, CURRENT_DATE())
    {% endif %}

),

alert_classification AS (

    SELECT
        warehouse_id,
        sku_id,
        stock_snapshot_date                         AS alert_date,
        product_name,
        category,
        supplier_id,
        warehouse_zone,
        region,

        /* STOCK POSITION METRICS */
        units_on_hand,
        units_reserved,
        units_available,
        units_on_order,
        reorder_point,
        reorder_quantity,
        unit_cost,
        unit_price,
        total_inventory_value,

        /* ALERT TIER CLASSIFICATION
           The three tier system maps directly to the operations dashboard
           red, amber, and yellow RAG status indicators. */

        CASE

            /* CRITICAL: zero available stock with nothing incoming */
            WHEN units_available = 0
             AND units_on_order  = 0
                THEN 'CRITICAL'

            /* CRITICAL: available stock is below reorder point AND nothing is on order */
            WHEN units_available < reorder_point
             AND reorder_point   > 0
             AND units_on_order  = 0
                THEN 'CRITICAL'

            /* WARNING: below or at reorder point but replenishment is in progress */
            WHEN units_available <= reorder_point
             AND reorder_point   > 0
             AND units_on_order  > 0
                THEN 'WARNING'

            /* WATCH: within 20 percent above the reorder point */
            WHEN reorder_point > 0
             AND units_available > reorder_point
             AND units_available <= reorder_point * 1.20
                THEN 'WATCH'

            /* NO ALERT: sufficient stock */
            ELSE 'OK'

        END                                         AS alert_tier,

        /* RECOMMENDED REPLENISHMENT QUANTITY
           Suggests the reorder_quantity from the WMS if stock has dropped below
           reorder point. If no reorder_quantity is configured, suggests enough
           to bring the position up to 2x the reorder point as a safe default. */

        CASE
            WHEN units_available <= reorder_point AND reorder_point > 0
                THEN COALESCE(
                    NULLIF(reorder_quantity, 0),
                    GREATEST(reorder_point * 2 - units_available, 0)
                )
            ELSE 0
        END                                         AS suggested_reorder_qty,

        /* DAYS OF STOCK COVERAGE
           Estimated days until stockout based on average daily demand.
           Since we do not have sales velocity in this model, we use a
           proxy: units_on_hand divided by reorder_point as a relative
           coverage ratio. A proper velocity join is added in the
           full analytics model when sales history is available. */

        CASE
            WHEN reorder_point > 0
                THEN ROUND(units_available::FLOAT / reorder_point, 2)
            ELSE NULL
        END                                         AS stock_coverage_ratio,

        /* FINANCIAL EXPOSURE
           Value at risk if this SKU stocks out completely: potential lost revenue
           based on the unit price times the reorder quantity needed. */

        CASE
            WHEN units_available <= reorder_point AND reorder_point > 0
                THEN COALESCE(
                    NULLIF(reorder_quantity, 0),
                    reorder_point
                ) * unit_price
            ELSE 0
        END                                         AS revenue_at_risk,

        /* DATA QUALITY FLAGS PASSTHROUGH */
        dq_flag_null_units_on_hand,
        dq_flag_missing_cost,
        dq_flag_missing_reorder_point,

        CURRENT_TIMESTAMP()                         AS gld_updated_at

    FROM silver_stock

)

/*
Final filter: only include rows with an active alert tier or DQ flags.
OK records with no flags are excluded from the Gold alerts table to keep
the table focused on actionable items. The full stock snapshot is available
in the Silver layer for any query needing the complete picture.
*/

SELECT *
FROM alert_classification
WHERE
    alert_tier IN ('CRITICAL', 'WARNING', 'WATCH')
    OR dq_flag_null_units_on_hand   = TRUE
    OR dq_flag_missing_cost         = TRUE
    OR dq_flag_missing_reorder_point = TRUE
ORDER BY
    CASE alert_tier
        WHEN 'CRITICAL' THEN 1
        WHEN 'WARNING'  THEN 2
        WHEN 'WATCH'    THEN 3
        ELSE 4
    END,
    revenue_at_risk DESC
