/*
================================================================================
  FILE        : models/gold/gld_ndd_success_rates.sql
  PURPOSE     : Gold layer next day delivery success rate tracking
  LAYER       : Gold (business ready, reporting optimized)
  DESCRIPTION : Reads the Silver shipping events model and computes daily
                next day delivery (NDD) performance metrics aggregated by
                ship date, warehouse, carrier, and region.
                Key metrics produced:
                  ndd_success_rate    : Percentage of NDD shipments delivered
                                        on or before the promised delivery date
                  on_time_rate        : Percentage of all service levels
                                        delivered on or before promise
                  avg_days_late       : Average days late for late deliveries
                  exception_rate      : Percentage of shipments with exceptions
                This table powers the logistics KPI dashboard and the carrier
                performance scorecard used by the wholesale distribution
                operations team to negotiate carrier contracts.
  MATERIALIZATION : incremental (Delta merge on ship_date, warehouse, carrier)
================================================================================
*/

{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'merge',
        unique_key          = ['ship_date', 'warehouse_id', 'carrier_id', 'region', 'service_level_normalized'],
        file_format         = 'delta',
        schema              = 'gold',
        tags                = ['gold', 'shipping', 'ndd', 'logistics', 'kpi'],
        on_schema_change    = 'sync_all_columns'
    )
}}


WITH silver_shipping AS (

    SELECT *
    FROM {{ ref('slv_shipping_events') }}

    /*
    NDD scoring window covers the last 7 days to catch late deliveries that
    were shipped earlier but only scanned as delivered recently.
    */
    {% if is_incremental() %}
        WHERE _ingestion_date >= DATEADD(DAY, -7, CURRENT_DATE())
    {% else %}
        WHERE ship_date >= DATEADD(DAY, -90, CURRENT_DATE())
    {% endif %}

),

/*
Filter to shipments that have a valid ship date and promised delivery date.
Shipments missing these are counted in the dq_excluded_count to give
operations teams visibility into data completeness.
*/

valid_shipments AS (

    SELECT *
    FROM silver_shipping
    WHERE ship_date           IS NOT NULL
      AND promised_delivery   IS NOT NULL
      AND dq_flag_missing_promised_delivery = FALSE

),

excluded_shipments AS (

    SELECT
        ship_date,
        warehouse_id,
        carrier_id,
        region,
        service_level_normalized,
        COUNT(*)                                AS dq_excluded_count
    FROM silver_shipping
    WHERE ship_date           IS NULL
       OR promised_delivery   IS NULL
       OR dq_flag_missing_promised_delivery = TRUE
    GROUP BY
        ship_date,
        warehouse_id,
        carrier_id,
        region,
        service_level_normalized

),

aggregated AS (

    SELECT

        /* GRAIN KEYS */
        ship_date,
        warehouse_id,
        carrier_id,
        region,
        service_level_normalized,

        /* VOLUME METRICS */
        COUNT(*)                                            AS total_shipments,
        SUM(units_shipped)                                  AS total_units_shipped,
        SUM(shipping_cost)                                  AS total_shipping_cost,

        /* DELIVERY STATUS BREAKDOWN */
        SUM(CASE WHEN is_delivered THEN 1 ELSE 0 END)       AS delivered_count,
        SUM(CASE WHEN NOT is_delivered
                  AND shipment_status_normalized = 'IN_TRANSIT'
                 THEN 1 ELSE 0 END)                         AS in_transit_count,
        SUM(CASE WHEN shipment_status_normalized = 'EXCEPTION'
                 THEN 1 ELSE 0 END)                         AS exception_count,
        SUM(CASE WHEN shipment_status_normalized = 'PENDING'
                 THEN 1 ELSE 0 END)                         AS pending_count,

        /* ON TIME DELIVERY METRICS (all service levels) */
        SUM(CASE WHEN delivery_met_promise THEN 1 ELSE 0 END)
                                                            AS on_time_delivered_count,

        /* NDD SPECIFIC METRICS */
        SUM(CASE WHEN service_level_normalized = 'NDD'
                 THEN 1 ELSE 0 END)                         AS ndd_total,
        SUM(CASE WHEN service_level_normalized = 'NDD'
                  AND delivery_met_promise
                 THEN 1 ELSE 0 END)                         AS ndd_on_time_count,
        SUM(CASE WHEN service_level_normalized = 'NDD'
                  AND is_delivered
                  AND NOT delivery_met_promise
                 THEN 1 ELSE 0 END)                         AS ndd_late_count,
        SUM(CASE WHEN service_level_normalized = 'NDD'
                  AND shipment_status_normalized = 'EXCEPTION'
                 THEN 1 ELSE 0 END)                         AS ndd_exception_count,

        /* LATENESS METRICS */
        AVG(CASE WHEN days_late > 0 THEN days_late END)     AS avg_days_late,
        MAX(days_late)                                       AS max_days_late,

        /* AVERAGE COST PER SHIPMENT */
        CASE
            WHEN COUNT(*) > 0
                THEN SUM(shipping_cost) / COUNT(*)
            ELSE 0
        END                                                 AS avg_shipping_cost

    FROM valid_shipments
    GROUP BY
        ship_date,
        warehouse_id,
        carrier_id,
        region,
        service_level_normalized

),

with_rates AS (

    SELECT
        a.*,

        /* NDD SUCCESS RATE
           Primary KPI for the logistics operations dashboard.
           Defined as NDD shipments delivered on or before promised date
           divided by all NDD shipments (including exceptions and in transit).
           In transit NDD shipments count against the rate because the delivery
           window has passed for any shipment older than 1 day. */

        CASE
            WHEN a.ndd_total > 0
                THEN ROUND(
                    100.0 * a.ndd_on_time_count / a.ndd_total,
                2)
            ELSE NULL
        END                                                 AS ndd_success_rate_pct,

        /* OVERALL ON TIME RATE across all service levels */
        CASE
            WHEN a.total_shipments > 0
                THEN ROUND(
                    100.0 * a.on_time_delivered_count / a.total_shipments,
                2)
            ELSE NULL
        END                                                 AS on_time_rate_pct,

        /* EXCEPTION RATE */
        CASE
            WHEN a.total_shipments > 0
                THEN ROUND(
                    100.0 * a.exception_count / a.total_shipments,
                2)
            ELSE NULL
        END                                                 AS exception_rate_pct,

        /* DELIVERY RATE (how many have been delivered vs still in flight) */
        CASE
            WHEN a.total_shipments > 0
                THEN ROUND(
                    100.0 * a.delivered_count / a.total_shipments,
                2)
            ELSE NULL
        END                                                 AS delivery_rate_pct,

        /* PERFORMANCE TIER
           Operational RAG rating for this carrier and warehouse combination
           on this ship date. Used directly in the Power BI conditional formatting. */
        CASE
            WHEN a.ndd_total = 0
                THEN 'NO_NDD'
            WHEN ROUND(100.0 * a.ndd_on_time_count / NULLIF(a.ndd_total, 0), 2) >= 95.0
                THEN 'GREEN'
            WHEN ROUND(100.0 * a.ndd_on_time_count / NULLIF(a.ndd_total, 0), 2) >= 85.0
                THEN 'AMBER'
            ELSE 'RED'
        END                                                 AS ndd_performance_tier,

        CURRENT_TIMESTAMP()                                 AS gld_updated_at

    FROM aggregated a

),

final AS (

    SELECT
        w.*,
        COALESCE(e.dq_excluded_count, 0)                    AS dq_excluded_shipment_count
    FROM with_rates w
    LEFT JOIN excluded_shipments e
        ON  w.ship_date               = e.ship_date
        AND w.warehouse_id            = e.warehouse_id
        AND w.carrier_id              = e.carrier_id
        AND w.region                  = e.region
        AND w.service_level_normalized = e.service_level_normalized

)

SELECT * FROM final
ORDER BY ship_date DESC, ndd_success_rate_pct ASC
