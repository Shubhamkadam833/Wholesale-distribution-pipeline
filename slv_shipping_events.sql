/*
================================================================================
  FILE        : models/silver/slv_shipping_events.sql
  PURPOSE     : Silver layer cleaned and normalized shipping event records
  LAYER       : Silver (cleansed, validated, business ready)
  DESCRIPTION : Reads the Bronze shipping events model, deduplicates to one
                row per shipment using the most recent event timestamp,
                normalizes service level and status codes to a controlled
                vocabulary, computes delivery performance metrics, and flags
                data quality issues for monitoring.
                Delivery performance is the critical output of this model.
                The Gold next day delivery rate is built entirely from the
                delivery_met_promise column computed here.
  MATERIALIZATION : incremental (Delta merge on shipment_id)
================================================================================
*/

{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'merge',
        unique_key          = ['shipment_id'],
        file_format         = 'delta',
        schema              = 'silver',
        tags                = ['silver', 'shipping', 'daily'],
        on_schema_change    = 'sync_all_columns'
    )
}}


WITH bronze_deduplicated AS (

    /*
    Keep only the most recent event row per shipment.
    Carrier systems emit multiple status events per shipment (picked up,
    in transit, out for delivery, delivered). The Silver layer consolidates
    to one row per shipment reflecting the current known status.
    */

    SELECT *
    FROM {{ ref('brz_shipping_events') }}
    WHERE _row_num = 1

    {% if is_incremental() %}
        AND _ingestion_date >= DATEADD(DAY, -3, CURRENT_DATE())
    {% endif %}

),

normalized AS (

    SELECT

        /* IDENTITY KEYS */
        UPPER(TRIM(shipment_id))                AS shipment_id,
        UPPER(TRIM(order_id))                   AS order_id,
        UPPER(TRIM(warehouse_id))               AS warehouse_id,
        UPPER(TRIM(customer_id))                AS customer_id,
        UPPER(TRIM(sku_id))                     AS sku_id,
        UPPER(TRIM(carrier_id))                 AS carrier_id,
        UPPER(TRIM(region))                     AS region,

        /* SERVICE LEVEL NORMALIZATION
           WMS and carrier systems use different codes for the same service.
           Normalize all next day variants to NDD for consistent Gold grouping. */
        CASE
            WHEN UPPER(TRIM(service_level)) IN (
                'NDD', 'NEXT_DAY', 'NEXT DAY', '1D', 'NEXTDAY', 'OVERNIGHT'
            )                                       THEN 'NDD'
            WHEN UPPER(TRIM(service_level)) IN (
                '2D', 'TWO_DAY', 'TWO DAY', '2DAY'
            )                                       THEN '2D'
            WHEN UPPER(TRIM(service_level)) IN (
                'STD', 'STANDARD', 'GROUND', 'ECONOMY'
            )                                       THEN 'STANDARD'
            ELSE UPPER(TRIM(COALESCE(service_level, 'UNKNOWN')))
        END                                     AS service_level_normalized,

        /* SHIPMENT STATUS NORMALIZATION */
        CASE
            WHEN UPPER(TRIM(shipment_status)) IN (
                'DELIVERED', 'DEL', 'COMPLETE', 'COMPLETED'
            )                                       THEN 'DELIVERED'
            WHEN UPPER(TRIM(shipment_status)) IN (
                'IN_TRANSIT', 'INTRANSIT', 'IN TRANSIT', 'SHIPPED'
            )                                       THEN 'IN_TRANSIT'
            WHEN UPPER(TRIM(shipment_status)) IN (
                'PENDING', 'PROCESSING', 'LABEL_CREATED'
            )                                       THEN 'PENDING'
            WHEN UPPER(TRIM(shipment_status)) IN (
                'FAILED', 'EXCEPTION', 'RETURNED', 'LOST'
            )                                       THEN 'EXCEPTION'
            ELSE UPPER(TRIM(COALESCE(shipment_status, 'UNKNOWN')))
        END                                     AS shipment_status_normalized,

        /* QUANTITIES AND FINANCIALS */
        COALESCE(units_shipped, 0)              AS units_shipped,
        COALESCE(shipping_cost, 0.0)            AS shipping_cost,

        /* DATES */
        ship_date,
        promised_delivery,
        actual_delivery,
        event_timestamp,

        /* DELIVERY PERFORMANCE METRICS
           delivery_met_promise is the foundation for the Gold NDD success rate.
           A shipment meets its promise if it was actually delivered on or before
           the promised delivery date and has a DELIVERED status.
           NULL actual_delivery means the shipment has not been delivered yet,
           which is counted as not meeting the promise for in window calculations. */

        CASE
            WHEN UPPER(TRIM(shipment_status)) IN ('DELIVERED', 'DEL', 'COMPLETE', 'COMPLETED')
             AND actual_delivery IS NOT NULL
             AND actual_delivery <= promised_delivery
                THEN TRUE
            ELSE FALSE
        END                                     AS delivery_met_promise,

        CASE
            WHEN UPPER(TRIM(shipment_status)) IN ('DELIVERED', 'DEL', 'COMPLETE', 'COMPLETED')
             AND actual_delivery IS NOT NULL
                THEN TRUE
            ELSE FALSE
        END                                     AS is_delivered,

        /* Days late: positive means late, negative means early, zero means on time */
        CASE
            WHEN actual_delivery IS NOT NULL AND promised_delivery IS NOT NULL
                THEN DATEDIFF(DAY, promised_delivery, actual_delivery)
            ELSE NULL
        END                                     AS days_late,

        /* AUDIT METADATA */
        _ingestion_date,
        _ingested_at,
        _source_name,
        _pipeline_run_id,

        /* DATA QUALITY FLAGS */
        CASE
            WHEN promised_delivery IS NULL THEN TRUE
            ELSE FALSE
        END                                     AS dq_flag_missing_promised_delivery,

        CASE
            WHEN ship_date IS NULL THEN TRUE
            ELSE FALSE
        END                                     AS dq_flag_missing_ship_date,

        CASE
            WHEN units_shipped IS NULL OR units_shipped = 0 THEN TRUE
            ELSE FALSE
        END                                     AS dq_flag_zero_units_shipped,

        CURRENT_TIMESTAMP()                     AS slv_updated_at

    FROM bronze_deduplicated

)

SELECT * FROM normalized
