/*
================================================================================
  FILE        : models/bronze/brz_shipping_events.sql
  PURPOSE     : Bronze layer model for raw shipping event records
  LAYER       : Bronze (raw ingestion pass through with audit columns)
  DESCRIPTION : Passes through raw shipping event records written by the
                PySpark ingestion script. Adds a row number for deduplication
                in the Silver layer and exposes the ingestion audit columns.
                No business logic or field transformation is applied here.
                The Silver layer is responsible for interpreting shipment
                status codes, computing delivery performance, and joining
                to product and warehouse dimension tables.
  MATERIALIZATION : incremental (Delta merge on shipment_id, order_id)
================================================================================
*/

{{
    config(
        materialized        = 'incremental',
        incremental_strategy = 'merge',
        unique_key          = ['shipment_id', 'order_id'],
        file_format         = 'delta',
        schema              = 'bronze',
        tags                = ['bronze', 'shipping', 'daily'],
        on_schema_change    = 'sync_all_columns'
    )
}}


WITH source AS (

    SELECT *
    FROM {{ source('fabric_bronze', 'raw_shipping_events') }}

    {% if is_incremental() %}
        WHERE _ingestion_date >= DATEADD(DAY, -3, CURRENT_DATE())
    {% endif %}

),

with_row_number AS (

    SELECT
        shipment_id,
        order_id,
        warehouse_id,
        customer_id,
        sku_id,
        units_shipped,
        carrier_id,
        service_level,
        promised_delivery,
        actual_delivery,
        ship_date,
        shipment_status,
        region,
        shipping_cost,
        event_timestamp,
        _ingestion_date,
        _ingested_at,
        _source_name,
        _pipeline_run_id,

        /*
        Row number within each shipment ordered by event timestamp descending.
        Shipping events are updated as the carrier scans the package at each
        hub. The Silver layer keeps only the most recent status row per
        shipment for the daily delivery performance calculation.
        */
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id
            ORDER BY event_timestamp DESC
        ) AS _row_num

    FROM source

)

SELECT * FROM with_row_number
