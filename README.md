# Wholesale Distribution Inventory Pipeline

A production grade Microsoft Fabric Medallion architecture pipeline for
wholesale distribution operations. A PySpark ingestion script reads daily
warehouse stock snapshots and shipping event feeds from the OneLake landing
zone and writes them to Bronze layer Delta tables. Six dbt SQL models then
transform raw Bronze data through Silver cleaning and normalization into Gold
aggregations that power two operational outputs: a three tier low stock alert
system for procurement teams and a next day delivery success rate tracker for
logistics operations. All layers are built on Delta Lake with incremental merge
strategies so every daily run is fully idempotent and reprocessing is always safe.

---

## Business Problem

Wholesale distributors managing multiple warehouses face three operational
problems that manual spreadsheet processes cannot solve at scale:

**Stockouts are discovered too late.**
Warehouse managers notice a SKU is out of stock when a customer order cannot
be fulfilled, not in advance when a replenishment order could have been placed.
The pipeline produces a daily CRITICAL and WARNING alert table that gives
procurement teams a ranked list of SKUs requiring immediate or planned action
every morning before warehouse operations begin.

**Next day delivery failures are invisible until customers complain.**
Carrier performance data lives in separate portal logins for each carrier with
no consolidated view across warehouses and regions. Operations managers cannot
identify which carrier and warehouse combinations are consistently underperforming
on NDD commitments without manually pulling and joining multiple reports.
The pipeline consolidates all carrier event data into a single daily NDD success
rate table with a RED AMBER GREEN performance tier for every carrier, warehouse,
and region combination.

**There is no reliable audit trail for stock levels or delivery performance.**
When a dispute arises about whether a product was in stock on a specific date
or whether a carrier met their NDD SLA for a specific shipment, there is no
versioned historical record to query. The Delta Lake Medallion architecture
preserves full history at every layer so any historical state can be queried
with a date filter.

---

## Architecture

```
WMS Daily Export (CSV)            Carrier Event Feed (JSON)
        |                                   |
        v                                   v
  OneLake Landing Zone
  Files/landing/warehouse_stock   Files/landing/shipping_events
        |                                   |
        v                                   v
  [ PySpark Ingestion Script ]
  wholesale_ingestion.py
    Schema enforcement with explicit StructType
    Date partitioned folder read with fallback
    Ingestion metadata columns appended
    Intra batch deduplication via window function
    Bronze quality null rate checks
    Delta MERGE for idempotent writes
        |                                   |
        v                                   v
  BRONZE LAYER (Delta Lake)
  brz_warehouse_stock             brz_shipping_events
  Row number for Silver dedup     Row number for Silver dedup
        |                                   |
        v                                   v
  SILVER LAYER (Delta Lake)
  slv_warehouse_stock             slv_shipping_events
  Dedup to one row per SKU date   Dedup to latest event per shipment
  Null imputation and capping     Service level normalization
  Available units computed        Status code normalization
  DQ flags for bad records        delivery_met_promise computed
        |                                   |
        v                                   v
  GOLD LAYER (Delta Lake)
  gld_low_stock_alerts            gld_ndd_success_rates
  Three tier alert classification NDD success rate per carrier
  Suggested reorder quantity      On time rate all service levels
  Revenue at risk estimate        Exception rate and lateness avg
  Power BI supply chain dashboard Power BI logistics KPI dashboard
```

---

## Repository Structure

```
wholesale_distribution_pipeline/
  ingestion/
    wholesale_ingestion.py          PySpark Bronze ingestion script
  models/
    bronze/
      brz_warehouse_stock.sql       Bronze pass through for stock snapshots
      brz_shipping_events.sql       Bronze pass through for shipping events
      sources.yml                   dbt source declarations and freshness config
    silver/
      slv_warehouse_stock.sql       Silver cleaning, normalization, and DQ flags
      slv_shipping_events.sql       Silver normalization and delivery performance
    gold/
      gld_low_stock_alerts.sql      Gold low stock alert tier aggregation
      gld_ndd_success_rates.sql     Gold NDD delivery success rate by carrier
      schema.yml                    Column docs and dbt tests for Gold models
  dbt_project.yml                   dbt project configuration
  requirements.txt                  Python package dependencies
  fabric_environment.md             Fabric workspace setup reference
  .gitignore                        Excludes secrets and build artifacts
  README.md                         This file
```

---

## Medallion Architecture Layers

### Bronze Layer

The Bronze layer is a faithful copy of the source data enriched only with
ingestion audit columns. No business logic is applied. Every row written
by the PySpark script is preserved here, including records that Silver will
later flag as having data quality issues.

Bronze tables use Delta Lake MERGE on natural business keys so running the
PySpark script twice for the same date is always safe and never duplicates rows.

Four metadata columns are added to every Bronze record:

| Column | Purpose |
|--------|---------|
| `_ingestion_date` | Business date the record was ingested |
| `_ingested_at` | UTC timestamp of the PySpark write |
| `_source_name` | Feed identifier for lineage tracking |
| `_pipeline_run_id` | Run identifier for correlating logs |

### Silver Layer

The Silver layer is the single source of truth for cleaned, normalized, and
validated data. Gold models always read from Silver, never from Bronze directly.

Key transformations in Silver:

**Stock model:** Available units are computed as on hand minus reserved with
a floor of zero. Null quantities are imputed to zero with a DQ flag set so
the imputation is visible downstream. Warehouse zones and categories are
normalized to uppercase controlled vocabulary.

**Shipping model:** All carrier service level codes are mapped to a normalized
vocabulary (NDD, 2D, STANDARD). All status codes are mapped to a normalized
vocabulary (DELIVERED, IN TRANSIT, PENDING, EXCEPTION). The `delivery_met_promise`
boolean is computed here as the foundation for all Gold delivery metrics.

Both Silver models include three DQ flag columns that pass through to Gold
so supply chain analysts can filter out or identify records with data issues
without them being silently excluded.

### Gold Layer

The Gold layer contains only business metric aggregations optimized for
Power BI query performance. Two models are produced:

**`gld_low_stock_alerts`**

One row per warehouse, SKU, and alert date where an actionable condition exists.
Records with `alert_tier = OK` are excluded to keep the table focused.

| Alert Tier | Condition | Action Required |
|------------|-----------|-----------------|
| CRITICAL | Zero available and nothing on order | Expedite replenishment immediately |
| WARNING | At or below reorder point but on order | Monitor incoming shipment ETA |
| WATCH | Within 20% above reorder point | Plan next replenishment order |

**`gld_ndd_success_rates`**

One row per ship date, warehouse, carrier, and region combination.

| Performance Tier | NDD Success Rate | Meaning |
|------------------|-----------------|---------|
| GREEN | 95% and above | Carrier meeting SLA consistently |
| AMBER | 85% to 94% | Carrier at risk, review with account manager |
| RED | Below 85% | Carrier failing SLA, escalate contract review |

---

## PySpark Ingestion Details

The ingestion script handles four common failure modes in wholesale WMS exports:

**Late partition folders:** The script attempts to read from a dated subfolder
first and falls back to the base landing path if the date folder does not exist.
This handles WMS systems that do not always create date partitioned exports.

**Intra batch duplicates:** WMS systems sometimes emit duplicate stock records
when a count update fires twice during the export window. The script uses a
Spark window function with `ROW_NUMBER()` partitioned by natural keys and
ordered by timestamp to keep only the most recent record per duplicate group.

**Schema drift:** All CSV and JSON reads use explicit `StructType` schemas
rather than schema inference. When the WMS adds or renames a column, the
script will raise a clear error rather than silently loading the wrong values
into the wrong columns.

**Idempotent reruns:** Delta MERGE with natural business key conditions means
that rerunning the pipeline after a failure never creates duplicate rows.
Only new records are inserted and existing records are updated if their values
changed in the source.

---

## dbt Model Lineage

```
sources (Bronze Delta tables via PySpark)
    |
    brz_warehouse_stock        brz_shipping_events
    |                              |
    slv_warehouse_stock        slv_shipping_events
    |                              |
    gld_low_stock_alerts       gld_ndd_success_rates
```

All four non source models are incremental with a 3 to 7 day lookback window
to handle late arriving records without full historical reprocessing. A full
refresh is available via `dbt run --full-refresh` when needed after a schema
migration or historical correction.

---

## How This Pipeline Supports Supply Chain Logistics

**Procurement teams** use the `gld_low_stock_alerts` Power BI dashboard to
start each morning with a ranked list of SKUs requiring purchase orders. The
CRITICAL tier is sorted by revenue at risk so the highest value stockouts are
always at the top. The suggested reorder quantity removes the manual calculation
step from the buyer's workflow.

**Logistics operations teams** use the `gld_ndd_success_rates` dashboard to
hold carriers accountable to their NDD SLAs. The carrier performance scorecard
built from this table is used in quarterly carrier contract review meetings to
negotiate rates and escalate underperforming carriers to their account managers.

**Finance teams** use the total inventory value column in the Silver stock model
for daily working capital reporting and the revenue at risk column in the Gold
alerts model for exposure reporting to treasury.

**Wholesale account managers** use the regional delivery performance metrics to
proactively communicate with key wholesale customers in regions experiencing
carrier performance issues before those customers submit complaints.

---

## Quick Start

**Step 1: Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/wholesale-distribution-pipeline.git
cd wholesale-distribution-pipeline
```

**Step 2: Install dependencies**
```bash
pip install -r requirements.txt
```

**Step 3: Configure Fabric workspace**

Open `ingestion/wholesale_ingestion.py` and replace `YOUR_WORKSPACE_GUID_HERE`
and `YOUR_LAKEHOUSE_GUID_HERE` with your real Fabric GUIDs. See
`fabric_environment.md` for the full setup reference.

**Step 4: Upload ingestion script to Fabric**

Upload `wholesale_ingestion.py` as a Fabric Notebook or Spark Job Definition.
Attach your Lakehouse to the Notebook environment.

**Step 5: Configure dbt profile**

Create a `profiles.yml` pointing to your Fabric Spark endpoint as shown in
`fabric_environment.md`.

**Step 6: Run dbt models**
```bash
dbt deps
dbt run --select bronze
dbt run --select silver
dbt run --select gold
dbt test
```

**Step 7: Connect Power BI**

Connect Power BI to your Fabric Lakehouse and point your supply chain and
logistics dashboards at the Gold layer Delta tables.

---

## Monitoring

After each daily run, query these tables to verify pipeline health:

```sql
SELECT alert_tier, COUNT(*), SUM(revenue_at_risk)
FROM gold.gld_low_stock_alerts
WHERE alert_date = CURRENT_DATE()
GROUP BY alert_tier
ORDER BY 1;

SELECT carrier_id, warehouse_id, ndd_success_rate_pct, ndd_performance_tier
FROM gold.gld_ndd_success_rates
WHERE ship_date = CURRENT_DATE() - 1
  AND service_level_normalized = 'NDD'
ORDER BY ndd_success_rate_pct ASC;
```

---

*Portfolio sample for Data Engineering and Supply Chain Analytics job applications.
Demonstrates Microsoft Fabric, PySpark, Delta Lake Medallion architecture, dbt
multi layer transformations, and wholesale distribution domain knowledge.*
