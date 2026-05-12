"""
================================================================================
  FILE        : ingestion/wholesale_ingestion.py
  PURPOSE     : Daily Wholesale Distribution Inventory and Shipping Ingestion
  DESCRIPTION : PySpark script that ingests daily warehouse stock snapshots and
                shipping event records into the Microsoft Fabric Lakehouse
                Bronze layer. Reads raw CSV and JSON feeds from the Fabric
                OneLake landing zone, applies schema enforcement and partition
                tagging, deduplicates within the daily batch, and writes
                Delta Lake tables to the Bronze layer.
                This script runs as a Fabric Notebook or Spark Job Definition
                on a daily schedule after warehouse management system exports
                land in the OneLake Files section.
  AUTHOR      : Data Engineering Portfolio Sample
  RUNTIME     : Microsoft Fabric Spark Runtime 1.2 (Spark 3.4, Delta 2.4)
  USAGE       : Submit as a Fabric Spark Job Definition or run inside a
                Fabric Notebook with the workspace Lakehouse attached.
================================================================================
"""

import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable


# ==============================================================================
# LOGGING SETUP
# ==============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
)
log = logging.getLogger(__name__)


# ==============================================================================
# SPARK SESSION
# In Microsoft Fabric, the SparkSession is pre initialized in Notebooks.
# When running as a Spark Job Definition, this block creates or retrieves it.
# Delta Lake is the default table format in Fabric Lakehouses.
# ==============================================================================

spark = (
    SparkSession.builder
    .appName("WholesaleDistributionIngestion")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

log.info("SPARK  Session initialized for Wholesale Distribution Ingestion")


# ==============================================================================
# CONFIGURATION
# All paths follow the Microsoft Fabric OneLake ABFS path convention.
# Replace WORKSPACE_ID and LAKEHOUSE_ID with your actual Fabric GUIDs.
# In production, read these from a Fabric environment variable or a config table.
# ==============================================================================

WORKSPACE_ID        = "YOUR_WORKSPACE_GUID_HERE"
LAKEHOUSE_ID        = "YOUR_LAKEHOUSE_GUID_HERE"

ONELAKE_BASE        = f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com/{LAKEHOUSE_ID}"

LANDING_BASE        = f"{ONELAKE_BASE}/Files/landing"
BRONZE_BASE         = f"{ONELAKE_BASE}/Tables/bronze"

STOCK_LANDING_PATH  = f"{LANDING_BASE}/warehouse_stock"
SHIP_LANDING_PATH   = f"{LANDING_BASE}/shipping_events"

BRONZE_STOCK_TABLE  = f"{BRONZE_BASE}/raw_warehouse_stock"
BRONZE_SHIP_TABLE   = f"{BRONZE_BASE}/raw_shipping_events"

PROCESS_DATE        = datetime.now(timezone.utc).strftime("%Y-%m-%d")


# ==============================================================================
# SCHEMA DEFINITIONS
# Explicit schemas prevent Spark from inferring wrong types on schema drift.
# The source WMS system sometimes exports numeric SKU IDs as strings or
# omits optional decimal places on unit costs. Explicit schemas catch this.
# ==============================================================================

STOCK_SCHEMA = T.StructType([
    T.StructField("warehouse_id",       T.StringType(),     nullable=False),
    T.StructField("sku_id",             T.StringType(),     nullable=False),
    T.StructField("product_name",       T.StringType(),     nullable=True),
    T.StructField("category",           T.StringType(),     nullable=True),
    T.StructField("supplier_id",        T.StringType(),     nullable=True),
    T.StructField("units_on_hand",      T.LongType(),       nullable=True),
    T.StructField("units_reserved",     T.LongType(),       nullable=True),
    T.StructField("units_on_order",     T.LongType(),       nullable=True),
    T.StructField("reorder_point",      T.LongType(),       nullable=True),
    T.StructField("reorder_quantity",   T.LongType(),       nullable=True),
    T.StructField("unit_cost",          T.DecimalType(12,4),nullable=True),
    T.StructField("unit_price",         T.DecimalType(12,4),nullable=True),
    T.StructField("warehouse_zone",     T.StringType(),     nullable=True),
    T.StructField("last_counted_at",    T.TimestampType(),  nullable=True),
    T.StructField("stock_snapshot_date",T.DateType(),       nullable=False),
])

SHIPPING_SCHEMA = T.StructType([
    T.StructField("shipment_id",        T.StringType(),     nullable=False),
    T.StructField("order_id",           T.StringType(),     nullable=False),
    T.StructField("warehouse_id",       T.StringType(),     nullable=False),
    T.StructField("customer_id",        T.StringType(),     nullable=True),
    T.StructField("sku_id",             T.StringType(),     nullable=False),
    T.StructField("units_shipped",      T.LongType(),       nullable=True),
    T.StructField("carrier_id",         T.StringType(),     nullable=True),
    T.StructField("service_level",      T.StringType(),     nullable=True),
    T.StructField("promised_delivery",  T.DateType(),       nullable=True),
    T.StructField("actual_delivery",    T.DateType(),       nullable=True),
    T.StructField("ship_date",          T.DateType(),       nullable=True),
    T.StructField("shipment_status",    T.StringType(),     nullable=True),
    T.StructField("region",             T.StringType(),     nullable=True),
    T.StructField("shipping_cost",      T.DecimalType(12,4),nullable=True),
    T.StructField("event_timestamp",    T.TimestampType(),  nullable=False),
])


# ==============================================================================
# SECTION 1: INGESTION HELPER FUNCTIONS
# ==============================================================================

def read_landing_files(
    landing_path: str,
    schema: T.StructType,
    file_format: str = "csv",
    date_filter: str = PROCESS_DATE,
) -> "pyspark.sql.DataFrame":
    """
    Read source files from the OneLake landing zone for today's process date.

    Reads from a date partitioned folder structure where upstream WMS exports
    land files in subdirectories named by date (e.g. landing/warehouse_stock/2024-01-15/).
    Falls back to the base path if no date partition folder exists.

    Parameters
    ----------
    landing_path : str
        Base ABFS path to the landing zone folder for this feed.
    schema : StructType
        Explicit Spark schema to enforce on read.
    file_format : str
        Source file format. Either 'csv' or 'json'.
    date_filter : str
        ISO format date string used to build the partition subfolder path.

    Returns
    -------
    pyspark.sql.DataFrame
        Raw DataFrame read from the landing zone with the applied schema.
    """

    dated_path = f"{landing_path}/{date_filter}"

    log.info(f"READ  Attempting to read {file_format.upper()} from: {dated_path}")

    try:
        if file_format == "csv":
            df = (
                spark.read
                .option("header", "true")
                .option("nullValue", "")
                .option("emptyValue", "")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .option("dateFormat", "yyyy-MM-dd")
                .schema(schema)
                .csv(dated_path)
            )
        elif file_format == "json":
            df = (
                spark.read
                .option("multiLine", "false")
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .schema(schema)
                .json(dated_path)
            )
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        count = df.count()
        log.info(f"READ  Successfully read {count} rows from {dated_path}")
        return df

    except AnalysisException as exc:
        log.warning(
            f"READ  Date partitioned path not found: {dated_path}. "
            f"Falling back to base path: {landing_path}. Error: {exc}"
        )
        if file_format == "csv":
            return (
                spark.read
                .option("header", "true")
                .option("nullValue", "")
                .schema(schema)
                .csv(landing_path)
            )
        return (
            spark.read
            .option("multiLine", "false")
            .schema(schema)
            .json(landing_path)
        )


def add_ingestion_metadata(df: "pyspark.sql.DataFrame", source_name: str) -> "pyspark.sql.DataFrame":
    """
    Append Bronze layer metadata columns to the DataFrame before writing.

    These columns are added to every Bronze table so downstream Silver models
    can filter by ingestion date, detect late arriving records, and trace
    every row back to its source file and ingestion run.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The raw DataFrame read from the landing zone.
    source_name : str
        Short name identifying the source feed (e.g. 'warehouse_stock').

    Returns
    -------
    pyspark.sql.DataFrame
        DataFrame with four additional metadata columns appended.
    """

    return df.withColumns({
        "_ingestion_date"     : F.lit(PROCESS_DATE).cast(T.DateType()),
        "_ingested_at"        : F.current_timestamp(),
        "_source_name"        : F.lit(source_name),
        "_pipeline_run_id"    : F.lit(f"run_{PROCESS_DATE.replace('-', '')}"),
    })


def deduplicate_batch(
    df: "pyspark.sql.DataFrame",
    dedup_keys: list,
    order_by_col: str,
) -> "pyspark.sql.DataFrame":
    """
    Remove duplicate rows within the current batch using a window function.

    In wholesale distribution feeds, the WMS sometimes emits duplicate stock
    snapshot rows for the same SKU and warehouse if a stock count update fires
    twice during the export window. This function keeps the most recent row
    per deduplication key based on a timestamp ordering column.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        Incoming raw DataFrame.
    dedup_keys : list
        Column names that define a unique record within the batch.
    order_by_col : str
        Column used to determine which duplicate row to keep (most recent).

    Returns
    -------
    pyspark.sql.DataFrame
        Deduplicated DataFrame with one row per unique key combination.
    """

    from pyspark.sql.window import Window

    log.info(
        f"DEDUP  Deduplicating on keys: {dedup_keys} ordered by: {order_by_col}"
    )

    window_spec = (
        Window
        .partitionBy([F.col(k) for k in dedup_keys])
        .orderBy(F.col(order_by_col).desc())
    )

    before_count = df.count()

    deduped_df = (
        df
        .withColumn("_row_rank", F.row_number().over(window_spec))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank")
    )

    after_count = deduped_df.count()
    removed     = before_count - after_count

    if removed > 0:
        log.warning(
            f"DEDUP  Removed {removed} duplicate rows from batch "
            f"({before_count} before, {after_count} after)"
        )
    else:
        log.info(f"DEDUP  No duplicates found. All {after_count} rows are unique.")

    return deduped_df


# ==============================================================================
# SECTION 2: BRONZE WRITE FUNCTIONS
# Uses Delta Lake MERGE (upsert) to write Bronze tables idempotently.
# Re running the pipeline for the same date will update existing rows rather
# than appending duplicates, which is critical for daily batch reruns.
# ==============================================================================

def write_bronze_delta(
    df: "pyspark.sql.DataFrame",
    target_path: str,
    merge_keys: list,
    partition_col: str = "_ingestion_date",
) -> None:
    """
    Write a DataFrame to a Bronze Delta table using MERGE for idempotency.

    If the Delta table does not yet exist (first ever run), a full write
    creates it. On subsequent runs, records are merged on the provided keys
    so that re processing the same day's feed never creates duplicate rows.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The enriched and deduplicated DataFrame to persist.
    target_path : str
        ABFS path to the Delta table in the Bronze Lakehouse layer.
    merge_keys : list
        Column names that uniquely identify a row for the MERGE condition.
    partition_col : str
        Column to use for Delta table partitioning. Defaults to ingestion date.
    """

    log.info(f"WRITE BRONZE  Target: {target_path}")

    try:
        delta_table = DeltaTable.forPath(spark, target_path)

        merge_condition = " AND ".join(
            [f"target.{k} = source.{k}" for k in merge_keys]
        )

        (
            delta_table.alias("target")
            .merge(df.alias("source"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        log.info(
            f"WRITE BRONZE  MERGE complete on {len(merge_keys)} key(s) "
            f"into {target_path}"
        )

    except AnalysisException:
        log.info(
            f"WRITE BRONZE  Delta table does not exist yet. "
            f"Creating with full write at {target_path}"
        )
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy(partition_col)
            .option("overwriteSchema", "true")
            .save(target_path)
        )
        log.info(f"WRITE BRONZE  Initial Delta table created at {target_path}")


# ==============================================================================
# SECTION 3: DATA QUALITY CHECKS
# Run lightweight checks on the Bronze output before signaling success.
# These are not blocking checks but they emit warnings that Fabric pipeline
# monitoring and alerting can pick up from the Spark logs.
# ==============================================================================

def run_bronze_quality_checks(
    df: "pyspark.sql.DataFrame",
    table_name: str,
    required_cols: list,
) -> dict:
    """
    Run basic quality checks on a Bronze DataFrame and return a report.

    Checks null rates on required columns and flags any that exceed 5 percent.
    Also confirms the DataFrame is non empty.

    Parameters
    ----------
    df : pyspark.sql.DataFrame
        The Bronze DataFrame to check.
    table_name : str
        Name of the table for logging context.
    required_cols : list
        Columns that should have no nulls (primary keys and business keys).

    Returns
    -------
    dict
        Quality report with null rates and row count.
    """

    log.info(f"QUALITY  Running Bronze quality checks on {table_name}")

    total_rows = df.count()
    report     = {"table": table_name, "total_rows": total_rows, "issues": []}

    if total_rows == 0:
        msg = f"QUALITY WARNING  {table_name} has zero rows. Check landing zone."
        log.warning(msg)
        report["issues"].append(msg)
        return report

    for col_name in required_cols:
        if col_name not in df.columns:
            continue
        null_count = df.filter(F.col(col_name).isNull()).count()
        null_rate  = null_count / total_rows

        if null_rate > 0.05:
            msg = (
                f"QUALITY WARNING  Column '{col_name}' in {table_name} has "
                f"{null_rate:.1%} null rate ({null_count} of {total_rows} rows)"
            )
            log.warning(msg)
            report["issues"].append(msg)
        else:
            log.info(
                f"QUALITY  Column '{col_name}': null rate {null_rate:.2%} is acceptable"
            )

    log.info(
        f"QUALITY  {table_name} check complete. "
        f"Rows: {total_rows} | Issues: {len(report['issues'])}"
    )
    return report


# ==============================================================================
# SECTION 4: MAIN PIPELINE ORCHESTRATION
# ==============================================================================

def run_stock_ingestion() -> dict:
    """
    Ingest today's warehouse stock snapshot from the landing zone to Bronze.

    Reads the daily WMS stock export, enforces the stock schema, appends
    ingestion metadata, deduplicates within the batch, runs quality checks,
    and merges into the Bronze raw_warehouse_stock Delta table.

    Returns
    -------
    dict
        Ingestion summary with row counts and quality check results.
    """

    log.info("STOCK INGESTION  Starting warehouse stock snapshot ingestion")

    raw_df = read_landing_files(
        landing_path = STOCK_LANDING_PATH,
        schema       = STOCK_SCHEMA,
        file_format  = "csv",
    )

    enriched_df = add_ingestion_metadata(raw_df, source_name="warehouse_stock")

    deduped_df = deduplicate_batch(
        df           = enriched_df,
        dedup_keys   = ["warehouse_id", "sku_id", "stock_snapshot_date"],
        order_by_col = "last_counted_at",
    )

    quality_report = run_bronze_quality_checks(
        df            = deduped_df,
        table_name    = "raw_warehouse_stock",
        required_cols = ["warehouse_id", "sku_id", "units_on_hand", "stock_snapshot_date"],
    )

    write_bronze_delta(
        df          = deduped_df,
        target_path = BRONZE_STOCK_TABLE,
        merge_keys  = ["warehouse_id", "sku_id", "stock_snapshot_date"],
    )

    summary = {
        "feed"          : "warehouse_stock",
        "process_date"  : PROCESS_DATE,
        "rows_ingested" : deduped_df.count(),
        "quality"       : quality_report,
        "status"        : "SUCCESS",
    }

    log.info(f"STOCK INGESTION  Complete. Summary: {summary}")
    return summary


def run_shipping_ingestion() -> dict:
    """
    Ingest today's shipping event records from the landing zone to Bronze.

    Reads the carrier and WMS shipping event feed, enforces the shipping schema,
    appends metadata, deduplicates on shipment ID and timestamp, runs quality
    checks, and merges into the Bronze raw_shipping_events Delta table.

    Returns
    -------
    dict
        Ingestion summary with row counts and quality check results.
    """

    log.info("SHIPPING INGESTION  Starting shipping events ingestion")

    raw_df = read_landing_files(
        landing_path = SHIP_LANDING_PATH,
        schema       = SHIPPING_SCHEMA,
        file_format  = "json",
    )

    enriched_df = add_ingestion_metadata(raw_df, source_name="shipping_events")

    deduped_df = deduplicate_batch(
        df           = enriched_df,
        dedup_keys   = ["shipment_id", "order_id"],
        order_by_col = "event_timestamp",
    )

    quality_report = run_bronze_quality_checks(
        df            = deduped_df,
        table_name    = "raw_shipping_events",
        required_cols = ["shipment_id", "order_id", "warehouse_id", "sku_id"],
    )

    write_bronze_delta(
        df          = deduped_df,
        target_path = BRONZE_SHIP_TABLE,
        merge_keys  = ["shipment_id", "order_id"],
    )

    summary = {
        "feed"          : "shipping_events",
        "process_date"  : PROCESS_DATE,
        "rows_ingested" : deduped_df.count(),
        "quality"       : quality_report,
        "status"        : "SUCCESS",
    }

    log.info(f"SHIPPING INGESTION  Complete. Summary: {summary}")
    return summary


def run_pipeline() -> dict:
    """
    Execute both ingestion feeds in sequence and return a combined run report.

    Both feeds are processed independently so a failure in one does not
    block the other. The run report captures outcomes for both feeds and
    is written to the Spark log for Fabric pipeline monitoring.

    Returns
    -------
    dict
        Combined pipeline run report with status for both feeds.
    """

    log.info("=" * 72)
    log.info(f"PIPELINE  Starting wholesale distribution ingestion for {PROCESS_DATE}")
    log.info("=" * 72)

    run_report = {
        "pipeline"      : "WholesaleDistributionIngestion",
        "process_date"  : PROCESS_DATE,
        "feeds"         : {},
    }

    try:
        run_report["feeds"]["warehouse_stock"]  = run_stock_ingestion()
    except Exception as exc:
        log.error(f"PIPELINE  Stock ingestion failed: {exc}")
        run_report["feeds"]["warehouse_stock"]  = {"status": "FAILED", "error": str(exc)}

    try:
        run_report["feeds"]["shipping_events"]  = run_shipping_ingestion()
    except Exception as exc:
        log.error(f"PIPELINE  Shipping ingestion failed: {exc}")
        run_report["feeds"]["shipping_events"]  = {"status": "FAILED", "error": str(exc)}

    all_success = all(
        v.get("status") == "SUCCESS"
        for v in run_report["feeds"].values()
    )
    run_report["overall_status"] = "SUCCESS" if all_success else "PARTIAL_FAILURE"

    log.info(f"PIPELINE  Complete. Report: {run_report}")
    log.info("=" * 72)

    return run_report


# ==============================================================================
# ENTRY POINT
# ==============================================================================

if __name__ == "__main__":
    result = run_pipeline()
    if result["overall_status"] != "SUCCESS":
        raise RuntimeError(
            f"Pipeline completed with status: {result['overall_status']}. "
            "Review the Spark logs for details on failed feeds."
        )
