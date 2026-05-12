# ==============================================================================
# FILE        : fabric_environment.md
# PURPOSE     : Microsoft Fabric environment configuration reference
# DESCRIPTION : Lists all environment variables, Fabric GUIDs, and connection
#               settings required to deploy this pipeline in a Fabric workspace.
#               Fill in your real values before deploying. Never commit real
#               GUIDs or credentials to version control.
# ==============================================================================

## Required Fabric Workspace Settings

### wholesale_ingestion.py Environment Variables

Replace these placeholder values in the ingestion script before running:

| Variable | Description | Example Format |
|----------|-------------|----------------|
| WORKSPACE_ID | Your Fabric workspace GUID | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx |
| LAKEHOUSE_ID | Your Fabric Lakehouse GUID | xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx |

### OneLake Landing Zone Structure

The PySpark script expects files in this OneLake Files layout:

```
Files/
  landing/
    warehouse_stock/
      2024-01-15/
        stock_snapshot_20240115.csv
    shipping_events/
      2024-01-15/
        shipping_events_20240115.json
```

### dbt profiles.yml (local development)

```yaml
wholesale_fabric:
  target: dev
  outputs:
    dev:
      type          : spark
      method        : http
      host          : YOUR_FABRIC_SPARK_ENDPOINT
      port          : 443
      token         : YOUR_AAD_TOKEN
      schema        : gold
      connect_retries : 3
      connect_timeout : 60
```

### Fabric Pipeline Schedule

The recommended Fabric Data Pipeline schedule:

1. PySpark Notebook or Job Definition runs at 01:00 UTC (ingestion)
2. dbt Job runs at 02:30 UTC (after ingestion completes)
3. Power BI semantic model refresh runs at 04:00 UTC
