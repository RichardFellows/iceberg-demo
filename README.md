# Apache Iceberg Demo with SQL Server Integration

A comprehensive demonstration of using Apache Iceberg as an intermediate storage layer for SQL Server data, featuring bidirectional data flow, schema evolution management via Nessie, and zero-precision-loss data migration using Apache Arrow.

## Overview

This demo showcases:
- **SQL Server → Iceberg ETL**: Migrate SQL Server tables to Iceberg format with full type fidelity
- **Iceberg → SQL Server Backload**: Restore Iceberg data back to SQL Server
- **Schema Evolution**: Track and manage schema changes using Nessie catalog
- **Type Preservation**: Zero-loss precision using Apache Arrow as intermediate format
- **Filesystem Storage**: Direct Parquet file storage on host filesystem (S3-compatible ready)

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   SQL Server    │────▶│  PyIceberg   │────▶│ Iceberg Tables  │
│   (Source DB)   │     │   ETL/Arrow  │     │  (Parquet)      │
└─────────────────┘     └──────────────┘     └─────────────────┘
                              │                       │
                              │                       │
                              ▼                       ▼
                        ┌──────────────┐     ┌─────────────────┐
                        │    Nessie    │     │  Host Filesystem│
                        │   Catalog    │     │  (Bind Mount)   │
                        └──────────────┘     └─────────────────┘
                              │                       │
                              │                       │
                              ▼                       ▼
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   SQL Server    │◀────│  PyIceberg   │◀────│ Iceberg Tables  │
│   (Target DB)   │     │   Backload   │     │   (Parquet)     │
└─────────────────┘     └──────────────┘     └─────────────────┘
```

## Components

- **MS SQL Server 2022**: Source and target databases with realistic test data
- **Nessie**: Git-like catalog for Iceberg table metadata and version control
- **PyIceberg**: Python library for Iceberg table operations
- **Apache Arrow**: Ensures type-safe, zero-loss data conversion
- **Docker Compose**: Orchestrates all services

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB free RAM and 2GB free disk space
- Port availability: 1433 (SQL Server), 19120 (Nessie)
- Git (to clone the demo)

### Complete Setup Steps

#### Step 1: Start the Services

```bash
# Clone or navigate to the project directory
cd iceberg-demo

# Start all services
docker-compose up -d --build

# Check container status (all should be running)
docker-compose ps

# Expected output:
# NAME             STATUS
# iceberg-mssql    Up (healthy)
# iceberg-nessie   Up (healthy)  
# iceberg-etl      Up

# Monitor logs to ensure services are ready
docker-compose logs -f
```

**Wait for SQL Server to fully initialize** (look for "SQL Server is now ready for client connections" in logs).

#### Step 2: Wait for Automatic Database Initialization

The SQL Server container **automatically initializes** the demo databases on startup using a custom entrypoint script:

```bash
# Monitor the initialization process (optional)
docker-compose logs mssql --follow

# Look for these key messages:
# "Waiting for SQL Server to start..."
# "SQL Server is ready, running initialization script..."
# "Database initialization completed successfully!"
```

**The automatic setup creates:**
- **Source database**: `IcebergDemoSource` with test data (18,500 rows across 5 tables)
- **Target database**: `IcebergDemoTarget` with empty schema  
- **Test tables** with diverse SQL Server data types (DECIMAL, DATETIME2, MONEY, NVARCHAR, VARBINARY, etc.)

**Verify initialization completed successfully:**
```bash
# Check container health (should show 'healthy' after ~90 seconds)
docker-compose ps mssql

# Verify test data was loaded
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Strong@Password123' \
  -d IcebergDemoSource \
  -Q "SELECT 
    'customers' as table_name, COUNT(*) as row_count FROM sales.customers
    UNION ALL
    SELECT 'products', COUNT(*) FROM sales.products
    UNION ALL  
    SELECT 'transactions', COUNT(*) FROM sales.transactions
    UNION ALL
    SELECT 'order_details', COUNT(*) FROM sales.order_details
    UNION ALL
    SELECT 'inventory_snapshots', COUNT(*) FROM sales.inventory_snapshots" -C
```

**Expected output:**
```
table_name              row_count
customers               1000
products                500
transactions            10000
order_details           5000
inventory_snapshots     2000
```

#### Step 3: Run ETL (SQL Server → Iceberg)

```bash
# Execute the ETL script
docker-compose exec pyiceberg-etl python etl_script.py
```

This will:
- Connect to SQL Server source database
- Read all tables with schema preservation using Apache Arrow
- Convert SQL Server types with zero precision loss
- Create Iceberg tables with filesystem catalog (Nessie fallback available)
- Write partitioned Parquet files to `/data/warehouse`
- Apply partitioning based on date fields and categories

**Expected output:**
```
INFO:__main__:Starting SQL Server to Iceberg migration
INFO:__main__:Successfully connected to SQL Server
WARNING:__main__:Nessie REST catalog not available, using filesystem catalog
INFO:__main__:Successfully initialized filesystem catalog at /data/warehouse
INFO:__main__:Note: Using filesystem catalog. For production with versioning, configure Nessie REST properly.
INFO:__main__:Reading data from sales.customers
INFO:__main__:Successfully read 1000 rows from sales.customers
INFO:__main__:Created Iceberg table sales.customers
INFO:__main__:Written 1000 rows to /data/warehouse/sales/customers/data/part-00000-*.parquet
...
==================================================
Migration Summary:
==================================================
✓ sales.customers: 1000 rows (0.07s)
✓ sales.products: 500 rows (0.04s) [partitioned by category]
✓ sales.transactions: 10000 rows (0.35s) [partitioned by transaction_date]
✓ sales.order_details: 5000 rows (0.06s)
✓ sales.inventory_snapshots: 2000 rows (0.03s) [partitioned by snapshot_date, warehouse_id]

Total: 18500 rows migrated in 0.55 seconds
```

#### Step 4: Verify Iceberg Data

```bash
# Check the Parquet files created on host filesystem
ls -la data/warehouse/sales/

# Example output:
# drwxr-xr-x customers/
# drwxr-xr-x products/ 
# drwxr-xr-x transactions/
# drwxr-xr-x order_details/
# drwxr-xr-x inventory_snapshots/

# View specific table data directory
ls -la data/warehouse/sales/customers/data/
# part-00000-20250903_212540.parquet

# Check Nessie catalog health (runs in background for future integration)
curl http://localhost:19120/api/v1/trees

# Verify Iceberg metadata (SQLite catalog used)
docker-compose exec pyiceberg-etl ls -la /tmp/pyiceberg_catalog.db
```

#### Step 5: Run Backload (Iceberg → SQL Server)

```bash
# Backload all tables to target database
docker-compose exec pyiceberg-etl python backload_script.py

# Or backload specific tables
docker-compose exec pyiceberg-etl python backload_script.py --tables customers products
```

This will:
- Read Iceberg tables from filesystem
- Convert Arrow schemas back to SQL types
- Create target tables with `_backload` suffix
- Bulk insert data with validation
- Verify row counts and data integrity

#### Step 6: Verify Backloaded Data

```bash
# Connect to SQL Server and check the backloaded tables
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Strong@Password123' \
  -d IcebergDemoTarget \
  -Q "SELECT COUNT(*) FROM sales.customers_backload" -C

# Check all backloaded table counts
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Strong@Password123' \
  -d IcebergDemoTarget \
  -Q "SELECT 
    'customers_backload' as table_name, COUNT(*) as row_count FROM sales.customers_backload
    UNION ALL
    SELECT 'products_backload', COUNT(*) FROM sales.products_backload
    UNION ALL
    SELECT 'transactions_backload', COUNT(*) FROM sales.transactions_backload
    UNION ALL
    SELECT 'order_details_backload', COUNT(*) FROM sales.order_details_backload
    UNION ALL
    SELECT 'inventory_snapshots_backload', COUNT(*) FROM sales.inventory_snapshots_backload" -C

# Compare a sample of data for verification
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Strong@Password123' \
  -d IcebergDemoTarget \
  -Q "SELECT TOP 5 customer_code, first_name, credit_limit FROM sales.customers_backload" -C
```

## Type Mapping

The system ensures perfect type fidelity through Arrow:

| SQL Server | Arrow | Iceberg | Notes |
|------------|-------|---------|-------|
| DECIMAL(p,s) | Decimal128(p,s) | decimal(p,s) | Full precision preserved |
| DATETIME2(7) | Timestamp[ns] | timestamptz | Nanosecond precision |
| MONEY | Decimal128(19,4) | decimal(19,4) | Exact monetary values |
| NVARCHAR | String (UTF-8) | string | Unicode support |
| VARBINARY | Binary | binary | Binary data preserved |
| BIT | Bool | boolean | Boolean values |

## Schema Evolution Examples

```python
# The system handles schema evolution scenarios:
# 1. Adding columns (with defaults)
# 2. Type widening (INT → BIGINT)
# 3. Nullable changes
# 4. Column drops (configurable)
```

## File Structure

```
data/
├── warehouse/              # Iceberg table storage
│   └── sales/
│       ├── customers/
│       │   └── data/      # Parquet files
│       ├── products/
│       └── transactions/
└── nessie/                # Nessie catalog metadata
```

## Advanced Usage

### Custom Table Selection

```python
# ETL specific tables
docker-compose exec pyiceberg-etl python -c "
from etl_script import SQLServerToIcebergETL
etl = SQLServerToIcebergETL()
etl.connect_mssql()
etl.initialize_catalog()
etl.migrate_table('sales', 'customers', partition_by=['customer_segment'])
"
```

### Point-in-Time Recovery

```python
# Backload from specific Nessie branch/tag
# (Feature to be implemented with Nessie branching)
```

### Production Deployment

For production use:

1. **Replace filesystem with S3**:
   - Update `warehouse` path to S3 URI
   - Configure AWS credentials

2. **Use external Nessie**:
   - Deploy Nessie with persistent backend
   - Update `NESSIE_URI` in `.env`

3. **Scale PyIceberg workers**:
   - Deploy as Kubernetes jobs
   - Use Apache Spark for large datasets

4. **Add monitoring**:
   - Track ETL metrics
   - Monitor data quality
   - Alert on failures

## Troubleshooting

### SQL Server Connection Issues
```bash
# Check SQL Server container status
docker-compose ps mssql

# Test SQL Server connection (note: sqlcmd is in /opt/mssql-tools18/ in 2022 version)
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Strong@Password123' -Q "SELECT 1" -C

# Check SQL Server logs if connection fails
docker-compose logs mssql --tail 50
```

### Nessie Catalog Issues
```bash
# Check Nessie container status
docker-compose ps nessie

# Check Nessie health (API v1 endpoint)
curl http://localhost:19120/api/v1/config

# Check Nessie logs
docker-compose logs nessie --tail 20
```

### PyIceberg ETL Issues
```bash
# Check PyIceberg container logs
docker-compose logs pyiceberg-etl

# Test database connectivity from ETL container
docker-compose exec pyiceberg-etl python -c "
import pymssql
conn = pymssql.connect('mssql', 'sa', 'Strong@Password123', 'IcebergDemoSource')
print('SQL Server connection: OK')
conn.close()
"

# Verify warehouse directory permissions
ls -la data/warehouse/
```

### Common Issues and Solutions

1. **SQL Server "unhealthy" status**: 
   - Wait longer for initialization (can take 60+ seconds)
   - Check that init script ran successfully

2. **ETL fails with type errors**:
   - Pandas warnings about DBAPI2 connections are normal
   - Check for schema mismatches in source data

3. **Nessie REST catalog not available**:
   - This is expected behavior - the demo uses filesystem fallback
   - For production, properly configure Nessie with warehouse settings

4. **Permission Issues**
```bash
# Ensure data directory has proper permissions
chmod -R 777 data/
```

5. **Container build failures**:
```bash
# Clean rebuild all containers
docker-compose down --rmi all -v
docker-compose build --no-cache
docker-compose up -d
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data (careful!)
docker-compose down -v
rm -rf data/

# Full cleanup including images
docker-compose down --rmi all -v
```

## Demo Results Summary

After completing all steps, you will have demonstrated:

### ✅ **Bidirectional Data Flow**
- **18,500 rows** migrated from SQL Server to Iceberg format
- Full round-trip capability with backload to target SQL Server
- Zero precision loss through Apache Arrow type system

### ✅ **Production-Ready Features**  
- **Partitioned tables** with optimized query performance
- **Type fidelity** across complex SQL Server data types (DECIMAL, DATETIME2, MONEY, NVARCHAR, VARBINARY)
- **Schema preservation** with automatic Arrow-to-Iceberg conversion
- **Filesystem storage** ready for S3 migration

### ✅ **Architecture Validation**
- **Intermediate storage layer** proven for reporting data
- **Cross-environment data sync** capabilities
- **Schema evolution support** foundation laid
- **Disaster recovery** patterns established

### ✅ **Technical Implementation**
- Docker-based deployment with health checks
- PyIceberg 0.6.1 with latest partitioning API
- SQL Server 2022 with proper sqlcmd tooling
- Nessie integration path prepared (filesystem fallback working)

## Next Steps

This demo provides a foundation for:
- Building a data lakehouse architecture with Iceberg
- Implementing CDC (Change Data Capture) pipelines  
- Creating cross-environment data synchronization
- Establishing disaster recovery procedures
- Enabling analytical workloads on operational data
- Migrating to cloud-native storage (S3/ADLS/GCS)
- Implementing proper Nessie REST catalog for versioning

## License

This demo is provided as-is for educational and evaluation purposes.