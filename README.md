# Apache Iceberg Demo with SQL Server Integration

A comprehensive demonstration of using Apache Iceberg as an intermediate storage layer for SQL Server data, featuring bidirectional data flow, schema evolution management via Nessie, S3-compatible storage with MinIO, and a modern web dashboard for interactive operations.

## 🚀 Key Features

- **Enterprise-Grade MinIO**: Source-built from UBI8 base image with zero external dependencies
- **Interactive Web Dashboard**: Real-time monitoring and one-click ETL operations
- **Bidirectional Data Flow**: SQL Server ↔ Apache Iceberg with full type preservation
- **S3-Compatible Storage**: Production-ready MinIO implementation
- **Schema Evolution**: Track and manage changes using Nessie catalog
- **Zero-Loss Precision**: Apache Arrow ensures perfect type fidelity

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     Web Dashboard (Port 8000)                    │
│          Real-time Monitoring & Interactive Controls             │
└────────────────────────────┬─────────────────────────────────────┘
                             │ FastAPI + WebSocket
┌────────────────────────────┼─────────────────────────────────────┐
│                            ▼                                     │
│  ┌─────────────────┐     ┌──────────────┐     ┌──────────────┐  │
│  │   SQL Server    │────▶│  PyIceberg   │────▶│    MinIO     │  │
│  │   (Source DB)   │     │   ETL/Arrow  │     │  S3 Storage  │  │
│  │    18.5K rows   │     │    0.9.1     │     │ Source-Built │  │
│  └─────────────────┘     └──────────────┘     └──────────────┘  │
│         │                       │                      │         │
│         │                       │ REST API             ▼         │
│         │                 ┌──────────────┐     ┌──────────────┐ │
│         │                 │    Nessie    │────▶│   Iceberg    │ │
│         │                 │ REST Catalog │     │    Tables    │ │
│         │                 │  Git-like    │     │  (Parquet)   │ │
│         │                 └──────────────┘     └──────────────┘ │
│         │                       │                      │         │
│         ▼                       ▼                      ▼         │
│  ┌─────────────────┐     ┌──────────────┐     ┌──────────────┐  │
│  │   SQL Server    │◀────│  PyIceberg   │◀────│   Iceberg    │  │
│  │   (Target DB)   │     │   Backload   │     │    Tables    │  │
│  │  Restored Data  │     │   Script     │     │  Partitioned │  │
│  └─────────────────┘     └──────────────┘     └──────────────┘  │
└───────────────────────────────────────────────────────────────────┘
```

## Components

### Core Services
- **MS SQL Server 2022**: Source and target databases with 18,500 rows of test data
- **Nessie**: Git-like catalog for Iceberg table metadata and version control
- **MinIO**: S3-compatible object storage (source-built from UBI8)
- **PyIceberg 0.9.1**: Latest Python library for Iceberg operations
- **Web Dashboard**: FastAPI-based interactive control panel

### Special Features
- **Source-Built MinIO**: Compiled from Go source using Red Hat UBI8 base
- **Apache Arrow**: Ensures type-safe, zero-loss data conversion
- **Docker Compose**: Full orchestration with health checks

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB free RAM and 2GB free disk space
- Ports available: 
  - 1433 (SQL Server)
  - 8000 (Web Dashboard)
  - 9000-9001 (MinIO)
  - 19120 (Nessie)

### 🎯 Option 1: Web Dashboard (Recommended)

The interactive dashboard provides real-time monitoring and one-click operations for the complete Apache Iceberg pipeline.

#### Step 1: Start All Services

```bash
# Clone the repository
git clone <repository-url>
cd iceberg-demo

# Start all services including dashboard
docker-compose up -d --build

# Wait for services to be healthy (about 90 seconds)
docker-compose ps

# Expected output - all services should be healthy:
# NAME               STATUS
# iceberg-dashboard  Up (healthy)
# iceberg-minio      Up (healthy)  
# iceberg-mssql      Up (healthy)
# iceberg-nessie     Up (healthy)
# iceberg-etl        Up
```

#### Step 2: Access the Web Dashboard

Open your browser and navigate to:

```
http://localhost:8000
```

The **FastAPI-powered dashboard** provides:

##### 🎛️ System Status Panel
- **Real-time Health Monitoring**: Live status for SQL Server, Nessie, MinIO
- **Connection Status**: Verify all service connectivity
- **Auto-refresh**: Updates every 30 seconds automatically

##### ⚡ ETL Controls
- **One-click ETL Pipeline**: Migrate SQL Server → Apache Iceberg
- **One-click Backload**: Restore Iceberg → SQL Server  
- **Progress Tracking**: Real-time progress bars and percentages
- **Streaming Logs**: Live terminal-style operation logs

##### 🗂️ Catalog Browser
- **Nessie Tables**: View all Iceberg tables and metadata
- **SQL Server Tables**: Monitor source/target databases
- **Row Counts**: Real-time data metrics and statistics
- **Last Sync Times**: Track operation timestamps

#### Step 3: Run ETL via Dashboard

1. **Check System Status**: Ensure all services show green/healthy status
2. **Click "▶️ Run ETL Pipeline"**: Start SQL Server → Iceberg migration
3. **Monitor Progress**: Watch live progress bar (0-100%)
4. **View Logs**: Real-time streaming logs show detailed operations
5. **Verify Results**: Check Nessie Catalog Browser for 5 new tables

**Expected Results**: 18,500 rows migrated across 5 partitioned tables

#### Step 4: Run Backload via Dashboard

1. **Ensure ETL Completed**: First run ETL to populate Iceberg tables
2. **Click "◀️ Run Backload"**: Start Iceberg → SQL Server restoration  
3. **Monitor Progress**: Track backload operation in log viewer
4. **Verify Results**: Check SQL Server Tables panel for restored data

**Expected Results**: All data restored to `*_backload` tables in target database

### 🔧 Option 2: Command Line Interface

#### Step 1: Initialize MinIO Storage

```bash
# Create warehouse bucket in MinIO
./init-minio.sh

# Or manually:
docker exec iceberg-minio mc alias set local http://localhost:9000 minioadmin minioadmin123
docker exec iceberg-minio mc mb local/warehouse --ignore-existing
```

#### Step 2: Run ETL Manually

```bash
# Execute the ETL script
docker-compose exec pyiceberg-etl python etl_script.py

# Expected output:
# INFO: Starting SQL Server to Iceberg migration
# INFO: Successfully connected to SQL Server
# INFO: Successfully initialized Nessie REST catalog at http://nessie:19120/iceberg
# INFO: Created 'sales' namespace in Iceberg catalog
# ✓ sales.customers: 1000 rows
# ✓ sales.products: 500 rows [partitioned by category]
# ✓ sales.transactions: 10000 rows [partitioned by transaction_date]
# ✓ sales.order_details: 5000 rows
# ✓ sales.inventory_snapshots: 2000 rows [partitioned by snapshot_date, warehouse_id]
# Total: 18500 rows migrated
```

#### Step 3: Run Backload Manually

```bash
# Backload all tables to target database
docker-compose exec pyiceberg-etl python backload_script.py

# Or backload specific tables
docker-compose exec pyiceberg-etl python backload_script.py --tables customers products
```

## 🏗️ Enterprise MinIO Implementation

This demo includes a **source-built MinIO** implementation that eliminates all external binary dependencies:

### Features
- **Built from Source**: MinIO compiled from official GitHub repositories
- **UBI8 Base Image**: Red Hat Universal Base Image for enterprise compliance
- **Zero External Dependencies**: No binary downloads or docker.io dependencies
- **Multi-Stage Build**: Efficient Docker pattern with separate build/runtime stages

### Building Custom MinIO

```bash
# The MinIO image is automatically built as part of docker-compose
cd minio/
docker build -t source-built-minio:latest .

# Verify source build
docker run --rm source-built-minio:latest /usr/local/bin/minio --version
# Output: minio version DEVELOPMENT.2025-08-29... (indicates source build)
```

## Type Mapping

Perfect type fidelity through Apache Arrow:

| SQL Server | Arrow | Iceberg | MinIO Storage |
|------------|-------|---------|---------------|
| DECIMAL(p,s) | Decimal128(p,s) | decimal(p,s) | Parquet in S3 |
| DATETIME2(7) | Timestamp[ns] | timestamptz | Partitioned by date |
| MONEY | Decimal128(19,4) | decimal(19,4) | Exact values |
| NVARCHAR | String (UTF-8) | string | UTF-8 encoded |
| VARBINARY | Binary | binary | Binary preserved |
| BIT | Bool | boolean | Boolean values |

## Data Verification

### Check MinIO Storage

```bash
# List buckets
docker exec iceberg-minio mc ls local/

# Browse warehouse data
docker exec iceberg-minio mc ls local/warehouse/

# Access MinIO Console
# URL: http://localhost:9001
# Credentials: minioadmin / minioadmin123
```

### Verify SQL Server Data

```bash
# Check source data counts
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Strong@Password123' \
  -d IcebergDemoSource \
  -Q "SELECT 
    'Total Rows' as Metric, 
    SUM(row_count) as Value 
    FROM (
      SELECT COUNT(*) as row_count FROM sales.customers
      UNION ALL SELECT COUNT(*) FROM sales.products
      UNION ALL SELECT COUNT(*) FROM sales.transactions
      UNION ALL SELECT COUNT(*) FROM sales.order_details
      UNION ALL SELECT COUNT(*) FROM sales.inventory_snapshots
    ) t" -C

# Check backloaded data
docker-compose exec mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P 'Strong@Password123' \
  -d IcebergDemoTarget \
  -Q "SELECT COUNT(*) FROM sales.customers_backload" -C
```

### Check Nessie Catalog

```bash
# API health check
curl http://localhost:19120/api/v1/trees

# View commits (if using Nessie REST catalog)
curl http://localhost:19120/api/v1/trees/tree/main/history
```

## 📊 Dashboard Features

### System Monitoring
- **Service Health**: Real-time status for SQL Server, Nessie, MinIO
- **Data Metrics**: Row counts, table statistics, sync timestamps
- **Auto-refresh**: Updates every 30 seconds

### ETL Operations
- **One-Click ETL**: Migrate data with a single button
- **One-Click Backload**: Restore data to SQL Server
- **Progress Tracking**: Real-time progress bars
- **Live Logs**: Terminal-style operation logs

### Catalog Management
- **Nessie Browser**: View Iceberg tables and metadata
- **SQL Tables**: Monitor source and target databases
- **Commit History**: Browse version control (when fully configured)

## Advanced Configuration

### Production Deployment

For production use:

1. **External S3 Storage**:
```yaml
# Update docker-compose.yml
environment:
  - AWS_ENDPOINT_URL=https://s3.amazonaws.com
  - AWS_ACCESS_KEY_ID=your-key
  - AWS_SECRET_ACCESS_KEY=your-secret
```

2. **Persistent Nessie**:
```yaml
# Use PostgreSQL backend for Nessie
environment:
  - NESSIE_VERSION_STORE_TYPE=JDBC
  - QUARKUS_DATASOURCE_URL=jdbc:postgresql://postgres:5432/nessie
```

3. **Scale ETL Workers**:
```yaml
# Deploy multiple ETL containers
deploy:
  replicas: 3
```

## Troubleshooting

### Dashboard Issues
```bash
# Check dashboard logs
docker-compose logs dashboard --tail 50

# Test API endpoint
curl http://localhost:8000/api/status

# Restart dashboard
docker-compose restart dashboard
```

### MinIO Issues
```bash
# Check MinIO health
curl http://localhost:9000/minio/health/live

# View MinIO logs
docker-compose logs minio --tail 20

# Access MinIO shell
docker exec -it iceberg-minio sh
```

### ETL Failures
```bash
# Check ETL container logs
docker-compose logs pyiceberg-etl --tail 50

# Test database connectivity
docker-compose exec pyiceberg-etl python -c "
import pymssql
conn = pymssql.connect('mssql', 'sa', 'Strong@Password123', 'IcebergDemoSource')
print('Connected successfully')
conn.close()
"
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

## 🎯 Demo Results Summary

After completing the demo, you will have:

### ✅ **Enterprise Architecture**
- **Source-built MinIO** from UBI8 with zero external dependencies
- **S3-compatible storage** for cloud-ready deployment
- **Interactive dashboard** for operations and monitoring

### ✅ **Data Pipeline**
- **18,500 rows** migrated with zero precision loss
- **Bidirectional flow** between SQL Server and Iceberg
- **Partitioned tables** optimized for analytics

### ✅ **Production Features**
- **Type preservation** across complex SQL types
- **Schema evolution** support via Nessie
- **Health monitoring** and automatic initialization
- **Web-based controls** for non-technical users

## Next Steps

This demo provides a foundation for:
- Building a data lakehouse architecture
- Implementing CDC (Change Data Capture) pipelines
- Migrating to cloud storage (AWS S3, Azure ADLS, Google GCS)
- Establishing disaster recovery procedures
- Scaling to production workloads

## License

This demo is provided as-is for educational and evaluation purposes.