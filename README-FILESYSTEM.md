# Apache Iceberg Demo - Filesystem Mode

## Overview

This is the **filesystem-only** version of the Apache Iceberg demo that operates without MinIO or Nessie dependencies. It's designed for environments where:

- External repository access is restricted
- S3-compatible storage is not required
- Version control/catalog features are not needed
- Simpler architecture is preferred for demos

## 🎯 Key Differences from Full Version

| Feature | Full Version | Filesystem Mode |
|---------|-------------|-----------------|
| Storage | MinIO (S3-compatible) | Local filesystem |
| Catalog | Nessie REST | SQLite + filesystem |
| Versioning | Git-like via Nessie | None |
| Dependencies | Requires MinIO image | No external images |
| Complexity | Production-ready | Simplified demo |

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                Web Dashboard (Port 8000)                         │
│               Simplified Controls & Monitoring                   │
└────────────────────────────┬─────────────────────────────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────────┐
│                            ▼                                     │
│  ┌─────────────────┐     ┌──────────────┐     ┌──────────────┐  │
│  │   SQL Server    │────▶│  PyIceberg   │────▶│  Filesystem  │  │
│  │   (Source DB)   │     │   ETL/Arrow  │     │   Storage    │  │
│  │    18.5K rows   │     │    0.9.1     │     │  /data/ware  │  │
│  └─────────────────┘     └──────────────┘     └──────────────┘  │
│         │                       │                      │         │
│         │                       │                      ▼         │
│         │                 ┌──────────────┐     ┌──────────────┐ │
│         │                 │    SQLite    │────▶│   Iceberg    │ │
│         │                 │   Catalog    │     │    Tables    │ │
│         │                 └──────────────┘     │  (Parquet)   │ │
│         │                       │               └──────────────┘ │
│         │                       │                      │         │
│         ▼                       ▼                      ▼         │
│  ┌─────────────────┐     ┌──────────────┐     ┌──────────────┐  │
│  │   SQL Server    │◀────│  PyIceberg   │◀────│   Iceberg    │  │
│  │   (Target DB)   │     │   Backload   │     │    Tables    │  │
│  └─────────────────┘     └──────────────┘     └──────────────┘  │
└───────────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- 4GB RAM and 2GB disk space
- Ports available: 1433 (SQL Server), 8000 (Dashboard)

### 1. Start Services

```bash
# Use the helper script
./start-filesystem.sh

# Or manually with docker-compose
docker-compose -f docker-compose-filesystem.yml up -d --build
```

### 2. Access Dashboard

Open your browser to: **http://localhost:8000**

The dashboard shows:
- System status (SQL Server, Filesystem, Catalog)
- ETL controls with progress tracking
- Table listings and row counts
- Operation logs

### 3. Run ETL Pipeline

#### Option A: Via Dashboard (Recommended)
1. Click **"Run ETL Pipeline"** button
2. Monitor progress bar and logs
3. View created Iceberg tables

#### Option B: Via Command Line
```bash
docker-compose -f docker-compose-filesystem.yml exec pyiceberg-etl \
    python etl_script_filesystem.py
```

Expected output:
```
============================================================
Starting SQL Server to Iceberg migration (Filesystem Mode)
============================================================
Successfully connected to SQL Server
Successfully initialized filesystem catalog at /data/warehouse
Created 'sales' namespace in Iceberg catalog
✓ sales.customers: 1000 rows
✓ sales.products: 500 rows [partitioned by category]
✓ sales.transactions: 10000 rows [partitioned by transaction_date]
✓ sales.order_details: 5000 rows
✓ sales.inventory_snapshots: 2000 rows [partitioned by snapshot_date, warehouse_id]

Total: 18500 rows migrated
Data stored in: /data/warehouse
```

### 4. Run Backload

#### Option A: Via Dashboard
1. Click **"Run Backload"** button
2. Monitor restoration progress

#### Option B: Via Command Line
```bash
docker-compose -f docker-compose-filesystem.yml exec pyiceberg-etl \
    python backload_script_filesystem.py
```

### 5. Verify Data

Check the filesystem storage:
```bash
# List Iceberg tables
ls -la data/warehouse/sales/

# View catalog database
ls -la data/warehouse/catalog.db

# Count Parquet files
find data/warehouse -name "*.parquet" | wc -l
```

Check SQL Server:
```bash
docker-compose -f docker-compose-filesystem.yml exec mssql \
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'Strong@Password123' \
    -d IcebergDemoTarget -Q "SELECT COUNT(*) FROM sales.customers_backload" -C
```

## File Structure

```
iceberg-demo/
├── docker-compose-filesystem.yml    # Simplified compose file
├── start-filesystem.sh              # Start script
├── stop-filesystem.sh               # Stop script
├── README-FILESYSTEM.md             # This file
├── data/
│   └── warehouse/                   # Iceberg data storage
│       ├── catalog.db               # SQLite catalog
│       └── sales/                   # Namespace
│           ├── customers/           # Table data
│           ├── products/
│           └── ...
├── pyiceberg-etl/
│   ├── etl_script_filesystem.py    # Filesystem ETL script
│   ├── backload_script_filesystem.py # Filesystem backload
│   ├── requirements-filesystem.txt  # Reduced dependencies
│   └── Dockerfile.filesystem       # ETL container
└── dashboard/
    ├── app_filesystem.py           # Simplified dashboard
    ├── requirements-filesystem.txt # Dashboard dependencies
    └── Dockerfile.filesystem       # Dashboard container
```

## Key Implementation Details

### Catalog Configuration
Uses PyIceberg's SQL catalog with SQLite backend:
```python
catalog = SqlCatalog(
    "filesystem_catalog",
    **{
        "uri": f"sqlite:////data/warehouse/catalog.db",
        "warehouse": f"file:///data/warehouse",
    }
)
```

### Storage Layout
```
/data/warehouse/
├── catalog.db              # Metadata catalog
└── sales/                  # Namespace
    ├── customers/
    │   └── data/
    │       └── part-00000-*.parquet
    ├── products/
    │   └── data/
    │       └── part-00000-*.parquet
    └── ...
```

### Type Mapping
Same Arrow-based type preservation as full version:
- DECIMAL → Decimal128
- DATETIME2 → Timestamp[ns]
- MONEY → Decimal128(19,4)
- NVARCHAR → String (UTF-8)
- VARBINARY → Binary

## Limitations

1. **No Version Control**: Without Nessie, there's no catalog versioning
2. **No S3 Compatibility**: Cannot integrate with cloud storage
3. **Local Storage Only**: Data stored on Docker host filesystem
4. **No Time Travel**: Cannot query historical snapshots
5. **Single Node**: Not suitable for distributed processing

## Troubleshooting

### Dashboard Not Loading
```bash
# Check container status
docker-compose -f docker-compose-filesystem.yml ps

# View dashboard logs
docker-compose -f docker-compose-filesystem.yml logs dashboard
```

### ETL Failures
```bash
# Check ETL container logs
docker-compose -f docker-compose-filesystem.yml logs pyiceberg-etl

# Verify SQL Server is ready
docker-compose -f docker-compose-filesystem.yml exec mssql \
    /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa \
    -P 'Strong@Password123' -Q "SELECT 1" -C
```

### Permission Issues
```bash
# Fix warehouse permissions
chmod -R 777 data/warehouse
```

### Cleanup
```bash
# Stop services
./stop-filesystem.sh

# Remove all data
rm -rf data/warehouse

# Remove containers and volumes
docker-compose -f docker-compose-filesystem.yml down -v
```

## When to Use This Mode

✅ **Use Filesystem Mode When:**
- Running demos in restricted environments
- MinIO repository access is blocked
- Simple proof-of-concept is sufficient
- Learning Apache Iceberg basics
- Testing ETL pipelines locally

❌ **Use Full Version When:**
- Production deployment needed
- S3/cloud storage integration required
- Catalog versioning is important
- Multiple users/distributed access
- Time travel queries needed

## Migration to Full Version

To migrate from filesystem to full version:

1. Export data from filesystem:
```bash
docker-compose -f docker-compose-filesystem.yml exec pyiceberg-etl \
    python -c "
import os
import shutil
shutil.make_archive('/tmp/iceberg-data', 'zip', '/data/warehouse')
"
docker cp iceberg-etl-filesystem:/tmp/iceberg-data.zip ./
```

2. Start full version:
```bash
docker-compose up -d --build
```

3. Import data to MinIO and re-catalog

## Support

This filesystem mode is designed for demonstration purposes. For production use, please refer to the full version with MinIO and Nessie integration.

## License

MIT License - Same as main project