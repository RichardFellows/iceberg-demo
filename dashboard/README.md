# Apache Iceberg Demo Dashboard

A modern, interactive web dashboard for demonstrating Apache Iceberg functionality with SQL Server, Nessie catalog, and MinIO S3 storage.

## Features

### 🎯 Real-Time System Monitoring
- **Service Health Checks**: Live status monitoring for SQL Server, Nessie, and MinIO
- **Data Metrics**: Total rows, table counts, and last sync timestamps
- **Auto-refresh**: Automatic status updates every 30 seconds

### ⚙️ ETL Pipeline Control
- **One-Click ETL**: Run SQL Server → Apache Iceberg migration with a single button
- **Backload Operations**: Restore data from Iceberg back to SQL Server
- **Progress Tracking**: Real-time progress bars and status updates
- **Live Logs**: Streaming log viewer for operation monitoring

### 🌲 Catalog Management
- **Nessie Catalog Browser**: View all Iceberg tables and their metadata
- **Commit History**: Browse Nessie version control commits
- **SQL Server Tables**: Monitor source database tables and row counts
- **Table Statistics**: Real-time row counts and schema information

### 🎨 Modern UI Design
- **Responsive Layout**: Works on desktop and mobile devices
- **Dark-themed Log Viewer**: Terminal-style operation logs
- **Visual Status Indicators**: Color-coded health checks and alerts
- **Interactive Controls**: Enable/disable buttons based on system state

## Architecture

```
┌─────────────────┐
│   Web Browser   │
│  (Dashboard UI) │
└────────┬────────┘
         │ HTTP/WebSocket
         ▼
┌─────────────────┐
│  FastAPI Server │
│   (Python 3.11) │
└────────┬────────┘
         │
    ┌────┴────┬──────────┬─────────┐
    ▼         ▼          ▼         ▼
┌────────┐ ┌──────┐ ┌────────┐ ┌─────┐
│SQL     │ │Nessie│ │MinIO S3│ │ ETL │
│Server  │ │ REST │ │Storage │ │Jobs │
└────────┘ └──────┘ └────────┘ └─────┘
```

## API Endpoints

### System Status
- `GET /` - Main dashboard HTML interface
- `GET /api/status` - System health and metrics

### Data Operations
- `POST /api/etl/run` - Start ETL pipeline
- `POST /api/backload/run` - Start backload process
- `GET /api/jobs/status` - Get running job status

### Catalog Management
- `GET /api/catalog/tables` - List Iceberg tables
- `GET /api/sql/tables` - List SQL Server tables
- `GET /api/nessie/commits` - Get Nessie commit history

## Usage

### Starting the Dashboard

The dashboard is automatically started as part of the docker-compose stack:

```bash
docker-compose up -d dashboard
```

### Accessing the Dashboard

Open your browser and navigate to:

```
http://localhost:8000
```

### Running ETL Operations

1. **Check System Status**: Ensure all services show green/healthy status
2. **Run ETL Pipeline**: Click "▶️ Run ETL Pipeline" to migrate data from SQL Server to Iceberg
3. **Monitor Progress**: Watch the progress bar and log viewer for real-time updates
4. **Verify Results**: Check the Nessie Catalog Browser for new Iceberg tables

### Running Backload Operations

1. **Ensure ETL Completed**: First run ETL to populate Iceberg tables
2. **Run Backload**: Click "◀️ Run Backload" to restore data to SQL Server
3. **Monitor Progress**: Track operation in the log viewer
4. **Verify Results**: Check SQL Server tables for restored data

## Development

### Local Development

For local development with hot-reload:

```bash
cd dashboard
pip install -r requirements.txt
uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

### Adding New Features

The dashboard uses:
- **Backend**: FastAPI with async/await patterns
- **Frontend**: Vanilla JavaScript with modern ES6+ features
- **Styling**: Embedded CSS with gradient designs
- **Data**: REST API with JSON responses

### Customization

Modify the embedded HTML template in `app.py` to customize:
- UI colors and styling
- Dashboard layout
- Polling intervals
- Log retention

## Environment Variables

The dashboard respects the following environment variables:

- `MSSQL_HOST`: SQL Server hostname (default: mssql)
- `MSSQL_PORT`: SQL Server port (default: 1433)
- `MSSQL_USER`: SQL Server username (default: sa)
- `MSSQL_PASSWORD`: SQL Server password
- `NESSIE_URI`: Nessie catalog URI (default: http://nessie:19120)
- `AWS_ENDPOINT_URL`: MinIO S3 endpoint (default: http://minio:9000)
- `AWS_ACCESS_KEY_ID`: MinIO access key
- `AWS_SECRET_ACCESS_KEY`: MinIO secret key
- `AWS_REGION`: AWS region (default: us-east-1)

## Troubleshooting

### Dashboard Not Loading
- Check if port 8000 is available: `docker-compose ps dashboard`
- Verify all dependencies are healthy: `docker-compose ps`
- Check logs: `docker-compose logs dashboard`

### ETL/Backload Not Running
- Ensure all services are healthy in the status panel
- Check if another job is already running
- Review error messages in the log viewer

### Connection Errors
- Verify network connectivity between containers
- Check environment variables in docker-compose.yml
- Ensure all services are on the same Docker network

## Security Notes

⚠️ **Development Only**: This dashboard is designed for demonstration purposes and includes:
- No authentication/authorization
- CORS enabled for all origins
- Default credentials visible in UI
- Direct database access

For production use, implement proper security measures including authentication, HTTPS, and credential management.