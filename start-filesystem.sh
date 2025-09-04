#!/bin/bash

# Start script for filesystem-only Apache Iceberg demo
# This script starts the demo without MinIO or Nessie dependencies

echo "=========================================="
echo "Apache Iceberg Demo - Filesystem Mode"
echo "=========================================="
echo ""
echo "This mode uses local filesystem storage without:"
echo "  - MinIO (S3-compatible storage)"
echo "  - Nessie (catalog versioning)"
echo ""

# Create necessary directories
echo "Creating data directories..."
mkdir -p data/warehouse
chmod -R 777 data/warehouse

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "Error: docker-compose is not installed"
    echo "Please install Docker Compose first"
    exit 1
fi

# Stop any existing containers
echo "Stopping any existing containers..."
docker-compose -f docker-compose-filesystem.yml down 2>/dev/null

# Build and start services
echo "Building and starting services..."
docker-compose -f docker-compose-filesystem.yml up -d --build

# Wait for services
echo ""
echo "Waiting for services to be ready..."
echo "  - SQL Server initialization may take 60-90 seconds"
echo "  - Dashboard will be available at http://localhost:8000"
echo ""

# Check service health
for i in {1..30}; do
    echo -n "."
    sleep 3
    
    # Check if SQL Server is healthy
    if docker-compose -f docker-compose-filesystem.yml ps | grep -q "healthy"; then
        echo ""
        echo ""
        echo "✓ Services are ready!"
        break
    fi
    
    if [ $i -eq 30 ]; then
        echo ""
        echo ""
        echo "⚠ Services are taking longer than expected to start"
        echo "Check logs with: docker-compose -f docker-compose-filesystem.yml logs"
    fi
done

# Display status
echo ""
echo "=========================================="
echo "Service Status:"
echo "=========================================="
docker-compose -f docker-compose-filesystem.yml ps

echo ""
echo "=========================================="
echo "Access Points:"
echo "=========================================="
echo "  Dashboard:   http://localhost:8000"
echo "  SQL Server:  localhost:1433 (sa / Strong@Password123)"
echo "  Data Path:   ./data/warehouse"
echo ""
echo "=========================================="
echo "Quick Start:"
echo "=========================================="
echo "1. Open the dashboard at http://localhost:8000"
echo "2. Click 'Run ETL Pipeline' to migrate data to Iceberg"
echo "3. Click 'Run Backload' to restore data to SQL Server"
echo ""
echo "Or run manually:"
echo "  ETL:      docker-compose -f docker-compose-filesystem.yml exec pyiceberg-etl python etl_script_filesystem.py"
echo "  Backload: docker-compose -f docker-compose-filesystem.yml exec pyiceberg-etl python backload_script_filesystem.py"
echo ""
echo "To stop:    ./stop-filesystem.sh"
echo "To view logs: docker-compose -f docker-compose-filesystem.yml logs -f"
echo ""