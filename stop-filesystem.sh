#!/bin/bash

# Stop script for filesystem-only Apache Iceberg demo

echo "=========================================="
echo "Stopping Apache Iceberg Demo - Filesystem Mode"
echo "=========================================="
echo ""

# Stop containers
echo "Stopping containers..."
docker-compose -f docker-compose-filesystem.yml down

echo ""
echo "✓ All services stopped"
echo ""
echo "Note: Data is preserved in ./data/warehouse"
echo "To completely remove data: rm -rf data/warehouse"
echo ""