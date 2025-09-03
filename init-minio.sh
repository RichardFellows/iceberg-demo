#!/bin/bash

# Wait for MinIO to be ready
echo "Waiting for MinIO to start..."
sleep 10

# Check if MinIO is accessible
until curl -f http://localhost:9000/minio/health/live; do
    echo "MinIO is starting up..."
    sleep 5
done

echo "MinIO is ready, creating warehouse bucket..."

# Install MinIO client if not present
if ! command -v mc &> /dev/null; then
    echo "Installing MinIO client..."
    curl https://dl.min.io/client/mc/release/linux-amd64/mc \
        --create-dirs \
        -o /usr/local/bin/mc
    chmod +x /usr/local/bin/mc
fi

# Configure MinIO client
mc alias set local http://localhost:9000 minioadmin minioadmin123

# Create warehouse bucket
mc mb local/warehouse --ignore-existing

# Set bucket policy to public for demo (not for production)
mc anonymous set public local/warehouse

echo "MinIO setup completed successfully!"
echo "MinIO Console available at: http://localhost:9001"
echo "Credentials: minioadmin / minioadmin123"