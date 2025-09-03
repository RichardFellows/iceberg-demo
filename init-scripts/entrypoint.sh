#!/bin/bash

# Start SQL Server in the background
/opt/mssql/bin/sqlservr &

# Wait for SQL Server to be ready
echo "Waiting for SQL Server to start..."
sleep 30

# Keep trying to connect until SQL Server is ready
until /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -Q "SELECT 1" -C > /dev/null 2>&1; do
    echo "SQL Server is starting up..."
    sleep 5
done

echo "SQL Server is ready, running initialization script..."

# Run the initialization script
/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "${MSSQL_SA_PASSWORD}" -i /docker-entrypoint-initdb.d/init-db.sql -C

if [ $? -eq 0 ]; then
    echo "Database initialization completed successfully!"
else
    echo "Database initialization failed!"
fi

# Keep the container running by waiting for the SQL Server process
wait