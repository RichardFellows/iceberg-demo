FROM python:3.11-slim

WORKDIR /app

# Install system dependencies required for pyodbc and pymssql
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Set Python path
ENV PYTHONPATH=/app

# Default command (can be overridden)
CMD ["python", "etl_script.py"]