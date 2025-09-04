#!/usr/bin/env python3
"""
Apache Iceberg Demo Dashboard - Filesystem Mode
Simplified dashboard for filesystem-based Iceberg demo without MinIO/Nessie
"""

import os
import sys
import json
import asyncio
import logging
import glob
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware

import pymssql
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="Apache Iceberg Demo Dashboard - Filesystem Mode",
    description="Simplified dashboard for filesystem-based Iceberg demo",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global state for job tracking
job_status = {
    "etl": {"status": "idle", "message": "", "progress": 0, "last_run": None},
    "backload": {"status": "idle", "message": "", "progress": 0, "last_run": None}
}

# Configuration
class Config:
    # SQL Server
    MSSQL_HOST = os.getenv('MSSQL_HOST', 'mssql')
    MSSQL_PORT = int(os.getenv('MSSQL_PORT', 1433))
    MSSQL_USER = os.getenv('MSSQL_USER', 'sa')
    MSSQL_PASSWORD = os.getenv('MSSQL_PASSWORD', 'Strong@Password123')
    
    # Warehouse
    WAREHOUSE_PATH = os.getenv('WAREHOUSE_PATH', '/data/warehouse')

config = Config()

# HTML Template (simplified for filesystem mode)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Apache Iceberg Demo - Filesystem Mode</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        
        h1 {
            color: #333;
            margin-bottom: 10px;
            display: flex;
            align-items: center;
            gap: 15px;
        }
        
        .subtitle {
            color: #666;
            font-size: 16px;
        }
        
        .mode-badge {
            background: #4CAF50;
            color: white;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 14px;
            font-weight: 500;
        }
        
        .dashboard {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 30px;
            margin-bottom: 30px;
        }
        
        .card {
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
        }
        
        .card h2 {
            color: #333;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .status {
            display: flex;
            align-items: center;
            gap: 10px;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }
        
        .status:last-child {
            border-bottom: none;
        }
        
        .status-indicator {
            width: 12px;
            height: 12px;
            border-radius: 50%;
        }
        
        .status-healthy {
            background: #4CAF50;
        }
        
        .status-unhealthy {
            background: #f44336;
        }
        
        .status-unknown {
            background: #FFC107;
        }
        
        .btn {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 500;
            transition: transform 0.2s;
            margin-right: 10px;
        }
        
        .btn:hover {
            transform: translateY(-2px);
        }
        
        .btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .progress {
            width: 100%;
            height: 25px;
            background: #f0f0f0;
            border-radius: 12px;
            overflow: hidden;
            margin: 15px 0;
        }
        
        .progress-bar {
            height: 100%;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s ease;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-size: 12px;
        }
        
        .log-viewer {
            background: #1e1e1e;
            color: #00ff00;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            padding: 15px;
            border-radius: 8px;
            height: 200px;
            overflow-y: auto;
            margin-top: 15px;
        }
        
        .table-list {
            max-height: 300px;
            overflow-y: auto;
        }
        
        .table-item {
            padding: 10px;
            background: #f8f9fa;
            margin-bottom: 5px;
            border-radius: 5px;
            display: flex;
            justify-content: space-between;
        }
        
        .row-count {
            color: #666;
            font-size: 14px;
        }
        
        .full-width {
            grid-column: 1 / -1;
        }
        
        .warning-box {
            background: #FFF3E0;
            border-left: 4px solid #FF9800;
            padding: 15px;
            margin: 15px 0;
            border-radius: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>
                🏔️ Apache Iceberg Demo Dashboard
                <span class="mode-badge">Filesystem Mode</span>
            </h1>
            <p class="subtitle">SQL Server ↔ Apache Iceberg Data Flow (No MinIO/Nessie Dependencies)</p>
        </div>
        
        <div class="dashboard">
            <!-- System Status -->
            <div class="card">
                <h2>🎛️ System Status</h2>
                <div class="warning-box">
                    <strong>Filesystem Mode:</strong> Using local storage without S3 or catalog versioning
                </div>
                <div id="system-status">
                    <div class="status">
                        <span class="status-indicator status-unknown"></span>
                        <span>SQL Server: Checking...</span>
                    </div>
                    <div class="status">
                        <span class="status-indicator status-healthy"></span>
                        <span>Filesystem Storage: Ready</span>
                    </div>
                    <div class="status">
                        <span class="status-indicator status-unknown"></span>
                        <span>Iceberg Catalog: Checking...</span>
                    </div>
                </div>
            </div>
            
            <!-- ETL Controls -->
            <div class="card">
                <h2>⚡ ETL Controls</h2>
                <button class="btn" id="run-etl" onclick="runETL()">
                    ▶️ Run ETL Pipeline
                </button>
                <button class="btn" id="run-backload" onclick="runBackload()">
                    ◀️ Run Backload
                </button>
                <div id="progress-container" style="display:none;">
                    <div class="progress">
                        <div class="progress-bar" id="progress-bar" style="width: 0%">0%</div>
                    </div>
                </div>
                <div id="status-message"></div>
            </div>
            
            <!-- SQL Server Tables -->
            <div class="card">
                <h2>🗄️ SQL Server Tables</h2>
                <div class="table-list" id="sql-tables">
                    <p>Loading...</p>
                </div>
            </div>
            
            <!-- Iceberg Tables -->
            <div class="card">
                <h2>❄️ Iceberg Tables (Filesystem)</h2>
                <div class="table-list" id="iceberg-tables">
                    <p>No tables yet. Run ETL to create.</p>
                </div>
            </div>
            
            <!-- Operation Log -->
            <div class="card full-width">
                <h2>📋 Operation Log</h2>
                <div class="log-viewer" id="log-viewer">
                    System ready. Waiting for operations...
                </div>
            </div>
        </div>
    </div>
    
    <script>
        let currentJob = null;
        
        async function updateStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                
                // Update system status
                const statusHtml = `
                    <div class="status">
                        <span class="status-indicator ${data.sql_server ? 'status-healthy' : 'status-unhealthy'}"></span>
                        <span>SQL Server: ${data.sql_server ? 'Connected' : 'Disconnected'}</span>
                    </div>
                    <div class="status">
                        <span class="status-indicator status-healthy"></span>
                        <span>Filesystem Storage: ${data.warehouse_path || '/data/warehouse'}</span>
                    </div>
                    <div class="status">
                        <span class="status-indicator ${data.catalog_exists ? 'status-healthy' : 'status-unknown'}"></span>
                        <span>Iceberg Catalog: ${data.catalog_exists ? 'Initialized' : 'Not initialized (Run ETL first)'}</span>
                    </div>
                `;
                document.getElementById('system-status').innerHTML = statusHtml;
                
                // Update SQL tables
                if (data.sql_tables && data.sql_tables.length > 0) {
                    const tablesHtml = data.sql_tables.map(table => 
                        `<div class="table-item">
                            <span>${table.name}</span>
                            <span class="row-count">${table.row_count.toLocaleString()} rows</span>
                        </div>`
                    ).join('');
                    document.getElementById('sql-tables').innerHTML = tablesHtml;
                }
                
                // Update Iceberg tables
                if (data.iceberg_tables && data.iceberg_tables.length > 0) {
                    const icebergHtml = data.iceberg_tables.map(table => 
                        `<div class="table-item">
                            <span>${table}</span>
                        </div>`
                    ).join('');
                    document.getElementById('iceberg-tables').innerHTML = icebergHtml;
                } else {
                    document.getElementById('iceberg-tables').innerHTML = '<p>No tables yet. Run ETL to create.</p>';
                }
                
            } catch (error) {
                console.error('Failed to update status:', error);
            }
        }
        
        async function runETL() {
            document.getElementById('run-etl').disabled = true;
            document.getElementById('run-backload').disabled = true;
            document.getElementById('progress-container').style.display = 'block';
            currentJob = 'etl';
            
            appendLog('Starting ETL pipeline (Filesystem Mode)...');
            
            try {
                const response = await fetch('/api/etl/run', { method: 'POST' });
                const data = await response.json();
                
                if (data.status === 'started') {
                    pollJobStatus();
                } else {
                    appendLog('Failed to start ETL: ' + data.message);
                    resetButtons();
                }
            } catch (error) {
                appendLog('Error starting ETL: ' + error.message);
                resetButtons();
            }
        }
        
        async function runBackload() {
            document.getElementById('run-etl').disabled = true;
            document.getElementById('run-backload').disabled = true;
            document.getElementById('progress-container').style.display = 'block';
            currentJob = 'backload';
            
            appendLog('Starting backload pipeline (Filesystem Mode)...');
            
            try {
                const response = await fetch('/api/backload/run', { method: 'POST' });
                const data = await response.json();
                
                if (data.status === 'started') {
                    pollJobStatus();
                } else {
                    appendLog('Failed to start backload: ' + data.message);
                    resetButtons();
                }
            } catch (error) {
                appendLog('Error starting backload: ' + error.message);
                resetButtons();
            }
        }
        
        async function pollJobStatus() {
            if (!currentJob) return;
            
            try {
                const response = await fetch(`/api/${currentJob}/status`);
                const data = await response.json();
                
                // Update progress bar
                const progressBar = document.getElementById('progress-bar');
                progressBar.style.width = data.progress + '%';
                progressBar.textContent = data.progress + '%';
                
                // Update status message
                document.getElementById('status-message').textContent = data.message;
                
                if (data.message) {
                    appendLog(data.message);
                }
                
                if (data.status === 'running') {
                    setTimeout(pollJobStatus, 1000);
                } else {
                    if (data.status === 'completed') {
                        appendLog(`✓ ${currentJob.toUpperCase()} completed successfully`);
                    } else if (data.status === 'failed') {
                        appendLog(`✗ ${currentJob.toUpperCase()} failed: ${data.message}`);
                    }
                    resetButtons();
                    updateStatus();
                }
            } catch (error) {
                appendLog('Error polling job status: ' + error.message);
                resetButtons();
            }
        }
        
        function resetButtons() {
            document.getElementById('run-etl').disabled = false;
            document.getElementById('run-backload').disabled = false;
            currentJob = null;
        }
        
        function appendLog(message) {
            const logViewer = document.getElementById('log-viewer');
            const timestamp = new Date().toLocaleTimeString();
            logViewer.innerHTML += `[${timestamp}] ${message}\n`;
            logViewer.scrollTop = logViewer.scrollHeight;
        }
        
        // Initialize
        updateStatus();
        setInterval(updateStatus, 30000); // Update every 30 seconds
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the dashboard HTML."""
    return HTML_TEMPLATE

@app.get("/api/status")
async def get_status():
    """Get current system status."""
    status = {
        "sql_server": False,
        "catalog_exists": False,
        "warehouse_path": config.WAREHOUSE_PATH,
        "sql_tables": [],
        "iceberg_tables": []
    }
    
    # Check SQL Server
    try:
        conn = pymssql.connect(
            server=config.MSSQL_HOST,
            port=config.MSSQL_PORT,
            user=config.MSSQL_USER,
            password=config.MSSQL_PASSWORD,
            database='IcebergDemoSource'
        )
        cursor = conn.cursor(as_dict=True)
        
        # Get table counts
        cursor.execute("""
            SELECT 'customers' as name, COUNT(*) as row_count FROM sales.customers
            UNION ALL SELECT 'products', COUNT(*) FROM sales.products
            UNION ALL SELECT 'transactions', COUNT(*) FROM sales.transactions
            UNION ALL SELECT 'order_details', COUNT(*) FROM sales.order_details
            UNION ALL SELECT 'inventory_snapshots', COUNT(*) FROM sales.inventory_snapshots
        """)
        
        status["sql_tables"] = cursor.fetchall()
        status["sql_server"] = True
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"SQL Server check failed: {e}")
    
    # Check Iceberg catalog
    catalog_db = os.path.join(config.WAREHOUSE_PATH, 'catalog.db')
    status["catalog_exists"] = os.path.exists(catalog_db)
    
    # List Iceberg tables (check filesystem)
    if os.path.exists(config.WAREHOUSE_PATH):
        sales_path = os.path.join(config.WAREHOUSE_PATH, 'sales')
        if os.path.exists(sales_path):
            tables = [d for d in os.listdir(sales_path) if os.path.isdir(os.path.join(sales_path, d))]
            status["iceberg_tables"] = tables
    
    return JSONResponse(status)

async def execute_etl():
    """Execute the ETL process."""
    global job_status
    
    try:
        job_status["etl"]["status"] = "running"
        job_status["etl"]["progress"] = 10
        job_status["etl"]["message"] = "Initializing ETL..."
        
        # Add etl_modules to path
        etl_path = '/app/etl_modules'
        if etl_path not in sys.path:
            sys.path.insert(0, etl_path)
        
        # Import and run ETL
        import etl_script_filesystem
        
        job_status["etl"]["progress"] = 30
        job_status["etl"]["message"] = "Connecting to SQL Server..."
        
        etl = etl_script_filesystem.SQLServerToIcebergETL()
        etl.connect_mssql()
        
        job_status["etl"]["progress"] = 40
        job_status["etl"]["message"] = "Initializing filesystem catalog..."
        
        etl.initialize_catalog()
        
        # Define tables
        tables = [
            {'schema': 'sales', 'table_name': 'customers', 'partition_by': None},
            {'schema': 'sales', 'table_name': 'products', 'partition_by': ['category']},
            {'schema': 'sales', 'table_name': 'transactions', 'partition_by': ['transaction_date']},
            {'schema': 'sales', 'table_name': 'order_details', 'partition_by': None},
            {'schema': 'sales', 'table_name': 'inventory_snapshots', 'partition_by': ['snapshot_date', 'warehouse_id']},
        ]
        
        progress_per_table = 50 / len(tables)
        
        for i, table_config in enumerate(tables):
            job_status["etl"]["message"] = f"Migrating {table_config['table_name']}..."
            result = etl.migrate_table(**table_config)
            
            if result['status'] == 'success':
                job_status["etl"]["message"] = f"✓ Migrated {result['table']}: {result['rows_migrated']} rows"
            
            job_status["etl"]["progress"] = int(40 + (i + 1) * progress_per_table)
        
        if etl.mssql_conn:
            etl.mssql_conn.close()
        
        job_status["etl"]["status"] = "completed"
        job_status["etl"]["progress"] = 100
        job_status["etl"]["message"] = "ETL completed successfully"
        job_status["etl"]["last_run"] = datetime.now().isoformat()
        
    except Exception as e:
        job_status["etl"]["status"] = "failed"
        job_status["etl"]["message"] = str(e)
        logger.error(f"ETL failed: {e}")

@app.post("/api/etl/run")
async def run_etl(background_tasks: BackgroundTasks):
    """Start the ETL process."""
    if job_status["etl"]["status"] == "running":
        return JSONResponse({"status": "already_running", "message": "ETL is already running"})
    
    background_tasks.add_task(execute_etl)
    return JSONResponse({"status": "started", "message": "ETL process started"})

@app.get("/api/etl/status")
async def get_etl_status():
    """Get ETL job status."""
    return JSONResponse(job_status["etl"])

async def execute_backload():
    """Execute the backload process."""
    global job_status
    
    try:
        job_status["backload"]["status"] = "running"
        job_status["backload"]["progress"] = 10
        job_status["backload"]["message"] = "Initializing backload..."
        
        # Add etl_modules to path
        etl_path = '/app/etl_modules'
        if etl_path not in sys.path:
            sys.path.insert(0, etl_path)
        
        # Import and run backload
        import backload_script_filesystem
        
        job_status["backload"]["progress"] = 30
        job_status["backload"]["message"] = "Connecting to SQL Server..."
        
        backload = backload_script_filesystem.IcebergToSQLServerBackload()
        backload.connect_mssql()
        
        job_status["backload"]["progress"] = 40
        job_status["backload"]["message"] = "Initializing filesystem catalog..."
        
        backload.initialize_catalog()
        
        # Define tables
        tables = ['customers', 'products', 'transactions', 'order_details', 'inventory_snapshots']
        
        progress_per_table = 50 / len(tables)
        
        for i, table_name in enumerate(tables):
            job_status["backload"]["message"] = f"Backloading {table_name}..."
            result = backload.backload_table('sales', table_name)
            
            if result['status'] == 'success':
                job_status["backload"]["message"] = f"✓ Backloaded {result['source_table']}: {result['rows_loaded']} rows"
            
            job_status["backload"]["progress"] = int(40 + (i + 1) * progress_per_table)
        
        if backload.mssql_conn:
            backload.mssql_conn.close()
        
        job_status["backload"]["status"] = "completed"
        job_status["backload"]["progress"] = 100
        job_status["backload"]["message"] = "Backload completed successfully"
        job_status["backload"]["last_run"] = datetime.now().isoformat()
        
    except Exception as e:
        job_status["backload"]["status"] = "failed"
        job_status["backload"]["message"] = str(e)
        logger.error(f"Backload failed: {e}")

@app.post("/api/backload/run")
async def run_backload(background_tasks: BackgroundTasks):
    """Start the backload process."""
    if job_status["backload"]["status"] == "running":
        return JSONResponse({"status": "already_running", "message": "Backload is already running"})
    
    background_tasks.add_task(execute_backload)
    return JSONResponse({"status": "started", "message": "Backload process started"})

@app.get("/api/backload/status")
async def get_backload_status():
    """Get backload job status."""
    return JSONResponse(job_status["backload"])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)