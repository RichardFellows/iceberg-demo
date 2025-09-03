#!/usr/bin/env python3
"""
Apache Iceberg Demo Dashboard
Interactive web interface for ETL operations, Nessie metadata, and monitoring
"""

import os
import sys
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware

import pymssql
import pandas as pd
import requests
from pyiceberg.catalog import load_catalog

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="Apache Iceberg Demo Dashboard",
    description="Interactive dashboard for Apache Iceberg + Nessie + SQL Server demo",
    version="1.0.0"
)

# CORS middleware for development
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
    
    # Nessie
    NESSIE_URI = os.getenv('NESSIE_URI', 'http://nessie:19120')
    NESSIE_API_URL = f"{NESSIE_URI}/api/v1"
    NESSIE_ICEBERG_URL = f"{NESSIE_URI}/iceberg"
    
    # MinIO/S3
    S3_ENDPOINT = os.getenv('AWS_ENDPOINT_URL', 'http://minio:9000')
    S3_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    S3_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin123')
    S3_REGION = os.getenv('AWS_REGION', 'us-east-1')
    
    # Warehouse
    WAREHOUSE_PATH = os.getenv('WAREHOUSE_PATH', 's3://warehouse/')

config = Config()

# HTML Template (embedded for simplicity)
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Apache Iceberg Demo Dashboard</title>
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
            max-width: 1400px;
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
        
        .status-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 25px;
        }
        
        .status-item {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 10px;
            border-left: 4px solid #667eea;
        }
        
        .status-item.healthy {
            border-left-color: #10b981;
        }
        
        .status-item.error {
            border-left-color: #ef4444;
        }
        
        .status-item.warning {
            border-left-color: #f59e0b;
        }
        
        .status-label {
            color: #666;
            font-size: 12px;
            text-transform: uppercase;
            margin-bottom: 5px;
        }
        
        .status-value {
            color: #333;
            font-size: 20px;
            font-weight: bold;
        }
        
        .button-group {
            display: flex;
            gap: 15px;
            margin-bottom: 20px;
        }
        
        button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 24px;
            border-radius: 8px;
            font-size: 16px;
            cursor: pointer;
            transition: all 0.3s;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        button:hover {
            transform: translateY(-2px);
            box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
        }
        
        button:disabled {
            background: #ccc;
            cursor: not-allowed;
            transform: none;
        }
        
        button.secondary {
            background: #6b7280;
        }
        
        button.danger {
            background: #ef4444;
        }
        
        .progress-bar {
            width: 100%;
            height: 30px;
            background: #f3f4f6;
            border-radius: 15px;
            overflow: hidden;
            margin-bottom: 15px;
        }
        
        .progress-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            transition: width 0.3s;
            display: flex;
            align-items: center;
            justify-content: center;
            color: white;
            font-weight: bold;
        }
        
        .log-viewer {
            background: #1e293b;
            color: #10b981;
            font-family: 'Monaco', 'Courier New', monospace;
            font-size: 12px;
            padding: 15px;
            border-radius: 8px;
            height: 200px;
            overflow-y: auto;
            margin-top: 15px;
        }
        
        .table-list {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            max-height: 300px;
            overflow-y: auto;
        }
        
        .table-item {
            padding: 10px;
            margin-bottom: 10px;
            background: white;
            border-radius: 5px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .table-name {
            font-weight: bold;
            color: #333;
        }
        
        .table-stats {
            color: #666;
            font-size: 14px;
        }
        
        .spinner {
            border: 3px solid #f3f3f3;
            border-top: 3px solid #667eea;
            border-radius: 50%;
            width: 20px;
            height: 20px;
            animation: spin 1s linear infinite;
            display: inline-block;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .alert {
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        
        .alert.info {
            background: #dbeafe;
            color: #1e40af;
        }
        
        .alert.success {
            background: #d1fae5;
            color: #065f46;
        }
        
        .alert.error {
            background: #fee2e2;
            color: #991b1b;
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 15px;
            margin-top: 20px;
        }
        
        .metric-card {
            text-align: center;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 10px;
        }
        
        .metric-value {
            font-size: 36px;
            font-weight: bold;
            color: #667eea;
        }
        
        .metric-label {
            color: #666;
            margin-top: 5px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>
                🏔️ Apache Iceberg Demo Dashboard
            </h1>
            <p class="subtitle">SQL Server ↔️ Apache Iceberg ↔️ Nessie Catalog | Real-time ETL & Backload Operations</p>
        </div>
        
        <div class="dashboard">
            <!-- System Status -->
            <div class="card">
                <h2>📊 System Status</h2>
                <div class="status-grid">
                    <div class="status-item" id="mssql-status">
                        <div class="status-label">SQL Server</div>
                        <div class="status-value">Checking...</div>
                    </div>
                    <div class="status-item" id="nessie-status">
                        <div class="status-label">Nessie Catalog</div>
                        <div class="status-value">Checking...</div>
                    </div>
                    <div class="status-item" id="minio-status">
                        <div class="status-label">MinIO S3</div>
                        <div class="status-value">Checking...</div>
                    </div>
                    <div class="status-item" id="iceberg-status">
                        <div class="status-label">Iceberg Tables</div>
                        <div class="status-value">0</div>
                    </div>
                </div>
                
                <h3 style="margin-top: 25px; margin-bottom: 15px;">📈 Data Statistics</h3>
                <div class="metrics-grid">
                    <div class="metric-card">
                        <div class="metric-value" id="total-rows">0</div>
                        <div class="metric-label">Total Rows</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value" id="total-tables">0</div>
                        <div class="metric-label">Tables</div>
                    </div>
                    <div class="metric-card">
                        <div class="metric-value" id="last-sync">Never</div>
                        <div class="metric-label">Last Sync</div>
                    </div>
                </div>
            </div>
            
            <!-- ETL Operations -->
            <div class="card">
                <h2>⚙️ ETL Operations</h2>
                
                <div class="alert info">
                    <strong>ETL Pipeline:</strong> Migrates data from SQL Server → Apache Iceberg
                </div>
                
                <div class="button-group">
                    <button onclick="runETL()" id="etl-button">
                        ▶️ Run ETL Pipeline
                    </button>
                    <button onclick="runBackload()" id="backload-button" class="secondary">
                        ◀️ Run Backload
                    </button>
                </div>
                
                <div class="progress-bar">
                    <div class="progress-fill" id="etl-progress" style="width: 0%">
                        0%
                    </div>
                </div>
                
                <div id="operation-status"></div>
                
                <div class="log-viewer" id="log-viewer">
                    Ready for operations...
                </div>
            </div>
        </div>
        
        <div class="dashboard">
            <!-- Nessie Catalog -->
            <div class="card">
                <h2>🌲 Nessie Catalog Browser</h2>
                
                <div class="button-group">
                    <button onclick="refreshCatalog()">
                        🔄 Refresh Catalog
                    </button>
                    <button onclick="viewCommits()" class="secondary">
                        📜 View Commits
                    </button>
                </div>
                
                <div class="table-list" id="catalog-tables">
                    <p style="color: #666;">Loading catalog...</p>
                </div>
            </div>
            
            <!-- SQL Server Tables -->
            <div class="card">
                <h2>🗄️ SQL Server Tables</h2>
                
                <div class="button-group">
                    <button onclick="refreshSQLTables()">
                        🔄 Refresh Tables
                    </button>
                </div>
                
                <div class="table-list" id="sql-tables">
                    <p style="color: #666;">Loading tables...</p>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        let etlRunning = false;
        let backloadRunning = false;
        
        // Initialize dashboard
        document.addEventListener('DOMContentLoaded', function() {
            checkSystemStatus();
            refreshCatalog();
            refreshSQLTables();
            setInterval(checkSystemStatus, 30000); // Check every 30 seconds
            setInterval(updateJobStatus, 2000); // Update job status every 2 seconds
        });
        
        async function checkSystemStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                
                // Update status indicators
                updateStatusItem('mssql-status', data.mssql);
                updateStatusItem('nessie-status', data.nessie);
                updateStatusItem('minio-status', data.minio);
                
                // Update metrics
                document.getElementById('total-rows').textContent = data.metrics.total_rows.toLocaleString();
                document.getElementById('total-tables').textContent = data.metrics.tables;
                document.getElementById('iceberg-status').querySelector('.status-value').textContent = data.metrics.iceberg_tables;
                
                if (data.metrics.last_sync) {
                    const date = new Date(data.metrics.last_sync);
                    document.getElementById('last-sync').textContent = date.toLocaleTimeString();
                }
            } catch (error) {
                console.error('Error checking status:', error);
            }
        }
        
        function updateStatusItem(elementId, status) {
            const element = document.getElementById(elementId);
            const valueElement = element.querySelector('.status-value');
            
            element.className = 'status-item';
            
            if (status.healthy) {
                element.classList.add('healthy');
                valueElement.innerHTML = '✅ Online';
            } else {
                element.classList.add('error');
                valueElement.innerHTML = '❌ Offline';
            }
            
            if (status.message) {
                valueElement.title = status.message;
            }
        }
        
        async function runETL() {
            if (etlRunning) return;
            
            etlRunning = true;
            document.getElementById('etl-button').disabled = true;
            addLog('Starting ETL pipeline...');
            
            try {
                const response = await fetch('/api/etl/run', {
                    method: 'POST'
                });
                const data = await response.json();
                
                if (data.status === 'started') {
                    addLog('ETL pipeline started successfully');
                } else {
                    addLog('Error: ' + data.message, 'error');
                }
            } catch (error) {
                addLog('Error starting ETL: ' + error.message, 'error');
                etlRunning = false;
                document.getElementById('etl-button').disabled = false;
            }
        }
        
        async function runBackload() {
            if (backloadRunning) return;
            
            backloadRunning = true;
            document.getElementById('backload-button').disabled = true;
            addLog('Starting backload process...');
            
            try {
                const response = await fetch('/api/backload/run', {
                    method: 'POST'
                });
                const data = await response.json();
                
                if (data.status === 'started') {
                    addLog('Backload process started successfully');
                } else {
                    addLog('Error: ' + data.message, 'error');
                }
            } catch (error) {
                addLog('Error starting backload: ' + error.message, 'error');
                backloadRunning = false;
                document.getElementById('backload-button').disabled = false;
            }
        }
        
        async function updateJobStatus() {
            if (!etlRunning && !backloadRunning) return;
            
            try {
                const response = await fetch('/api/jobs/status');
                const data = await response.json();
                
                // Update ETL status
                if (data.etl.status !== 'idle') {
                    document.getElementById('etl-progress').style.width = data.etl.progress + '%';
                    document.getElementById('etl-progress').textContent = data.etl.progress + '%';
                    
                    if (data.etl.status === 'completed') {
                        etlRunning = false;
                        document.getElementById('etl-button').disabled = false;
                        addLog('ETL pipeline completed successfully', 'success');
                        checkSystemStatus();
                        refreshCatalog();
                    } else if (data.etl.status === 'error') {
                        etlRunning = false;
                        document.getElementById('etl-button').disabled = false;
                        addLog('ETL pipeline failed: ' + data.etl.message, 'error');
                    }
                }
                
                // Update backload status
                if (data.backload.status !== 'idle') {
                    document.getElementById('etl-progress').style.width = data.backload.progress + '%';
                    document.getElementById('etl-progress').textContent = data.backload.progress + '%';
                    
                    if (data.backload.status === 'completed') {
                        backloadRunning = false;
                        document.getElementById('backload-button').disabled = false;
                        addLog('Backload completed successfully', 'success');
                        checkSystemStatus();
                        refreshSQLTables();
                    } else if (data.backload.status === 'error') {
                        backloadRunning = false;
                        document.getElementById('backload-button').disabled = false;
                        addLog('Backload failed: ' + data.backload.message, 'error');
                    }
                }
            } catch (error) {
                console.error('Error updating job status:', error);
            }
        }
        
        async function refreshCatalog() {
            try {
                const response = await fetch('/api/catalog/tables');
                const data = await response.json();
                
                const container = document.getElementById('catalog-tables');
                container.innerHTML = '';
                
                if (data.tables && data.tables.length > 0) {
                    data.tables.forEach(table => {
                        const item = document.createElement('div');
                        item.className = 'table-item';
                        item.innerHTML = `
                            <div>
                                <div class="table-name">🗂️ ${table.name}</div>
                                <div class="table-stats">${table.row_count.toLocaleString()} rows</div>
                            </div>
                            <button onclick="viewTableDetails('${table.namespace}', '${table.name}')" style="padding: 5px 10px; font-size: 12px;">
                                View
                            </button>
                        `;
                        container.appendChild(item);
                    });
                } else {
                    container.innerHTML = '<p style="color: #666;">No tables in catalog</p>';
                }
            } catch (error) {
                console.error('Error refreshing catalog:', error);
            }
        }
        
        async function refreshSQLTables() {
            try {
                const response = await fetch('/api/sql/tables');
                const data = await response.json();
                
                const container = document.getElementById('sql-tables');
                container.innerHTML = '';
                
                if (data.tables && data.tables.length > 0) {
                    data.tables.forEach(table => {
                        const item = document.createElement('div');
                        item.className = 'table-item';
                        item.innerHTML = `
                            <div>
                                <div class="table-name">📊 ${table.schema}.${table.name}</div>
                                <div class="table-stats">${table.row_count.toLocaleString()} rows | ${table.columns} columns</div>
                            </div>
                        `;
                        container.appendChild(item);
                    });
                } else {
                    container.innerHTML = '<p style="color: #666;">No tables found</p>';
                }
            } catch (error) {
                console.error('Error refreshing SQL tables:', error);
            }
        }
        
        async function viewTableDetails(namespace, tableName) {
            addLog(`Viewing details for ${namespace}.${tableName}`);
            // TODO: Implement table details view
        }
        
        async function viewCommits() {
            try {
                const response = await fetch('/api/nessie/commits');
                const data = await response.json();
                
                if (data.commits && data.commits.length > 0) {
                    addLog('Recent Nessie commits:');
                    data.commits.slice(0, 5).forEach(commit => {
                        addLog(`  ${commit.hash.substring(0, 8)} - ${commit.message || 'No message'}`);
                    });
                } else {
                    addLog('No commits found in Nessie');
                }
            } catch (error) {
                addLog('Error fetching commits: ' + error.message, 'error');
            }
        }
        
        function addLog(message, type = 'info') {
            const logViewer = document.getElementById('log-viewer');
            const timestamp = new Date().toLocaleTimeString();
            const logEntry = document.createElement('div');
            
            let prefix = '>';
            let color = '#10b981';
            if (type === 'error') {
                prefix = '✗';
                color = '#ef4444';
            } else if (type === 'success') {
                prefix = '✓';
                color = '#10b981';
            }
            
            logEntry.style.color = color;
            logEntry.textContent = `[${timestamp}] ${prefix} ${message}`;
            logViewer.appendChild(logEntry);
            logViewer.scrollTop = logViewer.scrollHeight;
        }
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
async def dashboard():
    """Serve the main dashboard page"""
    return HTML_TEMPLATE

@app.get("/api/status")
async def get_system_status():
    """Check status of all system components"""
    status = {
        "mssql": {"healthy": False, "message": ""},
        "nessie": {"healthy": False, "message": ""},
        "minio": {"healthy": False, "message": ""},
        "metrics": {
            "total_rows": 0,
            "tables": 0,
            "iceberg_tables": 0,
            "last_sync": job_status["etl"]["last_run"]
        }
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
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM sales.customers")
        status["mssql"]["healthy"] = True
        status["mssql"]["message"] = "Connected to SQL Server"
        
        # Get metrics
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM sales.customers) +
                (SELECT COUNT(*) FROM sales.products) +
                (SELECT COUNT(*) FROM sales.transactions) +
                (SELECT COUNT(*) FROM sales.order_details) +
                (SELECT COUNT(*) FROM sales.inventory_snapshots) as total_rows
        """)
        status["metrics"]["total_rows"] = cursor.fetchone()[0]
        status["metrics"]["tables"] = 5
        
        cursor.close()
        conn.close()
    except Exception as e:
        status["mssql"]["message"] = str(e)
    
    # Check Nessie
    try:
        response = requests.get(f"{config.NESSIE_API_URL}/trees", timeout=5)
        if response.status_code == 200:
            status["nessie"]["healthy"] = True
            status["nessie"]["message"] = "Nessie catalog online"
    except Exception as e:
        status["nessie"]["message"] = str(e)
    
    # Check MinIO
    try:
        response = requests.get(f"{config.S3_ENDPOINT}/minio/health/live", timeout=5)
        if response.status_code == 200:
            status["minio"]["healthy"] = True
            status["minio"]["message"] = "MinIO S3 storage online"
    except Exception as e:
        status["minio"]["message"] = str(e)
    
    # Count Iceberg tables
    try:
        catalog_config = {
            'type': 'rest',
            'uri': config.NESSIE_ICEBERG_URL,
            's3.endpoint': config.S3_ENDPOINT,
            's3.access-key-id': config.S3_ACCESS_KEY,
            's3.secret-access-key': config.S3_SECRET_KEY,
            's3.path-style-access': 'true',
            's3.region': config.S3_REGION,
        }
        catalog = load_catalog('nessie_rest', **catalog_config)
        namespaces = catalog.list_namespaces()
        table_count = 0
        for namespace in namespaces:
            tables = catalog.list_tables(namespace)
            table_count += len(tables)
        status["metrics"]["iceberg_tables"] = table_count
    except:
        pass
    
    return JSONResponse(content=status)

@app.get("/api/catalog/tables")
async def get_catalog_tables():
    """Get list of tables in Iceberg catalog"""
    try:
        catalog_config = {
            'type': 'rest',
            'uri': config.NESSIE_ICEBERG_URL,
            's3.endpoint': config.S3_ENDPOINT,
            's3.access-key-id': config.S3_ACCESS_KEY,
            's3.secret-access-key': config.S3_SECRET_KEY,
            's3.path-style-access': 'true',
            's3.region': config.S3_REGION,
        }
        catalog = load_catalog('nessie_rest', **catalog_config)
        
        tables_list = []
        namespaces = catalog.list_namespaces()
        
        for namespace in namespaces:
            tables = catalog.list_tables(namespace)
            for table_id in tables:
                # For now, just return basic info
                tables_list.append({
                    "namespace": '.'.join(namespace),
                    "name": table_id[1],
                    "row_count": 0  # Would need to scan table for actual count
                })
        
        return JSONResponse(content={"tables": tables_list})
    except Exception as e:
        logger.error(f"Error getting catalog tables: {e}")
        return JSONResponse(content={"tables": [], "error": str(e)})

@app.get("/api/sql/tables")
async def get_sql_tables():
    """Get list of tables in SQL Server"""
    try:
        conn = pymssql.connect(
            server=config.MSSQL_HOST,
            port=config.MSSQL_PORT,
            user=config.MSSQL_USER,
            password=config.MSSQL_PASSWORD,
            database='IcebergDemoSource'
        )
        cursor = conn.cursor()
        
        # Get table information
        cursor.execute("""
            SELECT 
                s.name as schema_name,
                t.name as table_name,
                p.rows as row_count,
                COUNT(c.column_id) as column_count
            FROM sys.tables t
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            INNER JOIN sys.partitions p ON t.object_id = p.object_id
            INNER JOIN sys.columns c ON t.object_id = c.object_id
            WHERE p.index_id IN (0, 1)
            GROUP BY s.name, t.name, p.rows
            ORDER BY s.name, t.name
        """)
        
        tables = []
        for row in cursor:
            tables.append({
                "schema": row[0],
                "name": row[1],
                "row_count": row[2],
                "columns": row[3]
            })
        
        cursor.close()
        conn.close()
        
        return JSONResponse(content={"tables": tables})
    except Exception as e:
        logger.error(f"Error getting SQL tables: {e}")
        return JSONResponse(content={"tables": [], "error": str(e)})

@app.get("/api/nessie/commits")
async def get_nessie_commits():
    """Get recent commits from Nessie"""
    try:
        response = requests.get(f"{config.NESSIE_API_URL}/trees/tree/main/history")
        if response.status_code == 200:
            data = response.json()
            commits = []
            for entry in data.get("logEntries", [])[:10]:  # Last 10 commits
                commit_meta = entry.get("commitMeta", {})
                commits.append({
                    "hash": commit_meta.get("hash", ""),
                    "message": commit_meta.get("message", ""),
                    "author": commit_meta.get("author", ""),
                    "timestamp": commit_meta.get("commitTime", "")
                })
            return JSONResponse(content={"commits": commits})
    except Exception as e:
        logger.error(f"Error getting Nessie commits: {e}")
    
    return JSONResponse(content={"commits": []})

@app.post("/api/etl/run")
async def run_etl_pipeline(background_tasks: BackgroundTasks):
    """Start ETL pipeline in background"""
    if job_status["etl"]["status"] == "running":
        return JSONResponse(
            content={"status": "error", "message": "ETL already running"},
            status_code=400
        )
    
    background_tasks.add_task(execute_etl)
    job_status["etl"]["status"] = "starting"
    job_status["etl"]["message"] = "Initializing ETL pipeline"
    
    return JSONResponse(content={"status": "started"})

@app.post("/api/backload/run")
async def run_backload_pipeline(background_tasks: BackgroundTasks):
    """Start backload pipeline in background"""
    if job_status["backload"]["status"] == "running":
        return JSONResponse(
            content={"status": "error", "message": "Backload already running"},
            status_code=400
        )
    
    background_tasks.add_task(execute_backload)
    job_status["backload"]["status"] = "starting"
    job_status["backload"]["message"] = "Initializing backload pipeline"
    
    return JSONResponse(content={"status": "started"})

@app.get("/api/jobs/status")
async def get_job_status():
    """Get status of running jobs"""
    return JSONResponse(content=job_status)

async def execute_etl():
    """Execute ETL pipeline"""
    try:
        job_status["etl"]["status"] = "running"
        job_status["etl"]["progress"] = 10
        
        # Add the ETL modules path
        etl_path = '/app/etl_modules'
        if etl_path not in sys.path:
            sys.path.insert(0, etl_path)
        
        # Import and run ETL script
        import etl_script
        
        job_status["etl"]["progress"] = 50
        job_status["etl"]["message"] = "Running ETL pipeline"
        
        # Run ETL
        etl = etl_script.SQLServerToIcebergETL()
        etl.run_migration()
        
        job_status["etl"]["status"] = "completed"
        job_status["etl"]["progress"] = 100
        job_status["etl"]["message"] = "ETL completed successfully"
        job_status["etl"]["last_run"] = datetime.now().isoformat()
        
    except Exception as e:
        job_status["etl"]["status"] = "error"
        job_status["etl"]["message"] = str(e)
        job_status["etl"]["progress"] = 0
        logger.error(f"ETL failed: {e}")

async def execute_backload():
    """Execute backload pipeline"""
    try:
        job_status["backload"]["status"] = "running"
        job_status["backload"]["progress"] = 10
        
        # Add the ETL modules path
        etl_path = '/app/etl_modules'
        if etl_path not in sys.path:
            sys.path.insert(0, etl_path)
        
        # Import and run backload script
        import backload_script
        
        job_status["backload"]["progress"] = 50
        job_status["backload"]["message"] = "Running backload pipeline"
        
        # Run backload
        backload = backload_script.IcebergToSQLServerBackload()
        backload.run_backload()
        
        job_status["backload"]["status"] = "completed"
        job_status["backload"]["progress"] = 100
        job_status["backload"]["message"] = "Backload completed successfully"
        job_status["backload"]["last_run"] = datetime.now().isoformat()
        
    except Exception as e:
        job_status["backload"]["status"] = "error"
        job_status["backload"]["message"] = str(e)
        job_status["backload"]["progress"] = 0
        logger.error(f"Backload failed: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)