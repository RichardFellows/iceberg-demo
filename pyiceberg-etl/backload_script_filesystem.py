#!/usr/bin/env python3
"""
Filesystem Backload Script: Restore data from Apache Iceberg tables back to SQL Server.
Uses local filesystem storage without MinIO or Nessie dependencies.
"""

import os
import sys
import time
import logging
import glob
import pymssql
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional, Tuple
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table
from utils import TypeMapper, SchemaEvolutionHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IcebergToSQLServerBackload:
    """Backload pipeline for restoring Iceberg data to SQL Server using filesystem storage."""
    
    def __init__(self, target_database: str = 'IcebergDemoTarget'):
        """Initialize backload with environment configurations."""
        self.mssql_config = {
            'server': os.getenv('MSSQL_HOST', 'localhost'),
            'port': int(os.getenv('MSSQL_PORT', 1433)),
            'user': os.getenv('MSSQL_USER', 'sa'),
            'password': os.getenv('MSSQL_PASSWORD', 'Strong@Password123'),
            'database': target_database
        }
        
        # Use local filesystem path
        self.warehouse_path = os.getenv('WAREHOUSE_PATH', '/data/warehouse')
        
        self.catalog = None
        self.mssql_conn = None
        
    def connect_mssql(self) -> None:
        """Establish connection to SQL Server."""
        try:
            self.mssql_conn = pymssql.connect(**self.mssql_config)
            logger.info(f"Successfully connected to SQL Server database: {self.mssql_config['database']}")
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server: {e}")
            raise
    
    def initialize_catalog(self) -> None:
        """Initialize Iceberg catalog using SQLite for metadata and filesystem for data."""
        try:
            # Use SQLite catalog for metadata storage
            catalog_db_path = os.path.join(self.warehouse_path, 'catalog.db')
            
            if not os.path.exists(catalog_db_path):
                logger.warning(f"Catalog database not found at {catalog_db_path}")
                logger.info("Please run ETL script first to create the catalog")
                raise FileNotFoundError(f"Catalog not found at {catalog_db_path}")
            
            self.catalog = SqlCatalog(
                "filesystem_catalog",
                **{
                    "uri": f"sqlite:///{catalog_db_path}",
                    "warehouse": f"file://{self.warehouse_path}",
                }
            )
            
            logger.info(f"Successfully initialized filesystem catalog at {self.warehouse_path}")
                
        except Exception as e:
            logger.error(f"Failed to initialize Iceberg catalog: {e}")
            raise
    
    def read_iceberg_table(self, namespace: str, table_name: str,
                          snapshot_id: Optional[int] = None) -> pa.Table:
        """Read data from Iceberg table into Arrow format."""
        full_table_name = f"{namespace}.{table_name}"
        
        try:
            # Load the table
            table = self.catalog.load_table(full_table_name)
            logger.info(f"Loaded Iceberg table: {full_table_name}")
            
            # Extract filesystem path from location
            location = table.location()
            if location.startswith('file://'):
                location = location[7:]  # Remove 'file://' prefix
            
            # Read Parquet files directly from the table location
            data_path = os.path.join(location, 'data')
            
            if not os.path.exists(data_path):
                logger.warning(f"No data found in {data_path}")
                return pa.Table.from_pandas(pd.DataFrame())
            
            # Find all Parquet files
            parquet_files = glob.glob(os.path.join(data_path, '*.parquet'))
            
            if not parquet_files:
                logger.warning(f"No Parquet files found in {data_path}")
                return pa.Table.from_pandas(pd.DataFrame())
            
            # Read all Parquet files and concatenate
            tables = []
            for file_path in parquet_files:
                table_part = pq.read_table(file_path)
                tables.append(table_part)
                logger.info(f"Read {len(table_part)} rows from {file_path}")
            
            # Concatenate all tables
            if tables:
                arrow_table = pa.concat_tables(tables)
                logger.info(f"Total rows read from {full_table_name}: {len(arrow_table)}")
                return arrow_table
            else:
                return pa.Table.from_pandas(pd.DataFrame())
                
        except Exception as e:
            logger.error(f"Failed to read Iceberg table {full_table_name}: {e}")
            raise
    
    def create_sql_table(self, schema: str, table_name: str, 
                        arrow_table: pa.Table) -> None:
        """Create SQL Server table from Arrow schema."""
        cursor = self.mssql_conn.cursor()
        
        # Drop existing table if it exists
        drop_query = f"""
        IF OBJECT_ID('[{schema}].[{table_name}]', 'U') IS NOT NULL
            DROP TABLE [{schema}].[{table_name}]
        """
        cursor.execute(drop_query)
        self.mssql_conn.commit()
        
        # Build CREATE TABLE statement
        columns = []
        for field in arrow_table.schema:
            sql_type = TypeMapper.arrow_to_sql_type(field.type)
            nullable = 'NULL' if field.nullable else 'NOT NULL'
            columns.append(f"[{field.name}] {sql_type} {nullable}")
        
        create_query = f"""
        CREATE TABLE [{schema}].[{table_name}] (
            {', '.join(columns)}
        )
        """
        
        logger.info(f"Creating table {schema}.{table_name}")
        cursor.execute(create_query)
        self.mssql_conn.commit()
        cursor.close()
    
    def bulk_insert_data(self, schema: str, table_name: str,
                        arrow_table: pa.Table) -> int:
        """Bulk insert Arrow table data into SQL Server."""
        # Convert Arrow table to pandas DataFrame
        df = arrow_table.to_pandas()
        
        if len(df) == 0:
            logger.warning(f"No data to insert into {schema}.{table_name}")
            return 0
        
        cursor = self.mssql_conn.cursor()
        
        # Build INSERT statement
        columns = ', '.join([f"[{col}]" for col in df.columns])
        placeholders = ', '.join(['%s' for _ in df.columns])
        insert_query = f"""
        INSERT INTO [{schema}].[{table_name}] ({columns})
        VALUES ({placeholders})
        """
        
        # Prepare data for insertion
        rows_inserted = 0
        batch_size = 1000
        
        for i in range(0, len(df), batch_size):
            batch_df = df.iloc[i:i+batch_size]
            
            for _, row in batch_df.iterrows():
                # Convert pandas values to appropriate Python types
                values = []
                for val in row.values:
                    if pd.isna(val):
                        values.append(None)
                    elif isinstance(val, (pd.Timestamp, datetime)):
                        values.append(val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
                    elif isinstance(val, bool):
                        values.append(1 if val else 0)
                    else:
                        values.append(val)
                
                cursor.execute(insert_query, tuple(values))
                rows_inserted += 1
            
            self.mssql_conn.commit()
            logger.info(f"Inserted batch: {rows_inserted} rows into {schema}.{table_name}")
        
        cursor.close()
        return rows_inserted
    
    def validate_data(self, schema: str, table_name: str,
                     expected_rows: int) -> bool:
        """Validate that data was correctly loaded."""
        cursor = self.mssql_conn.cursor()
        
        count_query = f"SELECT COUNT(*) FROM [{schema}].[{table_name}]"
        cursor.execute(count_query)
        actual_rows = cursor.fetchone()[0]
        cursor.close()
        
        if actual_rows == expected_rows:
            logger.info(f"✓ Validation passed: {actual_rows} rows in {schema}.{table_name}")
            return True
        else:
            logger.warning(f"✗ Validation failed: Expected {expected_rows}, found {actual_rows} rows")
            return False
    
    def backload_table(self, namespace: str, table_name: str,
                      target_schema: str = 'sales',
                      suffix: str = '_backload') -> Dict[str, Any]:
        """Backload a single Iceberg table to SQL Server."""
        start_time = time.time()
        target_table_name = f"{table_name}{suffix}"
        
        try:
            # Read from Iceberg
            arrow_table = self.read_iceberg_table(namespace, table_name)
            
            if len(arrow_table) == 0:
                logger.warning(f"No data found in {namespace}.{table_name}")
                return {
                    'status': 'skipped',
                    'source_table': f"{namespace}.{table_name}",
                    'target_table': f"{target_schema}.{target_table_name}",
                    'rows_loaded': 0,
                    'elapsed_time': time.time() - start_time
                }
            
            # Create SQL Server table
            self.create_sql_table(target_schema, target_table_name, arrow_table)
            
            # Bulk insert data
            rows_inserted = self.bulk_insert_data(target_schema, target_table_name, arrow_table)
            
            # Validate
            is_valid = self.validate_data(target_schema, target_table_name, rows_inserted)
            
            elapsed_time = time.time() - start_time
            
            result = {
                'status': 'success' if is_valid else 'warning',
                'source_table': f"{namespace}.{table_name}",
                'target_table': f"{target_schema}.{target_table_name}",
                'rows_loaded': rows_inserted,
                'elapsed_time': elapsed_time,
                'validated': is_valid
            }
            
            logger.info(f"Successfully backloaded {namespace}.{table_name}: "
                       f"{rows_inserted} rows in {elapsed_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to backload {namespace}.{table_name}: {e}")
            return {
                'status': 'failed',
                'source_table': f"{namespace}.{table_name}",
                'target_table': f"{target_schema}.{target_table_name}",
                'error': str(e),
                'elapsed_time': time.time() - start_time
            }
    
    def run_backload(self, tables: Optional[List[str]] = None) -> None:
        """Run the complete backload process."""
        logger.info("="*60)
        logger.info("Starting Iceberg to SQL Server backload (Filesystem Mode)")
        logger.info("="*60)
        
        # Connect to SQL Server
        self.connect_mssql()
        
        # Initialize Iceberg catalog
        self.initialize_catalog()
        
        # Define tables to backload
        if tables:
            tables_to_backload = [{'namespace': 'sales', 'table_name': t} for t in tables]
        else:
            # Backload all tables by default
            tables_to_backload = [
                {'namespace': 'sales', 'table_name': 'customers'},
                {'namespace': 'sales', 'table_name': 'products'},
                {'namespace': 'sales', 'table_name': 'transactions'},
                {'namespace': 'sales', 'table_name': 'order_details'},
                {'namespace': 'sales', 'table_name': 'inventory_snapshots'},
            ]
        
        results = []
        for table_config in tables_to_backload:
            result = self.backload_table(**table_config)
            results.append(result)
        
        # Print summary
        logger.info("\n" + "="*60)
        logger.info("Backload Summary:")
        logger.info("="*60)
        
        total_rows = 0
        total_time = 0
        
        for result in results:
            if result['status'] == 'success':
                logger.info(f"✓ {result['source_table']} → {result['target_table']}: "
                          f"{result['rows_loaded']} rows ({result['elapsed_time']:.2f}s)")
                total_rows += result['rows_loaded']
                total_time += result['elapsed_time']
            elif result['status'] == 'skipped':
                logger.info(f"⊘ {result['source_table']}: No data to backload")
            else:
                logger.error(f"✗ {result['source_table']}: {result.get('error', 'Unknown error')}")
        
        logger.info(f"\nTotal: {total_rows} rows backloaded in {total_time:.2f} seconds")
        logger.info(f"Data restored to database: {self.mssql_config['database']}")
        
        # Close connections
        if self.mssql_conn:
            self.mssql_conn.close()
            logger.info("Closed SQL Server connection")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Backload data from Iceberg to SQL Server')
    parser.add_argument('--tables', nargs='+', help='Specific tables to backload')
    parser.add_argument('--target-db', default='IcebergDemoTarget', 
                       help='Target SQL Server database')
    
    args = parser.parse_args()
    
    try:
        # Wait for services to be ready
        logger.info("Waiting for services to be ready...")
        time.sleep(5)
        
        backload = IcebergToSQLServerBackload(target_database=args.target_db)
        backload.run_backload(tables=args.tables)
        
    except Exception as e:
        logger.error(f"Backload process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()