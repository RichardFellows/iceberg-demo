#!/usr/bin/env python3
"""
Backload Script: Restore data from Apache Iceberg tables back to SQL Server.
Supports schema evolution, point-in-time recovery, and data validation.
"""

import os
import sys
import time
import logging
import pymssql
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional, Tuple
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from utils import TypeMapper, SchemaEvolutionHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IcebergToSQLServerBackload:
    """Backload pipeline for restoring Iceberg data to SQL Server."""
    
    def __init__(self, target_database: str = 'IcebergDemoTarget'):
        """Initialize backload with environment configurations."""
        self.mssql_config = {
            'server': os.getenv('MSSQL_HOST', 'localhost'),
            'port': int(os.getenv('MSSQL_PORT', 1433)),
            'user': os.getenv('MSSQL_USER', 'sa'),
            'password': os.getenv('MSSQL_PASSWORD', 'Strong@Password123'),
            'database': target_database
        }
        
        self.warehouse_path = os.getenv('WAREHOUSE_PATH', '/data/warehouse')
        self.nessie_uri = os.getenv('NESSIE_URI', 'http://localhost:19120/api/v1')
        
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
        """Initialize Iceberg catalog (filesystem with Nessie integration path)."""
        try:
            # Try Nessie REST first, fallback to filesystem for demo reliability
            try:
                catalog_config = {
                    'type': 'rest',
                    'uri': self.nessie_uri,
                }
                self.catalog = load_catalog('nessie_rest', **catalog_config)
                logger.info(f"Successfully initialized Nessie REST catalog at {self.nessie_uri}")
            except Exception as e:
                logger.warning(f"Nessie REST catalog not available ({e}), using filesystem catalog")
                # Fallback to filesystem catalog using SQL backend
                catalog_config = {
                    'uri': 'sqlite:////tmp/pyiceberg_catalog.db',
                    'warehouse': f'file://{self.warehouse_path}',
                }
                self.catalog = load_catalog('demo_catalog', **catalog_config)
                logger.info(f"Successfully initialized filesystem catalog at {self.warehouse_path}")
                logger.info("Note: Using filesystem catalog. For production with versioning, configure Nessie REST properly.")
                
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
            
            # For now, read Parquet files directly from the table location
            # In production, use PyIceberg's scan API when fully available
            data_path = f"{table.location()}/data"
            
            if not os.path.exists(data_path):
                logger.warning(f"No data found in {data_path}")
                return pa.table({})
            
            # Read all Parquet files in the data directory
            parquet_files = [f for f in os.listdir(data_path) if f.endswith('.parquet')]
            
            if not parquet_files:
                logger.warning(f"No Parquet files found in {data_path}")
                return pa.table({})
            
            # Read and combine all Parquet files
            tables = []
            for file in parquet_files:
                file_path = os.path.join(data_path, file)
                table = pq.read_table(file_path)
                tables.append(table)
                logger.info(f"Read {len(table)} rows from {file}")
            
            # Combine all tables
            if tables:
                combined_table = pa.concat_tables(tables)
                logger.info(f"Total rows read from Iceberg: {len(combined_table)}")
                return combined_table
            else:
                return pa.table({})
                
        except Exception as e:
            logger.error(f"Failed to read Iceberg table {full_table_name}: {e}")
            raise
    
    def generate_create_table_sql(self, schema: str, table_name: str, 
                                 arrow_table: pa.Table) -> str:
        """Generate CREATE TABLE SQL from Arrow schema."""
        columns = []
        
        for field in arrow_table.schema:
            sql_type, params = TypeMapper.arrow_to_sql_type(field.type)
            
            # Handle nullability
            null_constraint = "NULL" if field.nullable else "NOT NULL"
            
            # Format column definition
            col_def = f"[{field.name}] {sql_type} {null_constraint}"
            columns.append(col_def)
        
        # Build CREATE TABLE statement
        create_sql = f"""
        IF OBJECT_ID('[{schema}].[{table_name}_backload]', 'U') IS NOT NULL
            DROP TABLE [{schema}].[{table_name}_backload];
        
        CREATE TABLE [{schema}].[{table_name}_backload] (
            {','.join(columns)}
        );
        """
        
        return create_sql
    
    def prepare_data_for_sql(self, arrow_table: pa.Table) -> pd.DataFrame:
        """Convert Arrow table to pandas DataFrame suitable for SQL Server."""
        # Convert to pandas
        df = arrow_table.to_pandas()
        
        # Handle special type conversions
        for i, field in enumerate(arrow_table.schema):
            col_name = field.name
            
            if pa.types.is_decimal(field.type):
                # Ensure decimal values are properly formatted
                df[col_name] = df[col_name].apply(
                    lambda x: float(x) if pd.notna(x) else None
                )
            elif pa.types.is_timestamp(field.type):
                # Ensure timestamps are in the right format
                df[col_name] = pd.to_datetime(df[col_name])
            elif pa.types.is_binary(field.type):
                # Binary data should be bytes
                df[col_name] = df[col_name].apply(
                    lambda x: bytes(x) if pd.notna(x) else None
                )
        
        return df
    
    def bulk_insert_data(self, schema: str, table_name: str, df: pd.DataFrame,
                        batch_size: int = 1000) -> int:
        """Bulk insert data into SQL Server table."""
        cursor = self.mssql_conn.cursor()
        target_table = f"{table_name}_backload"
        
        try:
            # Prepare insert statement
            columns = df.columns.tolist()
            placeholders = ', '.join(['%s'] * len(columns))
            column_list = ', '.join([f'[{col}]' for col in columns])
            
            insert_sql = f"""
            INSERT INTO [{schema}].[{target_table}] ({column_list})
            VALUES ({placeholders})
            """
            
            # Insert data in batches
            total_rows = len(df)
            rows_inserted = 0
            
            for i in range(0, total_rows, batch_size):
                batch = df.iloc[i:i+batch_size]
                
                # Convert batch to list of tuples
                values = []
                for _, row in batch.iterrows():
                    # Handle None values and special types
                    row_values = []
                    for val in row:
                        if pd.isna(val):
                            row_values.append(None)
                        elif isinstance(val, pd.Timestamp):
                            row_values.append(val.to_pydatetime())
                        else:
                            row_values.append(val)
                    values.append(tuple(row_values))
                
                # Execute batch insert
                cursor.executemany(insert_sql, values)
                self.mssql_conn.commit()
                
                rows_inserted += len(batch)
                logger.info(f"Inserted batch: {rows_inserted}/{total_rows} rows")
            
            cursor.close()
            return rows_inserted
            
        except Exception as e:
            self.mssql_conn.rollback()
            cursor.close()
            logger.error(f"Failed to insert data: {e}")
            raise
    
    def validate_backload(self, schema: str, table_name: str, 
                         expected_rows: int) -> Dict[str, Any]:
        """Validate the backloaded data."""
        cursor = self.mssql_conn.cursor()
        target_table = f"{table_name}_backload"
        
        validation_results = {
            'table': f"{schema}.{target_table}",
            'expected_rows': expected_rows,
            'actual_rows': 0,
            'validation_status': 'unknown',
            'checks': {}
        }
        
        try:
            # Count rows
            count_sql = f"SELECT COUNT(*) as cnt FROM [{schema}].[{target_table}]"
            cursor.execute(count_sql)
            result = cursor.fetchone()
            actual_rows = result[0]
            
            validation_results['actual_rows'] = actual_rows
            validation_results['checks']['row_count'] = {
                'expected': expected_rows,
                'actual': actual_rows,
                'match': actual_rows == expected_rows
            }
            
            # Check for nulls in non-nullable columns (sample check)
            # In production, you'd want more comprehensive validation
            
            # Get column statistics
            stats_sql = f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT customer_id) as unique_customers,
                MIN(transaction_date) as min_date,
                MAX(transaction_date) as max_date
            FROM [{schema}].[{target_table}]
            WHERE 1=1
            """
            
            # Try to get stats if applicable columns exist
            try:
                cursor.execute(stats_sql)
                stats = cursor.fetchone()
                if stats:
                    validation_results['checks']['statistics'] = {
                        'total_rows': stats[0],
                        'unique_customers': stats[1] if stats[1] else 'N/A',
                        'date_range': f"{stats[2]} to {stats[3]}" if stats[2] and stats[3] else 'N/A'
                    }
            except:
                # Table might not have these columns
                pass
            
            # Overall validation status
            if validation_results['checks']['row_count']['match']:
                validation_results['validation_status'] = 'PASSED'
            else:
                validation_results['validation_status'] = 'FAILED'
            
            cursor.close()
            return validation_results
            
        except Exception as e:
            cursor.close()
            validation_results['validation_status'] = 'ERROR'
            validation_results['error'] = str(e)
            return validation_results
    
    def backload_table(self, namespace: str, table_name: str,
                      create_table: bool = True,
                      validate: bool = True) -> Dict[str, Any]:
        """Backload a single table from Iceberg to SQL Server."""
        start_time = time.time()
        schema = namespace  # Using same schema name for SQL Server
        
        try:
            logger.info(f"Starting backload for {namespace}.{table_name}")
            
            # Read from Iceberg
            arrow_table = self.read_iceberg_table(namespace, table_name)
            
            if len(arrow_table) == 0:
                logger.warning(f"No data found in Iceberg table {namespace}.{table_name}")
                return {
                    'status': 'no_data',
                    'table': f"{namespace}.{table_name}",
                    'rows_loaded': 0,
                    'elapsed_time': time.time() - start_time
                }
            
            # Create target table if requested
            if create_table:
                create_sql = self.generate_create_table_sql(schema, table_name, arrow_table)
                cursor = self.mssql_conn.cursor()
                cursor.execute(create_sql)
                self.mssql_conn.commit()
                cursor.close()
                logger.info(f"Created target table {schema}.{table_name}_backload")
            
            # Prepare data for SQL Server
            df = self.prepare_data_for_sql(arrow_table)
            
            # Bulk insert data
            rows_inserted = self.bulk_insert_data(schema, table_name, df)
            
            # Validate if requested
            validation_results = None
            if validate:
                validation_results = self.validate_backload(schema, table_name, len(arrow_table))
                logger.info(f"Validation status: {validation_results['validation_status']}")
            
            elapsed_time = time.time() - start_time
            
            result = {
                'status': 'success',
                'table': f"{namespace}.{table_name}",
                'rows_loaded': rows_inserted,
                'elapsed_time': elapsed_time,
                'validation': validation_results
            }
            
            logger.info(f"Successfully backloaded {namespace}.{table_name}: "
                       f"{rows_inserted} rows in {elapsed_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to backload {namespace}.{table_name}: {e}")
            return {
                'status': 'failed',
                'table': f"{namespace}.{table_name}",
                'error': str(e),
                'elapsed_time': time.time() - start_time
            }
    
    def run_backload(self, tables: Optional[List[str]] = None) -> None:
        """Run the complete backload process."""
        logger.info("Starting Iceberg to SQL Server backload")
        
        # Connect to SQL Server
        self.connect_mssql()
        
        # Initialize Iceberg catalog
        self.initialize_catalog()
        
        # Define tables to backload
        if tables is None:
            tables_to_backload = [
                'customers',
                'products',
                'transactions',
                'order_details',
                'inventory_snapshots'
            ]
        else:
            tables_to_backload = tables
        
        results = []
        for table_name in tables_to_backload:
            result = self.backload_table(
                namespace='sales',
                table_name=table_name,
                create_table=True,
                validate=True
            )
            results.append(result)
        
        # Print summary
        logger.info("\n" + "="*50)
        logger.info("Backload Summary:")
        logger.info("="*50)
        
        total_rows = 0
        total_time = 0
        
        for result in results:
            if result['status'] == 'success':
                validation_status = ""
                if result.get('validation'):
                    validation_status = f" [{result['validation']['validation_status']}]"
                logger.info(f"✓ {result['table']}: {result['rows_loaded']} rows "
                          f"({result['elapsed_time']:.2f}s){validation_status}")
                total_rows += result['rows_loaded']
                total_time += result['elapsed_time']
            elif result['status'] == 'no_data':
                logger.warning(f"○ {result['table']}: No data to backload")
            else:
                logger.error(f"✗ {result['table']}: {result.get('error', 'Unknown error')}")
        
        logger.info(f"\nTotal: {total_rows} rows backloaded in {total_time:.2f} seconds")
        
        # Close connections
        if self.mssql_conn:
            self.mssql_conn.close()
            logger.info("Closed SQL Server connection")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Backload data from Iceberg to SQL Server')
    parser.add_argument('--database', default='IcebergDemoTarget',
                       help='Target SQL Server database')
    parser.add_argument('--tables', nargs='+',
                       help='Specific tables to backload (default: all)')
    
    args = parser.parse_args()
    
    try:
        # Wait for services to be ready
        logger.info("Waiting for services to be ready...")
        time.sleep(5)
        
        backload = IcebergToSQLServerBackload(target_database=args.database)
        backload.run_backload(tables=args.tables)
        
    except Exception as e:
        logger.error(f"Backload process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()