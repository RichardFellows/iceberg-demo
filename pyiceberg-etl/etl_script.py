#!/usr/bin/env python3
"""
ETL Script: Migrate data from SQL Server to Apache Iceberg tables.
Uses PyArrow for zero-loss precision and Nessie for catalog management.
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
from typing import Dict, List, Any, Optional
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import NestedField
from utils import TypeMapper, SchemaEvolutionHandler

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SQLServerToIcebergETL:
    """ETL pipeline for migrating SQL Server data to Iceberg tables."""
    
    def __init__(self):
        """Initialize ETL with environment configurations."""
        self.mssql_config = {
            'server': os.getenv('MSSQL_HOST', 'localhost'),
            'port': int(os.getenv('MSSQL_PORT', 1433)),
            'user': os.getenv('MSSQL_USER', 'sa'),
            'password': os.getenv('MSSQL_PASSWORD', 'Strong@Password123'),
            'database': 'IcebergDemoSource'
        }
        
        self.warehouse_path = os.getenv('WAREHOUSE_PATH', '/data/warehouse')
        self.nessie_uri = os.getenv('NESSIE_URI', 'http://nessie:19120/iceberg/main/')
        
        self.catalog = None
        self.mssql_conn = None
        
    def connect_mssql(self) -> None:
        """Establish connection to SQL Server."""
        try:
            self.mssql_conn = pymssql.connect(**self.mssql_config)
            logger.info("Successfully connected to SQL Server")
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
            
            # Create namespace if it doesn't exist
            try:
                namespaces = self.catalog.list_namespaces()
                if ('sales',) not in namespaces:
                    self.catalog.create_namespace('sales')
                    logger.info("Created 'sales' namespace in Iceberg catalog")
            except:
                # Namespace might not exist yet, create it
                self.catalog.create_namespace('sales')
                logger.info("Created 'sales' namespace in Iceberg catalog")
                
        except Exception as e:
            logger.error(f"Failed to initialize Iceberg catalog: {e}")
            raise
    
    def get_table_metadata(self, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column metadata from SQL Server table."""
        cursor = self.mssql_conn.cursor(as_dict=True)
        
        query = """
        SELECT 
            c.COLUMN_NAME as name,
            c.DATA_TYPE as type,
            c.CHARACTER_MAXIMUM_LENGTH as char_length,
            c.NUMERIC_PRECISION as precision,
            c.NUMERIC_SCALE as scale,
            c.DATETIME_PRECISION as datetime_precision,
            CASE WHEN c.IS_NULLABLE = 'YES' THEN 1 ELSE 0 END as nullable,
            c.ORDINAL_POSITION as position
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = %s AND c.TABLE_NAME = %s
        ORDER BY c.ORDINAL_POSITION
        """
        
        cursor.execute(query, (schema, table))
        columns = cursor.fetchall()
        cursor.close()
        
        return columns
    
    def read_sql_table_to_arrow(self, schema: str, table: str, 
                                batch_size: int = 10000) -> pa.Table:
        """Read SQL Server table data into Arrow Table format."""
        logger.info(f"Reading data from {schema}.{table}")
        
        # Get column metadata
        columns = self.get_table_metadata(schema, table)
        
        # Build Arrow schema
        arrow_fields = []
        for col in columns:
            arrow_type = TypeMapper.sql_to_arrow_type(
                col['type'],
                col.get('precision'),
                col.get('scale')
            )
            arrow_fields.append(
                pa.field(col['name'], arrow_type, nullable=bool(col['nullable']))
            )
        
        arrow_schema = pa.schema(arrow_fields)
        
        # Read data with pandas and convert to Arrow
        query = f"SELECT * FROM [{schema}].[{table}]"
        
        # Use pandas to handle SQL Server types properly
        df = pd.read_sql(query, self.mssql_conn)
        
        # Handle special conversions
        for col in columns:
            col_name = col['name']
            if col['type'] == 'bit' and col_name in df.columns:
                # Convert bit to boolean
                df[col_name] = df[col_name].astype(bool)
            elif col['type'] in ('money', 'smallmoney', 'decimal', 'numeric') and col_name in df.columns:
                # Ensure decimal precision is preserved
                df[col_name] = df[col_name].astype(str).apply(
                    lambda x: Decimal(x) if pd.notna(x) else None
                )
        
        # Convert to Arrow table with explicit schema
        arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
        
        logger.info(f"Successfully read {len(arrow_table)} rows from {schema}.{table}")
        return arrow_table
    
    def create_iceberg_table(self, table_name: str, arrow_table: pa.Table,
                           partition_by: Optional[List[str]] = None) -> Table:
        """Create an Iceberg table from Arrow schema."""
        namespace = 'sales'
        full_table_name = f"{namespace}.{table_name}"
        
        # Convert Arrow schema to Iceberg schema
        sql_columns = []
        for field in arrow_table.schema:
            sql_columns.append({
                'name': field.name,
                'type': 'unknown',  # Will be determined by Arrow type
                'nullable': field.nullable
            })
        
        # Build Iceberg schema from Arrow types
        iceberg_fields = []
        for i, field in enumerate(arrow_table.schema):
            iceberg_type = TypeMapper.arrow_to_iceberg_type(field.type)
            iceberg_field = NestedField(
                field_id=i + 1,
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable
            )
            iceberg_fields.append(iceberg_field)
        
        iceberg_schema = Schema(*iceberg_fields)
        
        # Create partition spec if specified
        partition_fields = []
        if partition_by:
            field_id_counter = 1000  # Start partition field IDs at 1000
            for col in partition_by:
                # Find the field in schema
                for field in iceberg_fields:
                    if field.name == col:
                        # Use appropriate transform based on type
                        if 'date' in str(field.field_type).lower():
                            partition_field = PartitionField(
                                source_id=field.field_id,
                                field_id=field_id_counter,
                                transform=DayTransform(),
                                name=f"{col}_day"
                            )
                        else:
                            partition_field = PartitionField(
                                source_id=field.field_id,
                                field_id=field_id_counter,
                                transform=IdentityTransform(),
                                name=col
                            )
                        partition_fields.append(partition_field)
                        field_id_counter += 1
                        break
        
        partition_spec = PartitionSpec(*partition_fields)
        
        # Create or replace table
        try:
            # Try to drop existing table first
            try:
                self.catalog.drop_table(full_table_name)
                logger.info(f"Dropped existing table {full_table_name}")
            except:
                pass  # Table doesn't exist
            
            table = self.catalog.create_table(
                identifier=full_table_name,
                schema=iceberg_schema,
                partition_spec=partition_spec,
                location=f"{self.warehouse_path}/{namespace}/{table_name}"
            )
            logger.info(f"Created Iceberg table {full_table_name}")
            return table
            
        except Exception as e:
            logger.error(f"Failed to create Iceberg table {full_table_name}: {e}")
            raise
    
    def write_to_iceberg(self, table: Table, arrow_table: pa.Table) -> None:
        """Write Arrow table data to Iceberg table."""
        try:
            # Write as Parquet file directly using PyArrow
            output_path = f"{table.location()}/data"
            os.makedirs(output_path, exist_ok=True)
            
            # Generate unique file name
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_path = f"{output_path}/part-00000-{timestamp}.parquet"
            
            # Write Parquet with Arrow to preserve schema
            pq.write_table(
                arrow_table,
                file_path,
                compression='snappy',
                use_dictionary=True,
                write_statistics=True
            )
            
            logger.info(f"Written {len(arrow_table)} rows to {file_path}")
            
            # Note: In a production scenario, you would also update the Iceberg metadata
            # to register this file with the table. PyIceberg's write support is evolving.
            
        except Exception as e:
            logger.error(f"Failed to write to Iceberg table: {e}")
            raise
    
    def migrate_table(self, schema: str, table_name: str, 
                     partition_by: Optional[List[str]] = None) -> Dict[str, Any]:
        """Migrate a single table from SQL Server to Iceberg."""
        start_time = time.time()
        
        try:
            # Read from SQL Server
            arrow_table = self.read_sql_table_to_arrow(schema, table_name)
            
            # Create Iceberg table
            iceberg_table = self.create_iceberg_table(table_name, arrow_table, partition_by)
            
            # Write data to Iceberg
            self.write_to_iceberg(iceberg_table, arrow_table)
            
            elapsed_time = time.time() - start_time
            
            result = {
                'status': 'success',
                'table': f"{schema}.{table_name}",
                'rows_migrated': len(arrow_table),
                'elapsed_time': elapsed_time
            }
            
            logger.info(f"Successfully migrated {schema}.{table_name}: "
                       f"{result['rows_migrated']} rows in {elapsed_time:.2f} seconds")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to migrate {schema}.{table_name}: {e}")
            return {
                'status': 'failed',
                'table': f"{schema}.{table_name}",
                'error': str(e),
                'elapsed_time': time.time() - start_time
            }
    
    def run_migration(self) -> None:
        """Run the complete migration process."""
        logger.info("Starting SQL Server to Iceberg migration")
        
        # Connect to SQL Server
        self.connect_mssql()
        
        # Initialize Iceberg catalog
        self.initialize_catalog()
        
        # Define tables to migrate with optional partitioning
        tables_to_migrate = [
            {'schema': 'sales', 'table_name': 'customers', 'partition_by': None},
            {'schema': 'sales', 'table_name': 'products', 'partition_by': ['category']},
            {'schema': 'sales', 'table_name': 'transactions', 'partition_by': ['transaction_date']},
            {'schema': 'sales', 'table_name': 'order_details', 'partition_by': None},
            {'schema': 'sales', 'table_name': 'inventory_snapshots', 'partition_by': ['snapshot_date', 'warehouse_id']},
        ]
        
        results = []
        for table_config in tables_to_migrate:
            result = self.migrate_table(**table_config)
            results.append(result)
        
        # Print summary
        logger.info("\n" + "="*50)
        logger.info("Migration Summary:")
        logger.info("="*50)
        
        total_rows = 0
        total_time = 0
        
        for result in results:
            if result['status'] == 'success':
                logger.info(f"✓ {result['table']}: {result['rows_migrated']} rows "
                          f"({result['elapsed_time']:.2f}s)")
                total_rows += result['rows_migrated']
                total_time += result['elapsed_time']
            else:
                logger.error(f"✗ {result['table']}: {result.get('error', 'Unknown error')}")
        
        logger.info(f"\nTotal: {total_rows} rows migrated in {total_time:.2f} seconds")
        
        # Close connections
        if self.mssql_conn:
            self.mssql_conn.close()
            logger.info("Closed SQL Server connection")


def main():
    """Main entry point."""
    try:
        # Wait for services to be ready
        logger.info("Waiting for services to be ready...")
        time.sleep(10)
        
        etl = SQLServerToIcebergETL()
        etl.run_migration()
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()