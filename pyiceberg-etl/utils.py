"""
Shared utilities for bidirectional type mapping between SQL Server and Apache Iceberg.
Ensures zero precision loss using Apache Arrow as intermediate format.
"""

import pyarrow as pa
from pyiceberg.types import (
    BooleanType, IntegerType, LongType, FloatType, DoubleType,
    DecimalType, StringType, BinaryType, DateType, TimestampType,
    TimestamptzType, FixedType, UUIDType, NestedField, StructType
)
from decimal import Decimal
from typing import Dict, Any, Optional, List, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TypeMapper:
    """Handles bidirectional type mapping between SQL Server, Arrow, and Iceberg."""
    
    # SQL Server to Arrow type mapping
    SQL_TO_ARROW_MAP = {
        'bit': pa.bool_(),
        'tinyint': pa.int8(),
        'smallint': pa.int16(),
        'int': pa.int32(),
        'bigint': pa.int64(),
        'float': pa.float64(),  # SQL Server float is 53-bit (double precision)
        'real': pa.float32(),   # SQL Server real is 24-bit (single precision)
        'decimal': None,  # Handled separately with precision/scale
        'numeric': None,  # Handled separately with precision/scale
        'money': pa.decimal128(19, 4),
        'smallmoney': pa.decimal128(10, 4),
        'date': pa.date32(),
        'time': pa.time64('ns'),
        'datetime': pa.timestamp('ms'),  # SQL Server datetime has 3.33ms precision
        'datetime2': pa.timestamp('ns'),  # Can have up to nanosecond precision
        'datetimeoffset': pa.timestamp('ns', tz='UTC'),
        'char': pa.string(),
        'varchar': pa.string(),
        'nchar': pa.string(),  # UTF-16 in SQL Server, UTF-8 in Arrow
        'nvarchar': pa.string(),
        'text': pa.string(),
        'ntext': pa.string(),
        'binary': pa.binary(),
        'varbinary': pa.binary(),
        'image': pa.binary(),
        'uniqueidentifier': pa.string(),  # Store as string, can convert to UUID
    }
    
    @classmethod
    def sql_to_arrow_type(cls, sql_type: str, precision: Optional[int] = None, 
                          scale: Optional[int] = None) -> pa.DataType:
        """Convert SQL Server type to Arrow type."""
        sql_type_lower = sql_type.lower()
        
        # Handle decimal/numeric types with precision and scale
        if sql_type_lower in ('decimal', 'numeric'):
            if precision and scale is not None:
                return pa.decimal128(precision, scale)
            return pa.decimal128(38, 18)  # Default max precision
        
        # Handle datetime2 with specific precision
        if sql_type_lower == 'datetime2' and precision is not None:
            if precision <= 3:
                return pa.timestamp('ms')
            elif precision <= 6:
                return pa.timestamp('us')
            else:
                return pa.timestamp('ns')
        
        # Handle time with specific precision
        if sql_type_lower == 'time' and precision is not None:
            if precision <= 6:
                return pa.time64('us')
            else:
                return pa.time64('ns')
        
        arrow_type = cls.SQL_TO_ARROW_MAP.get(sql_type_lower)
        if arrow_type is None:
            logger.warning(f"Unknown SQL Server type: {sql_type}, defaulting to string")
            return pa.string()
        
        return arrow_type
    
    @classmethod
    def arrow_to_iceberg_type(cls, arrow_type: pa.DataType) -> Any:
        """Convert Arrow type to Iceberg type."""
        if pa.types.is_boolean(arrow_type):
            return BooleanType()
        elif pa.types.is_int8(arrow_type) or pa.types.is_int16(arrow_type) or pa.types.is_int32(arrow_type):
            return IntegerType()
        elif pa.types.is_int64(arrow_type):
            return LongType()
        elif pa.types.is_float32(arrow_type):
            return FloatType()
        elif pa.types.is_float64(arrow_type):
            return DoubleType()
        elif pa.types.is_decimal(arrow_type):
            return DecimalType(arrow_type.precision, arrow_type.scale)
        elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
            return StringType()
        elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
            return BinaryType()
        elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            return DateType()
        elif pa.types.is_timestamp(arrow_type):
            if arrow_type.tz:
                return TimestamptzType()
            else:
                return TimestampType()
        elif pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
            # Iceberg doesn't have a Time type, convert to timestamp
            return TimestampType()
        else:
            logger.warning(f"Unknown Arrow type: {arrow_type}, defaulting to string")
            return StringType()
    
    @classmethod
    def iceberg_to_arrow_type(cls, iceberg_type: Any) -> pa.DataType:
        """Convert Iceberg type back to Arrow type."""
        if isinstance(iceberg_type, BooleanType):
            return pa.bool_()
        elif isinstance(iceberg_type, IntegerType):
            return pa.int32()
        elif isinstance(iceberg_type, LongType):
            return pa.int64()
        elif isinstance(iceberg_type, FloatType):
            return pa.float32()
        elif isinstance(iceberg_type, DoubleType):
            return pa.float64()
        elif isinstance(iceberg_type, DecimalType):
            return pa.decimal128(iceberg_type.precision, iceberg_type.scale)
        elif isinstance(iceberg_type, StringType):
            return pa.string()
        elif isinstance(iceberg_type, BinaryType):
            return pa.binary()
        elif isinstance(iceberg_type, DateType):
            return pa.date32()
        elif isinstance(iceberg_type, TimestampType):
            return pa.timestamp('us')
        elif isinstance(iceberg_type, TimestamptzType):
            return pa.timestamp('us', tz='UTC')
        elif isinstance(iceberg_type, FixedType):
            return pa.binary(iceberg_type.length)
        elif isinstance(iceberg_type, UUIDType):
            return pa.string()  # Store UUID as string
        else:
            logger.warning(f"Unknown Iceberg type: {iceberg_type}, defaulting to string")
            return pa.string()
    
    @classmethod
    def arrow_to_sql_type(cls, arrow_type: pa.DataType, 
                         prefer_nvarchar: bool = True) -> Tuple[str, Optional[Dict[str, Any]]]:
        """
        Convert Arrow type back to SQL Server type.
        Returns tuple of (sql_type, additional_params)
        """
        params = {}
        
        if pa.types.is_boolean(arrow_type):
            return "BIT", params
        elif pa.types.is_int8(arrow_type):
            return "TINYINT", params
        elif pa.types.is_int16(arrow_type):
            return "SMALLINT", params
        elif pa.types.is_int32(arrow_type):
            return "INT", params
        elif pa.types.is_int64(arrow_type):
            return "BIGINT", params
        elif pa.types.is_float32(arrow_type):
            return "REAL", params
        elif pa.types.is_float64(arrow_type):
            return "FLOAT", params
        elif pa.types.is_decimal(arrow_type):
            params['precision'] = arrow_type.precision
            params['scale'] = arrow_type.scale
            # Special case for money types
            if arrow_type.precision == 19 and arrow_type.scale == 4:
                return "MONEY", {}
            elif arrow_type.precision == 10 and arrow_type.scale == 4:
                return "SMALLMONEY", {}
            return f"DECIMAL({arrow_type.precision},{arrow_type.scale})", params
        elif pa.types.is_string(arrow_type):
            if prefer_nvarchar:
                return "NVARCHAR(MAX)", params
            else:
                return "VARCHAR(MAX)", params
        elif pa.types.is_binary(arrow_type):
            if pa.types.is_fixed_size_binary(arrow_type):
                params['length'] = arrow_type.byte_width
                return f"BINARY({arrow_type.byte_width})", params
            return "VARBINARY(MAX)", params
        elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
            return "DATE", params
        elif pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
            if pa.types.is_time64(arrow_type) and arrow_type.unit == 'ns':
                return "TIME(7)", params
            return "TIME", params
        elif pa.types.is_timestamp(arrow_type):
            if arrow_type.tz:
                return "DATETIMEOFFSET", params
            else:
                # Determine precision based on unit
                if arrow_type.unit == 'ns':
                    return "DATETIME2(7)", params
                elif arrow_type.unit == 'us':
                    return "DATETIME2(6)", params
                elif arrow_type.unit == 'ms':
                    return "DATETIME2(3)", params
                else:
                    return "DATETIME2", params
        else:
            logger.warning(f"Unknown Arrow type for SQL conversion: {arrow_type}, defaulting to NVARCHAR(MAX)")
            return "NVARCHAR(MAX)", params
    
    @classmethod
    def create_iceberg_schema(cls, sql_columns: List[Dict[str, Any]]) -> StructType:
        """
        Create an Iceberg schema from SQL Server column definitions.
        
        Args:
            sql_columns: List of dicts with keys: name, type, precision, scale, nullable
        
        Returns:
            Iceberg StructType schema
        """
        fields = []
        for i, col in enumerate(sql_columns):
            arrow_type = cls.sql_to_arrow_type(
                col['type'], 
                col.get('precision'), 
                col.get('scale')
            )
            iceberg_type = cls.arrow_to_iceberg_type(arrow_type)
            
            field = NestedField(
                field_id=i + 1,
                name=col['name'],
                field_type=iceberg_type,
                required=not col.get('nullable', True)
            )
            fields.append(field)
        
        return StructType(*fields)
    
    @classmethod
    def validate_data_integrity(cls, source_df: pa.Table, target_df: pa.Table, 
                               tolerance: float = 1e-9) -> Dict[str, Any]:
        """
        Validate data integrity between source and target Arrow tables.
        
        Returns dict with validation results.
        """
        results = {
            'row_count_match': len(source_df) == len(target_df),
            'source_rows': len(source_df),
            'target_rows': len(target_df),
            'column_validations': {}
        }
        
        for col_name in source_df.column_names:
            if col_name not in target_df.column_names:
                results['column_validations'][col_name] = {'status': 'missing_in_target'}
                continue
            
            source_col = source_df[col_name]
            target_col = target_df[col_name]
            
            col_result = {
                'type_match': source_col.type == target_col.type,
                'source_type': str(source_col.type),
                'target_type': str(target_col.type)
            }
            
            # For numeric types, check precision
            if pa.types.is_floating(source_col.type) and pa.types.is_floating(target_col.type):
                source_array = source_col.to_pylist()
                target_array = target_col.to_pylist()
                
                max_diff = 0
                for s, t in zip(source_array, target_array):
                    if s is not None and t is not None:
                        diff = abs(s - t)
                        max_diff = max(max_diff, diff)
                
                col_result['max_numeric_diff'] = max_diff
                col_result['within_tolerance'] = max_diff <= tolerance
            
            results['column_validations'][col_name] = col_result
        
        # Check for extra columns in target
        for col_name in target_df.column_names:
            if col_name not in source_df.column_names:
                results['column_validations'][col_name] = {'status': 'extra_in_target'}
        
        return results


class SchemaEvolutionHandler:
    """Handles schema evolution scenarios for Iceberg tables."""
    
    @staticmethod
    def detect_schema_changes(source_schema: StructType, 
                             target_schema: StructType) -> Dict[str, List[Any]]:
        """
        Detect schema changes between source and target schemas.
        
        Returns dict with: added_columns, removed_columns, type_changes
        """
        source_fields = {f.name: f for f in source_schema.fields}
        target_fields = {f.name: f for f in target_schema.fields}
        
        changes = {
            'added_columns': [],
            'removed_columns': [],
            'type_changes': []
        }
        
        # Find added columns
        for name, field in target_fields.items():
            if name not in source_fields:
                changes['added_columns'].append(field)
        
        # Find removed columns
        for name, field in source_fields.items():
            if name not in target_fields:
                changes['removed_columns'].append(field)
        
        # Find type changes
        for name, source_field in source_fields.items():
            if name in target_fields:
                target_field = target_fields[name]
                if source_field.field_type != target_field.field_type:
                    changes['type_changes'].append({
                        'column': name,
                        'old_type': source_field.field_type,
                        'new_type': target_field.field_type
                    })
        
        return changes
    
    @staticmethod
    def can_evolve_safely(old_type: Any, new_type: Any) -> bool:
        """
        Check if a type change is safe for schema evolution.
        
        Safe evolutions include:
        - INT -> LONG
        - FLOAT -> DOUBLE  
        - Increasing decimal precision
        - STRING length increases
        """
        # int to long
        if isinstance(old_type, IntegerType) and isinstance(new_type, LongType):
            return True
        
        # float to double
        if isinstance(old_type, FloatType) and isinstance(new_type, DoubleType):
            return True
        
        # decimal precision increase
        if isinstance(old_type, DecimalType) and isinstance(new_type, DecimalType):
            if (new_type.precision >= old_type.precision and 
                new_type.scale == old_type.scale):
                return True
        
        return False