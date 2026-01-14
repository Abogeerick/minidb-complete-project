"""
Data Types Module - Defines supported column data types for MiniDB

Supports: INTEGER, FLOAT, VARCHAR, BOOLEAN, TEXT, DATE, TIMESTAMP
"""

from enum import Enum, auto
from dataclasses import dataclass
from typing import Any, Optional
from datetime import datetime, date
import re


class DataType(Enum):
    """Supported data types in MiniDB"""
    INTEGER = auto()
    FLOAT = auto()
    VARCHAR = auto()
    TEXT = auto()
    BOOLEAN = auto()
    DATE = auto()
    TIMESTAMP = auto()


@dataclass
class ColumnType:
    """Column type with optional size constraint (for VARCHAR)"""
    dtype: DataType
    size: Optional[int] = None  # For VARCHAR(n)
    
    def __str__(self) -> str:
        if self.dtype == DataType.VARCHAR and self.size:
            return f"VARCHAR({self.size})"
        return self.dtype.name


class TypeValidator:
    """Validates and converts values to appropriate Python types"""
    
    @staticmethod
    def parse_type(type_str: str) -> ColumnType:
        """Parse SQL type string into ColumnType"""
        type_str = type_str.upper().strip()
        
        # Check for VARCHAR with size
        varchar_match = re.match(r'VARCHAR\s*\(\s*(\d+)\s*\)', type_str)
        if varchar_match:
            return ColumnType(DataType.VARCHAR, int(varchar_match.group(1)))
        
        # Map type names
        type_map = {
            'INTEGER': DataType.INTEGER,
            'INT': DataType.INTEGER,
            'FLOAT': DataType.FLOAT,
            'REAL': DataType.FLOAT,
            'DOUBLE': DataType.FLOAT,
            'VARCHAR': DataType.VARCHAR,
            'TEXT': DataType.TEXT,
            'STRING': DataType.TEXT,
            'BOOLEAN': DataType.BOOLEAN,
            'BOOL': DataType.BOOLEAN,
            'DATE': DataType.DATE,
            'TIMESTAMP': DataType.TIMESTAMP,
            'DATETIME': DataType.TIMESTAMP,
        }
        
        if type_str in type_map:
            return ColumnType(type_map[type_str])
        
        raise ValueError(f"Unknown data type: {type_str}")
    
    @staticmethod
    def validate_and_convert(value: Any, col_type: ColumnType) -> Any:
        """Validate and convert a value to the expected type"""
        if value is None:
            return None
            
        dtype = col_type.dtype
        
        try:
            if dtype == DataType.INTEGER:
                if isinstance(value, bool):
                    return 1 if value else 0
                return int(value)
                
            elif dtype == DataType.FLOAT:
                return float(value)
                
            elif dtype == DataType.VARCHAR:
                str_val = str(value)
                if col_type.size and len(str_val) > col_type.size:
                    raise ValueError(f"String exceeds VARCHAR({col_type.size}) limit")
                return str_val
                
            elif dtype == DataType.TEXT:
                return str(value)
                
            elif dtype == DataType.BOOLEAN:
                if isinstance(value, bool):
                    return value
                if isinstance(value, str):
                    if value.upper() in ('TRUE', '1', 'YES'):
                        return True
                    elif value.upper() in ('FALSE', '0', 'NO'):
                        return False
                return bool(value)
                
            elif dtype == DataType.DATE:
                if isinstance(value, date):
                    return value
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, str):
                    return datetime.strptime(value, '%Y-%m-%d').date()
                raise ValueError(f"Cannot convert {type(value)} to DATE")
                
            elif dtype == DataType.TIMESTAMP:
                if isinstance(value, datetime):
                    return value
                if isinstance(value, str):
                    # Try multiple formats
                    for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d']:
                        try:
                            return datetime.strptime(value, fmt)
                        except ValueError:
                            continue
                raise ValueError(f"Cannot convert {type(value)} to TIMESTAMP")
                
        except (ValueError, TypeError) as e:
            raise ValueError(f"Cannot convert '{value}' to {dtype.name}: {e}")
        
        return value
    
    @staticmethod
    def serialize(value: Any, col_type: ColumnType) -> str:
        """Serialize a value to string for storage"""
        if value is None:
            return "NULL"
        
        dtype = col_type.dtype
        
        if dtype in (DataType.INTEGER, DataType.FLOAT, DataType.BOOLEAN):
            return str(value)
        elif dtype == DataType.VARCHAR or dtype == DataType.TEXT:
            return value
        elif dtype == DataType.DATE:
            return value.strftime('%Y-%m-%d')
        elif dtype == DataType.TIMESTAMP:
            return value.strftime('%Y-%m-%d %H:%M:%S')
        
        return str(value)
    
    @staticmethod
    def deserialize(value: str, col_type: ColumnType) -> Any:
        """Deserialize a string value to appropriate type"""
        if value == "NULL":
            return None
        
        dtype = col_type.dtype
        
        if dtype == DataType.INTEGER:
            return int(value)
        elif dtype == DataType.FLOAT:
            return float(value)
        elif dtype == DataType.BOOLEAN:
            return value.lower() == 'true'
        elif dtype == DataType.DATE:
            return datetime.strptime(value, '%Y-%m-%d').date()
        elif dtype == DataType.TIMESTAMP:
            return datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        
        return value
