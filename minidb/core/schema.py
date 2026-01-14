"""
Schema Module - Defines table structure, columns, and constraints

Supports:
- Column definitions with types
- PRIMARY KEY constraint
- UNIQUE constraint
- NOT NULL constraint
- Foreign key references (for join operations)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any
from .types import ColumnType, DataType, TypeValidator


@dataclass
class Column:
    """Represents a column in a table"""
    name: str
    col_type: ColumnType
    primary_key: bool = False
    unique: bool = False
    not_null: bool = False
    default: Any = None
    references: Optional[tuple] = None  # (table_name, column_name) for FK
    
    def __post_init__(self):
        # Primary keys are implicitly NOT NULL and UNIQUE
        if self.primary_key:
            self.not_null = True
            self.unique = True


@dataclass
class TableSchema:
    """Represents the schema of a table"""
    name: str
    columns: List[Column] = field(default_factory=list)
    primary_key: Optional[str] = None  # Column name that is PK
    unique_columns: Set[str] = field(default_factory=set)
    
    def __post_init__(self):
        self._column_map: Dict[str, Column] = {}
        self._column_indices: Dict[str, int] = {}
        
        for idx, col in enumerate(self.columns):
            self._column_map[col.name.lower()] = col
            self._column_indices[col.name.lower()] = idx
            
            if col.primary_key:
                self.primary_key = col.name
            if col.unique:
                self.unique_columns.add(col.name)
    
    def add_column(self, column: Column) -> None:
        """Add a column to the schema"""
        if column.name.lower() in self._column_map:
            raise ValueError(f"Column '{column.name}' already exists")
        
        self.columns.append(column)
        self._column_map[column.name.lower()] = column
        self._column_indices[column.name.lower()] = len(self.columns) - 1
        
        if column.primary_key:
            if self.primary_key:
                raise ValueError("Table already has a primary key")
            self.primary_key = column.name
        
        if column.unique:
            self.unique_columns.add(column.name)
    
    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name (case-insensitive)"""
        return self._column_map.get(name.lower())
    
    def get_column_index(self, name: str) -> Optional[int]:
        """Get column index by name"""
        return self._column_indices.get(name.lower())
    
    def get_column_names(self) -> List[str]:
        """Get list of column names"""
        return [col.name for col in self.columns]
    
    def validate_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and convert a row of data"""
        validated = {}
        
        for col in self.columns:
            col_name_lower = col.name.lower()
            
            # Get value from row (case-insensitive)
            value = None
            for key, val in row.items():
                if key.lower() == col_name_lower:
                    value = val
                    break
            
            # Handle missing values
            if value is None:
                if col.default is not None:
                    value = col.default
                elif col.not_null:
                    raise ValueError(f"Column '{col.name}' cannot be NULL")
            
            # Validate and convert
            if value is not None:
                value = TypeValidator.validate_and_convert(value, col.col_type)
            
            validated[col.name] = value
        
        return validated
    
    def to_dict(self) -> dict:
        """Serialize schema to dictionary"""
        return {
            'name': self.name,
            'columns': [
                {
                    'name': col.name,
                    'type': str(col.col_type),
                    'primary_key': col.primary_key,
                    'unique': col.unique,
                    'not_null': col.not_null,
                    'default': col.default,
                }
                for col in self.columns
            ],
            'primary_key': self.primary_key,
            'unique_columns': list(self.unique_columns),
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'TableSchema':
        """Deserialize schema from dictionary"""
        schema = cls(name=data['name'])
        
        for col_data in data['columns']:
            col = Column(
                name=col_data['name'],
                col_type=TypeValidator.parse_type(col_data['type']),
                primary_key=col_data.get('primary_key', False),
                unique=col_data.get('unique', False),
                not_null=col_data.get('not_null', False),
                default=col_data.get('default'),
            )
            schema.add_column(col)
        
        return schema


class Catalog:
    """
    System catalog that manages all table schemas.
    This is the metadata store for the database.
    """
    
    def __init__(self):
        self.tables: Dict[str, TableSchema] = {}
    
    def create_table(self, schema: TableSchema) -> None:
        """Register a new table schema"""
        table_name = schema.name.lower()
        if table_name in self.tables:
            raise ValueError(f"Table '{schema.name}' already exists")
        self.tables[table_name] = schema
    
    def drop_table(self, name: str) -> None:
        """Remove a table schema"""
        table_name = name.lower()
        if table_name not in self.tables:
            raise ValueError(f"Table '{name}' does not exist")
        del self.tables[table_name]
    
    def get_table(self, name: str) -> Optional[TableSchema]:
        """Get table schema by name"""
        return self.tables.get(name.lower())
    
    def table_exists(self, name: str) -> bool:
        """Check if table exists"""
        return name.lower() in self.tables
    
    def list_tables(self) -> List[str]:
        """List all table names"""
        return list(self.tables.keys())
    
    def to_dict(self) -> dict:
        """Serialize catalog to dictionary"""
        return {
            'tables': {name: schema.to_dict() for name, schema in self.tables.items()}
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Catalog':
        """Deserialize catalog from dictionary"""
        catalog = cls()
        for name, schema_data in data.get('tables', {}).items():
            catalog.tables[name] = TableSchema.from_dict(schema_data)
        return catalog
