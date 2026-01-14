"""
Storage Engine - Handles persistence of table data to disk

Features:
- File-per-table storage model
- JSON-based serialization (simple but functional)
- Row ID generation
- Support for index storage
"""

import os
import json
from typing import Dict, List, Any, Optional, Iterator, Tuple
from dataclasses import dataclass, field
from threading import Lock
from datetime import datetime, date

from ..core.schema import TableSchema, Catalog
from ..core.types import TypeValidator


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return {'__datetime__': obj.strftime('%Y-%m-%d %H:%M:%S')}
        if isinstance(obj, date):
            return {'__date__': obj.strftime('%Y-%m-%d')}
        return super().default(obj)


def datetime_decoder(dct):
    """Custom JSON decoder for datetime objects"""
    if '__datetime__' in dct:
        return datetime.strptime(dct['__datetime__'], '%Y-%m-%d %H:%M:%S')
    if '__date__' in dct:
        return datetime.strptime(dct['__date__'], '%Y-%m-%d').date()
    return dct


@dataclass
class TableStorage:
    """
    Storage manager for a single table.
    Handles reading/writing rows and maintaining row IDs.
    """
    schema: TableSchema
    data_dir: str
    rows: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    next_row_id: int = 1
    _lock: Lock = field(default_factory=Lock)
    
    def __post_init__(self):
        self.table_file = os.path.join(self.data_dir, f"{self.schema.name}.json")
        self._load()
    
    def _load(self) -> None:
        """Load table data from disk"""
        if os.path.exists(self.table_file):
            with open(self.table_file, 'r') as f:
                data = json.load(f, object_hook=datetime_decoder)
                self.rows = {int(k): v for k, v in data.get('rows', {}).items()}
                self.next_row_id = data.get('next_row_id', 1)
    
    def _save(self) -> None:
        """Persist table data to disk"""
        data = {
            'rows': self.rows,
            'next_row_id': self.next_row_id,
        }
        with open(self.table_file, 'w') as f:
            json.dump(data, f, cls=DateTimeEncoder, indent=2)
    
    def insert(self, row: Dict[str, Any]) -> int:
        """Insert a row and return its row ID"""
        with self._lock:
            # Validate row against schema
            validated_row = self.schema.validate_row(row)
            
            # Check unique constraints
            self._check_unique_constraints(validated_row)
            
            # Assign row ID
            row_id = self.next_row_id
            self.next_row_id += 1
            
            # Store row
            self.rows[row_id] = validated_row
            self._save()
            
            return row_id
    
    def _check_unique_constraints(self, row: Dict[str, Any], exclude_id: Optional[int] = None) -> None:
        """Check unique and primary key constraints"""
        for col_name in self.schema.unique_columns:
            value = row.get(col_name)
            if value is None:
                continue
            
            for row_id, existing_row in self.rows.items():
                if exclude_id and row_id == exclude_id:
                    continue
                if existing_row.get(col_name) == value:
                    raise ValueError(f"Duplicate value '{value}' for unique column '{col_name}'")
    
    def get(self, row_id: int) -> Optional[Dict[str, Any]]:
        """Get a row by ID"""
        return self.rows.get(row_id)
    
    def update(self, row_id: int, updates: Dict[str, Any]) -> bool:
        """Update a row by ID"""
        with self._lock:
            if row_id not in self.rows:
                return False
            
            # Merge updates with existing row
            current_row = self.rows[row_id].copy()
            for key, value in updates.items():
                # Find matching column (case-insensitive)
                for col in self.schema.columns:
                    if col.name.lower() == key.lower():
                        current_row[col.name] = value
                        break
            
            # Validate
            validated_row = self.schema.validate_row(current_row)
            
            # Check unique constraints (excluding current row)
            self._check_unique_constraints(validated_row, exclude_id=row_id)
            
            self.rows[row_id] = validated_row
            self._save()
            return True
    
    def delete(self, row_id: int) -> bool:
        """Delete a row by ID"""
        with self._lock:
            if row_id not in self.rows:
                return False
            del self.rows[row_id]
            self._save()
            return True
    
    def scan(self) -> Iterator[Tuple[int, Dict[str, Any]]]:
        """Iterate over all rows"""
        for row_id, row in self.rows.items():
            yield row_id, row
    
    def count(self) -> int:
        """Return number of rows"""
        return len(self.rows)
    
    def truncate(self) -> None:
        """Remove all rows"""
        with self._lock:
            self.rows.clear()
            self.next_row_id = 1
            self._save()
    
    def drop(self) -> None:
        """Delete table file"""
        if os.path.exists(self.table_file):
            os.remove(self.table_file)


class StorageEngine:
    """
    Main storage engine that manages all table storage.
    Coordinates storage operations across tables.
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.catalog_file = os.path.join(data_dir, '_catalog.json')
        self.catalog = Catalog()
        self.tables: Dict[str, TableStorage] = {}
        
        # Ensure data directory exists
        os.makedirs(data_dir, exist_ok=True)
        
        # Load catalog
        self._load_catalog()
    
    def _load_catalog(self) -> None:
        """Load catalog and all table storage"""
        if os.path.exists(self.catalog_file):
            with open(self.catalog_file, 'r') as f:
                data = json.load(f)
                self.catalog = Catalog.from_dict(data)
            
            # Load storage for each table
            for table_name, schema in self.catalog.tables.items():
                self.tables[table_name] = TableStorage(schema, self.data_dir)
    
    def _save_catalog(self) -> None:
        """Persist catalog to disk"""
        with open(self.catalog_file, 'w') as f:
            json.dump(self.catalog.to_dict(), f, indent=2)
    
    def create_table(self, schema: TableSchema) -> None:
        """Create a new table"""
        self.catalog.create_table(schema)
        self.tables[schema.name.lower()] = TableStorage(schema, self.data_dir)
        self._save_catalog()
    
    def drop_table(self, name: str) -> None:
        """Drop a table"""
        table_name = name.lower()
        if table_name in self.tables:
            self.tables[table_name].drop()
            del self.tables[table_name]
        self.catalog.drop_table(name)
        self._save_catalog()
    
    def get_table_storage(self, name: str) -> Optional[TableStorage]:
        """Get storage for a table"""
        return self.tables.get(name.lower())
    
    def get_table_schema(self, name: str) -> Optional[TableSchema]:
        """Get schema for a table"""
        return self.catalog.get_table(name)
    
    def table_exists(self, name: str) -> bool:
        """Check if table exists"""
        return self.catalog.table_exists(name)
    
    def list_tables(self) -> List[str]:
        """List all tables"""
        return self.catalog.list_tables()
