"""
Database - Main entry point for MiniDB

This is the primary interface for interacting with MiniDB.
It coordinates the storage engine, parser, and query executor.
"""

import os
from typing import Any, Dict, List, Optional

from ..storage.engine import StorageEngine
from ..indexing.btree import IndexManager
from ..parser.parser import parse_sql
from .executor import QueryExecutor, QueryResult


class Database:
    """
    MiniDB Database instance.
    
    Usage:
        db = Database("./mydata")
        db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        result = db.execute("SELECT * FROM users")
        for row in result.rows:
            print(row)
    """
    
    def __init__(self, data_dir: str = "./minidb_data"):
        """
        Initialize MiniDB database.
        
        Args:
            data_dir: Directory to store database files
        """
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize components
        self.storage = StorageEngine(data_dir)
        self.index_manager = IndexManager(data_dir)
        self.executor = QueryExecutor(self.storage, self.index_manager)
    
    def execute(self, sql: str) -> QueryResult:
        """
        Execute a SQL statement.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            QueryResult containing columns, rows, and metadata
            
        Raises:
            ValueError: If SQL is invalid or execution fails
        """
        # Parse SQL
        ast = parse_sql(sql)
        
        # Execute
        return self.executor.execute(ast)
    
    def execute_many(self, sql: str) -> List[QueryResult]:
        """
        Execute multiple SQL statements separated by semicolons.
        
        Args:
            sql: Multiple SQL statements
            
        Returns:
            List of QueryResult objects
        """
        results = []
        
        # Simple split by semicolon (doesn't handle strings with semicolons)
        statements = [s.strip() for s in sql.split(';') if s.strip()]
        
        for stmt in statements:
            result = self.execute(stmt)
            results.append(result)
        
        return results
    
    def tables(self) -> List[str]:
        """List all tables in the database."""
        return self.storage.list_tables()
    
    def describe(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema information.
        
        Args:
            table_name: Name of table to describe
            
        Returns:
            Dictionary with table schema
        """
        schema = self.storage.get_table_schema(table_name)
        if schema is None:
            raise ValueError(f"Table '{table_name}' does not exist")
        return schema.to_dict()
    
    def count(self, table_name: str) -> int:
        """
        Get row count for a table.
        
        Args:
            table_name: Name of table
            
        Returns:
            Number of rows in table
        """
        storage = self.storage.get_table_storage(table_name)
        if storage is None:
            raise ValueError(f"Table '{table_name}' does not exist")
        return storage.count()
    
    def close(self) -> None:
        """Close database connection (placeholder for cleanup)."""
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
