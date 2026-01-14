"""
Query Executor - Executes parsed SQL statements

Takes AST nodes from the parser and executes them against
the storage engine, returning results.
"""

from typing import Any, Dict, List, Optional, Tuple, Set
from dataclasses import dataclass
import re
import fnmatch

from ..parser.parser import (
    SelectStatement, InsertStatement, UpdateStatement, DeleteStatement,
    CreateTableStatement, DropTableStatement, CreateIndexStatement, DropIndexStatement,
    ShowTablesStatement, DescribeTableStatement, TruncateTableStatement,
    ColumnRef, Literal, BinaryOp, UnaryOp, FunctionCall, TableRef, JoinClause,
    JoinType, OrderDirection, ColumnDef
)
from ..storage.engine import StorageEngine, TableStorage
from ..indexing.btree import IndexManager
from ..core.schema import TableSchema, Column
from ..core.types import TypeValidator


@dataclass
class QueryResult:
    """Result of a query execution"""
    columns: List[str]
    rows: List[Dict[str, Any]]
    affected_rows: int = 0
    message: str = ""


class ExpressionEvaluator:
    """Evaluates expressions against row data"""
    
    def __init__(self, row: Dict[str, Any], tables: Dict[str, Dict[str, Any]] = None):
        self.row = row
        self.tables = tables or {}  # For multi-table queries (JOINs)
    
    def evaluate(self, expr: Any) -> Any:
        """Evaluate an expression"""
        if isinstance(expr, Literal):
            return expr.value
        
        elif isinstance(expr, ColumnRef):
            return self._get_column_value(expr)
        
        elif isinstance(expr, BinaryOp):
            return self._eval_binary_op(expr)
        
        elif isinstance(expr, UnaryOp):
            return self._eval_unary_op(expr)
        
        elif isinstance(expr, FunctionCall):
            # Aggregate functions are handled separately
            return self._eval_function(expr)
        
        elif expr == '*':
            return '*'
        
        return expr
    
    def _get_column_value(self, col_ref: ColumnRef) -> Any:
        """Get value of a column reference"""
        if col_ref.table:
            # Qualified column reference (table.column)
            if col_ref.table in self.tables:
                return self.tables[col_ref.table].get(col_ref.column)
            # Try case-insensitive match
            for tbl_name, tbl_data in self.tables.items():
                if tbl_name.lower() == col_ref.table.lower():
                    for col_name, val in tbl_data.items():
                        if col_name.lower() == col_ref.column.lower():
                            return val
        
        # Search in row (case-insensitive)
        for key, value in self.row.items():
            if key.lower() == col_ref.column.lower():
                return value
        
        # Search in all tables
        for tbl_data in self.tables.values():
            for key, value in tbl_data.items():
                if key.lower() == col_ref.column.lower():
                    return value
        
        return None
    
    def _eval_binary_op(self, op: BinaryOp) -> Any:
        """Evaluate binary operation"""
        # Handle special operators first
        if op.operator == 'IS NULL':
            left = self.evaluate(op.left)
            return left is None
        
        if op.operator == 'IS NOT NULL':
            left = self.evaluate(op.left)
            return left is not None
        
        if op.operator == 'IN':
            left = self.evaluate(op.left)
            if isinstance(op.right, list):
                values = [self.evaluate(v) for v in op.right]
                return left in values
            return False
        
        if op.operator == 'LIKE':
            left = self.evaluate(op.left)
            pattern = self.evaluate(op.right)
            if left is None or pattern is None:
                return False
            # Convert SQL LIKE pattern to regex
            regex_pattern = pattern.replace('%', '.*').replace('_', '.')
            return bool(re.match(f'^{regex_pattern}$', str(left), re.IGNORECASE))
        
        left = self.evaluate(op.left)
        right = self.evaluate(op.right)
        
        # Logical operators
        if op.operator == 'AND':
            return bool(left) and bool(right)
        if op.operator == 'OR':
            return bool(left) or bool(right)
        
        # Comparison operators
        if op.operator == '=':
            return left == right
        if op.operator in ('!=', '<>'):
            return left != right
        if op.operator == '<':
            return left < right if left is not None and right is not None else False
        if op.operator == '>':
            return left > right if left is not None and right is not None else False
        if op.operator == '<=':
            return left <= right if left is not None and right is not None else False
        if op.operator == '>=':
            return left >= right if left is not None and right is not None else False
        
        # Arithmetic operators
        if op.operator == '+':
            return (left or 0) + (right or 0)
        if op.operator == '-':
            return (left or 0) - (right or 0)
        if op.operator == '*':
            return (left or 0) * (right or 0)
        if op.operator == '/':
            if right == 0:
                return None
            return (left or 0) / right
        
        return None
    
    def _eval_unary_op(self, op: UnaryOp) -> Any:
        """Evaluate unary operation"""
        operand = self.evaluate(op.operand)
        
        if op.operator == 'NOT':
            return not bool(operand)
        if op.operator == '-':
            return -(operand or 0)
        
        return operand
    
    def _eval_function(self, func: FunctionCall) -> Any:
        """Evaluate scalar function (aggregates handled elsewhere)"""
        # Scalar functions could be added here
        return None


class QueryExecutor:
    """
    Executes SQL queries against the database.
    
    Handles all CRUD operations and DDL statements.
    """
    
    def __init__(self, storage: StorageEngine, index_manager: IndexManager):
        self.storage = storage
        self.index_manager = index_manager
    
    def execute(self, ast: Any) -> QueryResult:
        """Execute a parsed statement"""
        if isinstance(ast, SelectStatement):
            return self._execute_select(ast)
        elif isinstance(ast, InsertStatement):
            return self._execute_insert(ast)
        elif isinstance(ast, UpdateStatement):
            return self._execute_update(ast)
        elif isinstance(ast, DeleteStatement):
            return self._execute_delete(ast)
        elif isinstance(ast, CreateTableStatement):
            return self._execute_create_table(ast)
        elif isinstance(ast, DropTableStatement):
            return self._execute_drop_table(ast)
        elif isinstance(ast, CreateIndexStatement):
            return self._execute_create_index(ast)
        elif isinstance(ast, DropIndexStatement):
            return self._execute_drop_index(ast)
        elif isinstance(ast, ShowTablesStatement):
            return self._execute_show_tables()
        elif isinstance(ast, DescribeTableStatement):
            return self._execute_describe_table(ast)
        elif isinstance(ast, TruncateTableStatement):
            return self._execute_truncate_table(ast)
        else:
            raise ValueError(f"Unknown statement type: {type(ast)}")
    
    def _execute_select(self, stmt: SelectStatement) -> QueryResult:
        """Execute SELECT statement"""
        # Get base table data
        if stmt.from_table is None:
            # Simple expressions without table
            evaluator = ExpressionEvaluator({})
            row = {}
            for col in stmt.columns:
                if isinstance(col, tuple):
                    expr, alias = col
                    row[alias] = evaluator.evaluate(expr)
                else:
                    value = evaluator.evaluate(col)
                    if isinstance(col, ColumnRef):
                        row[col.alias or col.column] = value
                    else:
                        row[str(col)] = value
            return QueryResult(columns=list(row.keys()), rows=[row])
        
        # Get rows from base table
        table_storage = self.storage.get_table_storage(stmt.from_table.name)
        if table_storage is None:
            raise ValueError(f"Table '{stmt.from_table.name}' does not exist")
        
        table_alias = stmt.from_table.alias or stmt.from_table.name
        
        # Build initial result set
        rows = []
        for row_id, row in table_storage.scan():
            rows.append({table_alias: row.copy(), '_row_id': row_id})
        
        # Process JOINs
        for join in stmt.joins:
            rows = self._process_join(rows, join, table_alias)
        
        # Apply WHERE filter
        if stmt.where:
            filtered_rows = []
            for row_data in rows:
                # Flatten row for evaluation
                flat_row = self._flatten_row(row_data)
                evaluator = ExpressionEvaluator(flat_row, row_data)
                if evaluator.evaluate(stmt.where):
                    filtered_rows.append(row_data)
            rows = filtered_rows
        
        # Handle GROUP BY and aggregates
        if stmt.group_by or self._has_aggregates(stmt.columns):
            rows = self._process_aggregates(rows, stmt)
        
        # Apply HAVING filter
        if stmt.having:
            filtered_rows = []
            for row in rows:
                evaluator = ExpressionEvaluator(row)
                if evaluator.evaluate(stmt.having):
                    filtered_rows.append(row)
            rows = filtered_rows
        
        # Handle DISTINCT
        if stmt.distinct:
            seen = set()
            unique_rows = []
            for row in rows:
                key = tuple(sorted(row.items()))
                if key not in seen:
                    seen.add(key)
                    unique_rows.append(row)
            rows = unique_rows
        
        # Apply ORDER BY
        if stmt.order_by:
            rows = self._apply_order_by(rows, stmt.order_by)
        
        # Apply LIMIT and OFFSET
        if stmt.offset:
            rows = rows[stmt.offset:]
        if stmt.limit:
            rows = rows[:stmt.limit]
        
        # Project columns
        result_rows, columns = self._project_columns(rows, stmt.columns, table_alias)
        
        return QueryResult(columns=columns, rows=result_rows)
    
    def _flatten_row(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """Flatten nested table data into single dict"""
        flat = {}
        for key, value in row_data.items():
            if key.startswith('_'):
                continue
            if isinstance(value, dict):
                flat.update(value)
            else:
                flat[key] = value
        return flat
    
    def _process_join(self, left_rows: List[Dict], join: JoinClause, 
                      left_alias: str) -> List[Dict]:
        """Process a JOIN operation"""
        right_storage = self.storage.get_table_storage(join.table.name)
        if right_storage is None:
            raise ValueError(f"Table '{join.table.name}' does not exist")
        
        right_alias = join.table.alias or join.table.name
        result = []
        
        # Get all right rows
        right_rows = list(right_storage.scan())
        
        for left_data in left_rows:
            matched = False
            
            for right_id, right_row in right_rows:
                # Build combined row for condition evaluation
                combined = left_data.copy()
                combined[right_alias] = right_row
                
                # Evaluate join condition
                if join.condition:
                    flat = self._flatten_row(combined)
                    evaluator = ExpressionEvaluator(flat, combined)
                    if not evaluator.evaluate(join.condition):
                        continue
                
                matched = True
                result.append(combined.copy())
            
            # Handle LEFT JOIN - include unmatched left rows
            if not matched and join.join_type == JoinType.LEFT:
                combined = left_data.copy()
                combined[right_alias] = {col.name: None for col in 
                                         self.storage.get_table_schema(join.table.name).columns}
                result.append(combined)
        
        # Handle RIGHT JOIN - include unmatched right rows
        if join.join_type == JoinType.RIGHT:
            matched_right_ids = set()
            for row in result:
                if right_alias in row and '_row_id' in row.get(right_alias, {}):
                    matched_right_ids.add(row[right_alias]['_row_id'])
            
            for right_id, right_row in right_rows:
                if right_id not in matched_right_ids:
                    combined = {right_alias: right_row}
                    for key in left_rows[0] if left_rows else []:
                        if isinstance(left_rows[0].get(key), dict):
                            combined[key] = {col: None for col in left_rows[0][key]}
                    result.append(combined)
        
        return result
    
    def _has_aggregates(self, columns: List[Any]) -> bool:
        """Check if column list has aggregate functions"""
        for col in columns:
            if isinstance(col, FunctionCall):
                return True
            if isinstance(col, tuple) and isinstance(col[0], FunctionCall):
                return True
        return False
    
    def _process_aggregates(self, rows: List[Dict], stmt: SelectStatement) -> List[Dict]:
        """Process GROUP BY and aggregate functions"""
        # Group rows
        if stmt.group_by:
            groups = {}
            for row_data in rows:
                flat = self._flatten_row(row_data)
                evaluator = ExpressionEvaluator(flat, row_data)
                
                # Build group key
                key_parts = []
                for expr in stmt.group_by:
                    key_parts.append(evaluator.evaluate(expr))
                key = tuple(key_parts)
                
                if key not in groups:
                    groups[key] = []
                groups[key].append(row_data)
        else:
            # Single group for all rows
            groups = {(): rows}
        
        # Compute aggregates for each group
        result = []
        for group_key, group_rows in groups.items():
            row = {}
            
            # Add group by columns
            if stmt.group_by:
                for i, expr in enumerate(stmt.group_by):
                    if isinstance(expr, ColumnRef):
                        row[expr.column] = group_key[i]
            
            # Compute aggregates
            for col in stmt.columns:
                expr = col[0] if isinstance(col, tuple) else col
                alias = col[1] if isinstance(col, tuple) else None
                
                if isinstance(expr, FunctionCall):
                    value = self._compute_aggregate(expr, group_rows)
                    col_name = alias or f"{expr.name}({', '.join(str(a) for a in expr.args)})"
                    row[col_name] = value
                elif isinstance(expr, ColumnRef) and expr.column not in row:
                    # Get value from first row in group
                    if group_rows:
                        flat = self._flatten_row(group_rows[0])
                        evaluator = ExpressionEvaluator(flat, group_rows[0])
                        row[alias or expr.column] = evaluator.evaluate(expr)
            
            result.append(row)
        
        return result
    
    def _compute_aggregate(self, func: FunctionCall, rows: List[Dict]) -> Any:
        """Compute aggregate function value"""
        values = []
        
        for row_data in rows:
            flat = self._flatten_row(row_data)
            evaluator = ExpressionEvaluator(flat, row_data)
            
            for arg in func.args:
                if arg == '*':
                    values.append(1)  # For COUNT(*)
                else:
                    val = evaluator.evaluate(arg)
                    if val is not None:
                        values.append(val)
        
        if func.distinct:
            values = list(set(values))
        
        if func.name == 'COUNT':
            return len(values)
        elif func.name == 'SUM':
            return sum(values) if values else 0
        elif func.name == 'AVG':
            return sum(values) / len(values) if values else None
        elif func.name == 'MIN':
            return min(values) if values else None
        elif func.name == 'MAX':
            return max(values) if values else None
        
        return None
    
    def _apply_order_by(self, rows: List[Dict], order_by: List) -> List[Dict]:
        """Apply ORDER BY sorting"""
        def sort_key(row):
            keys = []
            flat = self._flatten_row(row) if not all(isinstance(v, (int, float, str, bool, type(None))) 
                                                      for v in row.values()) else row
            evaluator = ExpressionEvaluator(flat if isinstance(flat, dict) else row)
            
            for item in order_by:
                val = evaluator.evaluate(item.expr)
                # Handle None values
                if val is None:
                    val = (0, '')  # Sort nulls first
                else:
                    val = (1, val)
                
                if item.direction == OrderDirection.DESC:
                    # Negate for descending
                    if isinstance(val[1], (int, float)):
                        val = (val[0], -val[1])
                    else:
                        val = (val[0], val[1])  # String comparison needs different handling
                
                keys.append(val)
            return keys
        
        # Custom sorting for mixed ASC/DESC
        from functools import cmp_to_key
        
        def compare(a, b):
            flat_a = self._flatten_row(a) if not all(isinstance(v, (int, float, str, bool, type(None))) 
                                                      for v in a.values()) else a
            flat_b = self._flatten_row(b) if not all(isinstance(v, (int, float, str, bool, type(None))) 
                                                      for v in b.values()) else b
            
            eval_a = ExpressionEvaluator(flat_a if isinstance(flat_a, dict) else a)
            eval_b = ExpressionEvaluator(flat_b if isinstance(flat_b, dict) else b)
            
            for item in order_by:
                val_a = eval_a.evaluate(item.expr)
                val_b = eval_b.evaluate(item.expr)
                
                # Handle None
                if val_a is None and val_b is None:
                    continue
                if val_a is None:
                    return -1 if item.direction == OrderDirection.ASC else 1
                if val_b is None:
                    return 1 if item.direction == OrderDirection.ASC else -1
                
                if val_a < val_b:
                    return -1 if item.direction == OrderDirection.ASC else 1
                if val_a > val_b:
                    return 1 if item.direction == OrderDirection.ASC else -1
            
            return 0
        
        return sorted(rows, key=cmp_to_key(compare))
    
    def _project_columns(self, rows: List[Dict], columns: List[Any], 
                         default_table: str) -> Tuple[List[Dict], List[str]]:
        """Project specified columns from result rows"""
        if not rows:
            return [], []
        
        result_rows = []
        column_names = []
        
        for row_data in rows:
            # Handle pre-aggregated rows (already flat)
            if all(not isinstance(v, dict) for v in row_data.values() if not str(v).startswith('_')):
                flat = {k: v for k, v in row_data.items() if not k.startswith('_')}
            else:
                flat = self._flatten_row(row_data)
            
            evaluator = ExpressionEvaluator(flat, row_data)
            result_row = {}
            
            for col in columns:
                if col == '*':
                    # Select all columns
                    for key, value in flat.items():
                        if not key.startswith('_'):
                            result_row[key] = value
                            if key not in column_names:
                                column_names.append(key)
                elif isinstance(col, tuple):
                    expr, alias = col
                    # For aggregates, the value is already computed and stored by alias
                    if isinstance(expr, FunctionCall):
                        result_row[alias] = flat.get(alias)
                    else:
                        result_row[alias] = evaluator.evaluate(expr)
                    if alias not in column_names:
                        column_names.append(alias)
                elif isinstance(col, ColumnRef):
                    value = evaluator.evaluate(col)
                    name = col.alias or col.column
                    result_row[name] = value
                    if name not in column_names:
                        column_names.append(name)
                elif isinstance(col, FunctionCall):
                    name = f"{col.name}({', '.join(str(a) for a in col.args)})"
                    result_row[name] = flat.get(name)
                    if name not in column_names:
                        column_names.append(name)
                else:
                    value = evaluator.evaluate(col)
                    name = str(col)
                    result_row[name] = value
                    if name not in column_names:
                        column_names.append(name)
            
            result_rows.append(result_row)
        
        return result_rows, column_names
    
    def _execute_insert(self, stmt: InsertStatement) -> QueryResult:
        """Execute INSERT statement"""
        table_storage = self.storage.get_table_storage(stmt.table)
        if table_storage is None:
            raise ValueError(f"Table '{stmt.table}' does not exist")
        
        schema = self.storage.get_table_schema(stmt.table)
        indexes = self.index_manager.get_table_indexes(stmt.table)
        
        inserted = 0
        for value_row in stmt.values:
            # Build row dict
            if stmt.columns:
                if len(stmt.columns) != len(value_row):
                    raise ValueError("Column count doesn't match value count")
                row = {col: val.value if isinstance(val, Literal) else val 
                       for col, val in zip(stmt.columns, value_row)}
            else:
                # Use schema column order
                col_names = schema.get_column_names()
                if len(col_names) != len(value_row):
                    raise ValueError("Value count doesn't match table columns")
                row = {col: val.value if isinstance(val, Literal) else val 
                       for col, val in zip(col_names, value_row)}
            
            # Insert row
            row_id = table_storage.insert(row)
            
            # Update indexes
            for col_name, index in indexes.items():
                value = row.get(col_name)
                if value is not None:
                    index.insert(value, row_id)
            
            inserted += 1
        
        return QueryResult(
            columns=[],
            rows=[],
            affected_rows=inserted,
            message=f"Inserted {inserted} row(s)"
        )
    
    def _execute_update(self, stmt: UpdateStatement) -> QueryResult:
        """Execute UPDATE statement"""
        table_storage = self.storage.get_table_storage(stmt.table)
        if table_storage is None:
            raise ValueError(f"Table '{stmt.table}' does not exist")
        
        indexes = self.index_manager.get_table_indexes(stmt.table)
        updated = 0
        
        # Find rows to update
        rows_to_update = []
        for row_id, row in table_storage.scan():
            if stmt.where:
                evaluator = ExpressionEvaluator(row)
                if not evaluator.evaluate(stmt.where):
                    continue
            rows_to_update.append((row_id, row.copy()))
        
        # Update rows
        for row_id, old_row in rows_to_update:
            updates = {}
            for col, expr in stmt.assignments:
                evaluator = ExpressionEvaluator(old_row)
                value = evaluator.evaluate(expr)
                if isinstance(value, Literal):
                    value = value.value
                updates[col] = value
            
            # Update indexes (remove old, add new)
            for col_name, index in indexes.items():
                if col_name.lower() in [c.lower() for c in updates]:
                    old_value = old_row.get(col_name)
                    new_value = updates.get(col_name)
                    if old_value is not None:
                        index.delete(old_value, row_id)
                    if new_value is not None:
                        index.insert(new_value, row_id)
            
            table_storage.update(row_id, updates)
            updated += 1
        
        return QueryResult(
            columns=[],
            rows=[],
            affected_rows=updated,
            message=f"Updated {updated} row(s)"
        )
    
    def _execute_delete(self, stmt: DeleteStatement) -> QueryResult:
        """Execute DELETE statement"""
        table_storage = self.storage.get_table_storage(stmt.table)
        if table_storage is None:
            raise ValueError(f"Table '{stmt.table}' does not exist")
        
        indexes = self.index_manager.get_table_indexes(stmt.table)
        deleted = 0
        
        # Find rows to delete
        rows_to_delete = []
        for row_id, row in table_storage.scan():
            if stmt.where:
                evaluator = ExpressionEvaluator(row)
                if not evaluator.evaluate(stmt.where):
                    continue
            rows_to_delete.append((row_id, row))
        
        # Delete rows
        for row_id, row in rows_to_delete:
            # Remove from indexes
            for col_name, index in indexes.items():
                value = row.get(col_name)
                if value is not None:
                    index.delete(value, row_id)
            
            table_storage.delete(row_id)
            deleted += 1
        
        return QueryResult(
            columns=[],
            rows=[],
            affected_rows=deleted,
            message=f"Deleted {deleted} row(s)"
        )
    
    def _execute_create_table(self, stmt: CreateTableStatement) -> QueryResult:
        """Execute CREATE TABLE statement"""
        if self.storage.table_exists(stmt.table):
            if stmt.if_not_exists:
                return QueryResult(columns=[], rows=[], message=f"Table '{stmt.table}' already exists")
            raise ValueError(f"Table '{stmt.table}' already exists")
        
        # Build schema
        schema = TableSchema(name=stmt.table)
        
        for col_def in stmt.columns:
            col = Column(
                name=col_def.name,
                col_type=TypeValidator.parse_type(col_def.data_type),
                primary_key=col_def.primary_key,
                unique=col_def.unique,
                not_null=col_def.not_null,
                default=col_def.default,
            )
            schema.add_column(col)
        
        self.storage.create_table(schema)
        
        # Create index for primary key
        if schema.primary_key:
            self.index_manager.create_index(
                name=f"pk_{stmt.table}_{schema.primary_key}",
                table_name=stmt.table,
                column_name=schema.primary_key,
                unique=True
            )
        
        # Create indexes for unique columns
        for col in schema.columns:
            if col.unique and not col.primary_key:
                self.index_manager.create_index(
                    name=f"unique_{stmt.table}_{col.name}",
                    table_name=stmt.table,
                    column_name=col.name,
                    unique=True
                )
        
        return QueryResult(columns=[], rows=[], message=f"Table '{stmt.table}' created")
    
    def _execute_drop_table(self, stmt: DropTableStatement) -> QueryResult:
        """Execute DROP TABLE statement"""
        if not self.storage.table_exists(stmt.table):
            if stmt.if_exists:
                return QueryResult(columns=[], rows=[], message=f"Table '{stmt.table}' does not exist")
            raise ValueError(f"Table '{stmt.table}' does not exist")
        
        # Drop indexes first
        self.index_manager.drop_table_indexes(stmt.table)
        
        # Drop table
        self.storage.drop_table(stmt.table)
        
        return QueryResult(columns=[], rows=[], message=f"Table '{stmt.table}' dropped")
    
    def _execute_create_index(self, stmt: CreateIndexStatement) -> QueryResult:
        """Execute CREATE INDEX statement"""
        if not self.storage.table_exists(stmt.table):
            raise ValueError(f"Table '{stmt.table}' does not exist")
        
        schema = self.storage.get_table_schema(stmt.table)
        if not schema.get_column(stmt.column):
            raise ValueError(f"Column '{stmt.column}' does not exist in table '{stmt.table}'")
        
        # Create index
        index = self.index_manager.create_index(
            name=stmt.name,
            table_name=stmt.table,
            column_name=stmt.column,
            unique=stmt.unique
        )
        
        # Build index from existing data
        table_storage = self.storage.get_table_storage(stmt.table)
        for row_id, row in table_storage.scan():
            value = row.get(stmt.column)
            if value is not None:
                index.insert(value, row_id)
        
        return QueryResult(columns=[], rows=[], message=f"Index '{stmt.name}' created")
    
    def _execute_drop_index(self, stmt: DropIndexStatement) -> QueryResult:
        """Execute DROP INDEX statement"""
        self.index_manager.drop_index(stmt.table, stmt.name)
        return QueryResult(columns=[], rows=[], message=f"Index '{stmt.name}' dropped")
    
    def _execute_show_tables(self) -> QueryResult:
        """Execute SHOW TABLES statement"""
        tables = self.storage.list_tables()
        rows = [{'table_name': name} for name in tables]
        return QueryResult(columns=['table_name'], rows=rows)
    
    def _execute_describe_table(self, stmt: DescribeTableStatement) -> QueryResult:
        """Execute DESCRIBE TABLE statement"""
        schema = self.storage.get_table_schema(stmt.table)
        if schema is None:
            raise ValueError(f"Table '{stmt.table}' does not exist")
        
        rows = []
        for col in schema.columns:
            rows.append({
                'column_name': col.name,
                'data_type': str(col.col_type),
                'nullable': 'NO' if col.not_null else 'YES',
                'key': 'PRI' if col.primary_key else ('UNI' if col.unique else ''),
                'default': col.default,
            })
        
        return QueryResult(
            columns=['column_name', 'data_type', 'nullable', 'key', 'default'],
            rows=rows
        )
    
    def _execute_truncate_table(self, stmt: TruncateTableStatement) -> QueryResult:
        """Execute TRUNCATE TABLE statement"""
        table_storage = self.storage.get_table_storage(stmt.table)
        if table_storage is None:
            raise ValueError(f"Table '{stmt.table}' does not exist")
        
        # Clear indexes
        indexes = self.index_manager.get_table_indexes(stmt.table)
        for col_name, index in indexes.items():
            index.drop()
        
        # Truncate table
        table_storage.truncate()
        
        # Recreate indexes
        for col_name in indexes:
            self.index_manager.create_index(
                name=f"idx_{stmt.table}_{col_name}",
                table_name=stmt.table,
                column_name=col_name
            )
        
        return QueryResult(columns=[], rows=[], message=f"Table '{stmt.table}' truncated")
