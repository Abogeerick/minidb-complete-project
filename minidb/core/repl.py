"""
REPL - Interactive SQL shell for MiniDB

Provides a command-line interface for executing SQL statements
and managing the database.
"""

import sys
import os
from typing import Optional
from .database import Database


class REPL:
    """
    Interactive SQL REPL (Read-Eval-Print Loop) for MiniDB.
    
    Features:
    - Multi-line SQL input (statements ending with ;)
    - Command history
    - Special commands (.tables, .schema, .quit, etc.)
    - Pretty-printed results
    """
    
    BANNER = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   ███╗   ███╗██╗███╗   ██╗██╗██████╗ ██████╗                ║
║   ████╗ ████║██║████╗  ██║██║██╔══██╗██╔══██╗               ║
║   ██╔████╔██║██║██╔██╗ ██║██║██║  ██║██████╔╝               ║
║   ██║╚██╔╝██║██║██║╚██╗██║██║██║  ██║██╔══██╗               ║
║   ██║ ╚═╝ ██║██║██║ ╚████║██║██████╔╝██████╔╝               ║
║   ╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝╚═════╝ ╚═════╝                ║
║                                                              ║
║   A Simple Relational Database Management System             ║
║   Built for Pesapal Junior Dev Challenge '26                 ║
║                                                              ║
║   Author: Erick Aboge                                        ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝

Type .help for commands, or enter SQL statements.
Statements must end with a semicolon (;).
"""
    
    HELP = """
Special Commands:
  .help             Show this help message
  .tables           List all tables
  .schema <table>   Show schema for a table
  .count <table>    Show row count for a table
  .indexes <table>  Show indexes for a table
  .clear            Clear the screen
  .quit / .exit     Exit the REPL

SQL Commands:
  CREATE TABLE      Create a new table
  DROP TABLE        Delete a table
  INSERT INTO       Insert rows into a table
  SELECT            Query data from tables
  UPDATE            Update existing rows
  DELETE FROM       Delete rows from a table
  CREATE INDEX      Create an index on a column
  DROP INDEX        Remove an index
  SHOW TABLES       List all tables
  DESCRIBE          Show table structure
  TRUNCATE TABLE    Remove all rows from a table

Example:
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE
  );
  
  INSERT INTO users VALUES (1, 'Alice', 'alice@example.com');
  
  SELECT * FROM users WHERE name LIKE 'A%';
"""
    
    def __init__(self, data_dir: str = "./minidb_data"):
        """Initialize REPL with database connection."""
        self.db = Database(data_dir)
        self.running = False
        self.buffer = []
    
    def run(self) -> None:
        """Start the REPL loop."""
        self.running = True
        print(self.BANNER)
        
        while self.running:
            try:
                self._process_input()
            except KeyboardInterrupt:
                print("\n(Use .quit to exit)")
            except EOFError:
                print()
                self._quit()
    
    def _get_prompt(self) -> str:
        """Get the appropriate prompt."""
        if self.buffer:
            return "   ...> "
        return "minidb> "
    
    def _process_input(self) -> None:
        """Read and process user input."""
        try:
            line = input(self._get_prompt())
        except EOFError:
            raise
        
        line = line.strip()
        
        # Empty line
        if not line:
            return
        
        # Special commands (only when not in multi-line mode)
        if not self.buffer and line.startswith('.'):
            self._handle_command(line)
            return
        
        # Add to buffer
        self.buffer.append(line)
        
        # Check if statement is complete
        full_statement = ' '.join(self.buffer)
        if full_statement.rstrip().endswith(';'):
            self._execute_statement(full_statement)
            self.buffer = []
    
    def _handle_command(self, cmd: str) -> None:
        """Handle special dot commands."""
        parts = cmd.split(None, 1)
        command = parts[0].lower()
        args = parts[1] if len(parts) > 1 else None
        
        if command in ('.quit', '.exit', '.q'):
            self._quit()
        elif command == '.help':
            print(self.HELP)
        elif command == '.tables':
            self._show_tables()
        elif command == '.schema':
            self._show_schema(args)
        elif command == '.count':
            self._show_count(args)
        elif command == '.indexes':
            self._show_indexes(args)
        elif command == '.clear':
            os.system('clear' if os.name == 'posix' else 'cls')
        else:
            print(f"Unknown command: {command}")
            print("Type .help for available commands.")
    
    def _quit(self) -> None:
        """Exit the REPL."""
        print("Goodbye!")
        self.running = False
        self.db.close()
    
    def _show_tables(self) -> None:
        """List all tables."""
        tables = self.db.tables()
        if tables:
            print("\nTables:")
            for table in tables:
                count = self.db.count(table)
                print(f"  {table} ({count} rows)")
            print()
        else:
            print("No tables found.")
    
    def _show_schema(self, table_name: Optional[str]) -> None:
        """Show schema for a table."""
        if not table_name:
            print("Usage: .schema <table_name>")
            return
        
        try:
            schema = self.db.describe(table_name)
            print(f"\nTable: {schema['name']}")
            print("-" * 60)
            
            for col in schema['columns']:
                flags = []
                if col.get('primary_key'):
                    flags.append('PRIMARY KEY')
                if col.get('unique') and not col.get('primary_key'):
                    flags.append('UNIQUE')
                if col.get('not_null') and not col.get('primary_key'):
                    flags.append('NOT NULL')
                if col.get('default') is not None:
                    flags.append(f"DEFAULT {col['default']}")
                
                flags_str = ' '.join(flags)
                print(f"  {col['name']:20} {col['type']:15} {flags_str}")
            print()
        except ValueError as e:
            print(f"Error: {e}")
    
    def _show_count(self, table_name: Optional[str]) -> None:
        """Show row count for a table."""
        if not table_name:
            print("Usage: .count <table_name>")
            return
        
        try:
            count = self.db.count(table_name)
            print(f"{table_name}: {count} rows")
        except ValueError as e:
            print(f"Error: {e}")
    
    def _show_indexes(self, table_name: Optional[str]) -> None:
        """Show indexes for a table."""
        if not table_name:
            print("Usage: .indexes <table_name>")
            return
        
        indexes = self.db.index_manager.get_table_indexes(table_name)
        if indexes:
            print(f"\nIndexes on {table_name}:")
            for col_name, idx in indexes.items():
                unique = "UNIQUE " if idx.unique else ""
                print(f"  {idx.name}: {unique}INDEX on {col_name}")
            print()
        else:
            print(f"No indexes on {table_name}")
    
    def _execute_statement(self, sql: str) -> None:
        """Execute a SQL statement and display results."""
        try:
            result = self.db.execute(sql)
            
            if result.rows:
                self._print_results(result)
            elif result.message:
                print(result.message)
            
            if result.affected_rows > 0:
                print(f"({result.affected_rows} row(s) affected)")
            
        except Exception as e:
            print(f"Error: {e}")
    
    def _print_results(self, result) -> None:
        """Pretty-print query results as a table."""
        if not result.rows:
            print("(0 rows)")
            return
        
        # Calculate column widths
        columns = result.columns
        widths = {col: len(col) for col in columns}
        
        for row in result.rows:
            for col in columns:
                val = str(row.get(col, 'NULL'))
                widths[col] = max(widths[col], len(val))
        
        # Limit column width for readability
        max_width = 40
        widths = {col: min(w, max_width) for col, w in widths.items()}
        
        # Print header
        header = " | ".join(col.ljust(widths[col])[:widths[col]] for col in columns)
        separator = "-+-".join("-" * widths[col] for col in columns)
        
        print()
        print(header)
        print(separator)
        
        # Print rows
        for row in result.rows:
            values = []
            for col in columns:
                val = row.get(col)
                if val is None:
                    val_str = 'NULL'
                else:
                    val_str = str(val)
                values.append(val_str.ljust(widths[col])[:widths[col]])
            print(" | ".join(values))
        
        print(f"\n({len(result.rows)} row(s))")


def main():
    """Entry point for the REPL."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="MiniDB - A Simple Relational Database Management System"
    )
    parser.add_argument(
        '-d', '--data-dir',
        default='./minidb_data',
        help='Directory to store database files (default: ./minidb_data)'
    )
    parser.add_argument(
        '-e', '--execute',
        help='Execute SQL statement and exit'
    )
    parser.add_argument(
        '-f', '--file',
        help='Execute SQL from file and exit'
    )
    
    args = parser.parse_args()
    
    # Execute single statement
    if args.execute:
        db = Database(args.data_dir)
        try:
            result = db.execute(args.execute)
            if result.rows:
                for row in result.rows:
                    print(row)
            if result.message:
                print(result.message)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        return
    
    # Execute from file
    if args.file:
        db = Database(args.data_dir)
        try:
            with open(args.file, 'r') as f:
                sql = f.read()
            results = db.execute_many(sql)
            for result in results:
                if result.rows:
                    for row in result.rows:
                        print(row)
                if result.message:
                    print(result.message)
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        return
    
    # Start interactive REPL
    repl = REPL(args.data_dir)
    repl.run()


if __name__ == '__main__':
    main()
