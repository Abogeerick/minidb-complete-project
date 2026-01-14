# MiniDB

A relational database management system built from scratch in Python. No external database libraries are used - the SQL parser, query executor, B-tree indexing, and storage engine are all custom implementations.

**Author:** Erick Aboge  
**Email:** abogeerick@gmail.com

---

## Quick Start

```bash
# Clone the repository
git clone https://github.com/Abogeerick/minidb-complete-project.git
cd minidb-complete-project

# Run the interactive SQL shell
python -m minidb

# Run the test suite
python minidb/tests/test_minidb.py

# Run the demo web application
pip install flask
cd demo_app
python app.py
# Open http://localhost:5000
```

---

## Project Structure

```
minidb-project/
├── minidb/
│   ├── core/
│   │   ├── database.py      # Main Database class (public API)
│   │   ├── executor.py      # Query execution engine
│   │   ├── repl.py          # Interactive SQL shell
│   │   ├── schema.py        # Table schema and catalog
│   │   └── types.py         # Data type definitions and validation
│   ├── parser/
│   │   ├── lexer.py         # SQL tokenizer
│   │   └── parser.py        # Recursive descent parser
│   ├── storage/
│   │   └── engine.py        # Persistence layer
│   ├── indexing/
│   │   └── btree.py         # B-tree index implementation
│   └── tests/
│       └── test_minidb.py   # Test suite (49 tests)
└── demo_app/
    ├── app.py               # Flask expense tracker
    └── templates/           # HTML templates
```

---

## Supported SQL

### Data Definition

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INTEGER,
    active BOOLEAN DEFAULT TRUE
);

DROP TABLE users;

CREATE INDEX idx_name ON users(name);
CREATE UNIQUE INDEX idx_email ON users(email);
```

### Data Manipulation

```sql
-- Insert
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30, TRUE);
INSERT INTO users (id, name, email) VALUES (2, 'Bob', 'bob@example.com');

-- Select
SELECT * FROM users;
SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC LIMIT 10;
SELECT DISTINCT category FROM products;

-- Update
UPDATE users SET age = 31, active = FALSE WHERE id = 1;

-- Delete
DELETE FROM users WHERE active = FALSE;
```

### Queries

```sql
-- Filtering
SELECT * FROM users WHERE age > 25 AND active = TRUE;
SELECT * FROM products WHERE name LIKE 'A%';
SELECT * FROM orders WHERE status IN ('pending', 'shipped');
SELECT * FROM items WHERE price BETWEEN 10 AND 50;
SELECT * FROM users WHERE email IS NOT NULL;

-- Joins
SELECT e.name, d.name AS department
FROM employees e
JOIN departments d ON e.dept_id = d.id;

SELECT e.name, d.name AS department
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id;

-- Aggregations
SELECT COUNT(*) FROM users;
SELECT category, SUM(amount), AVG(amount) FROM expenses GROUP BY category;
SELECT category, COUNT(*) AS cnt FROM products GROUP BY category HAVING cnt > 5;

-- Sorting and pagination
SELECT * FROM users ORDER BY created_at DESC LIMIT 20 OFFSET 40;
```

### Utility Commands

```sql
SHOW TABLES;
DESCRIBE users;
TRUNCATE TABLE logs;
```

---

## Supported Data Types

| Type | Description |
|------|-------------|
| `INTEGER` | 64-bit signed integer |
| `FLOAT` | Double-precision floating point |
| `VARCHAR(n)` | Variable-length string with max length n |
| `TEXT` | Unlimited length string |
| `BOOLEAN` | TRUE or FALSE |
| `DATE` | Date in YYYY-MM-DD format |
| `TIMESTAMP` | Date and time in YYYY-MM-DD HH:MM:SS format |

---

## Architecture

### Query Processing Pipeline

```
SQL String
    │
    ▼
┌─────────────────┐
│     Lexer       │  Tokenizes input into keywords, operators, literals
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│     Parser      │  Builds Abstract Syntax Tree using recursive descent
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Executor     │  Interprets AST, evaluates expressions, executes operations
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌───────┐ ┌───────────┐
│ Index │ │  Storage  │
│Manager│ │  Engine   │
└───────┘ └───────────┘
```

### Storage Model

- One JSON file per table containing all rows
- Separate catalog file for schema metadata
- Separate files for each B-tree index
- Row IDs are auto-generated integers

### Indexing

B-tree indexes are used for:
- Primary key lookups
- Unique constraint enforcement
- Range queries (BETWEEN, <, >, etc.)

Indexes are automatically created for PRIMARY KEY and UNIQUE columns.

---

## API Usage

```python
from minidb import Database

db = Database('./data')

# Create table
db.execute('''
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    )
''')

# Insert data
db.execute("INSERT INTO users VALUES (1, 'Alice')")

# Query
result = db.execute("SELECT * FROM users WHERE id = 1")
for row in result.rows:
    print(row)  # {'id': 1, 'name': 'Alice'}

# Get table list
tables = db.tables()

# Get row count
count = db.count('users')
```

---

## Demo Application

The `demo_app/` directory contains a Flask expense tracker that demonstrates:

- Table creation with foreign keys
- CRUD operations through a web interface
- JOIN queries for displaying related data
- Aggregate queries for dashboard statistics
- Index usage for category lookups

To run:

```bash
pip install flask
cd demo_app
python app.py
```

---

## Testing

The test suite covers:

- Table operations (CREATE, DROP, SHOW, DESCRIBE)
- All CRUD operations
- WHERE clause with all operators
- ORDER BY, LIMIT, OFFSET
- Aggregate functions and GROUP BY
- INNER and LEFT JOINs
- Index creation and unique constraints
- All data types

Run tests:

```bash
python minidb/tests/test_minidb.py
```

Expected output: 49 tests, all passing.

---

## Design Decisions

**Why recursive descent parsing?**  
Each grammar rule maps directly to a function, making the code readable and debuggable. Error messages include line and column numbers.

**Why JSON storage?**  
Simplicity and debuggability. The data files are human-readable, which is useful for development and demonstration. A production system would use a binary format.

**Why B-trees?**  
Standard choice for database indexes. O(log n) lookups, efficient range scans, and straightforward implementation.

**Why no query optimizer?**  
Scope limitation. The executor uses straightforward nested-loop evaluation. A real DBMS would include cost-based optimization.

---

## Limitations

- No transactions or ACID guarantees
- No concurrent access handling (single-threaded)
- No query optimization (plans are executed as written)
- JSON storage is not space-efficient
- No support for subqueries
- No ALTER TABLE

---

## Contact

Erick Aboge  
abogeerick@gmail.com  
[LinkedIn](https://www.linkedin.com/in/erick-aboge-3a09572a6/)  
[GitHub](https://github.com/Abogeerick)
