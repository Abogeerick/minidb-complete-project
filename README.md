# MiniDB - A Relational Database Management System from Scratch

> **Pesapal Junior Developer Challenge '26 Submission**  
> **Author:** Erick Aboge  
> **Email:** abogeerick@gmail.com

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🎯 Overview

MiniDB is a **complete relational database management system built entirely from scratch** without using any existing database engines. It features a custom SQL parser, B-tree indexing, query executor, and persistent storage - demonstrating deep understanding of how databases work under the hood.

### Key Features

✅ **Complete SQL Parser** - Hand-written lexer and recursive descent parser  
✅ **B-Tree Indexing** - O(log n) lookups with range query support  
✅ **CRUD Operations** - Full INSERT, SELECT, UPDATE, DELETE support  
✅ **JOIN Support** - INNER JOIN, LEFT JOIN with ON conditions  
✅ **Aggregations** - COUNT, SUM, AVG, MIN, MAX with GROUP BY  
✅ **Data Type System** - INTEGER, FLOAT, VARCHAR, BOOLEAN, DATE, TIMESTAMP  
✅ **Constraints** - PRIMARY KEY, UNIQUE, NOT NULL, DEFAULT  
✅ **Interactive REPL** - Command-line SQL shell  
✅ **Demo Web App** - Flask expense tracker showcasing all features

## 📁 Project Structure

```
pesapal-challenge/
├── minidb/                     # Core RDBMS Implementation
│   ├── __init__.py
│   ├── __main__.py            # Entry point
│   ├── core/
│   │   ├── database.py        # Main Database class
│   │   ├── executor.py        # Query executor
│   │   ├── repl.py            # Interactive SQL shell
│   │   ├── schema.py          # Table schemas & catalog
│   │   └── types.py           # Data type definitions
│   ├── parser/
│   │   ├── lexer.py           # SQL tokenizer
│   │   └── parser.py          # Recursive descent parser → AST
│   ├── storage/
│   │   └── engine.py          # Persistence layer
│   ├── indexing/
│   │   └── btree.py           # B-tree index implementation
│   └── tests/
│       └── test_minidb.py     # Comprehensive test suite
│
└── demo_app/                   # Flask Demo Application
    ├── app.py                  # Expense tracker app
    └── templates/              # HTML templates
```

## 🏗️ Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                         SQL Query                               │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     LEXER (Tokenizer)                           │
│  • Converts SQL string to token stream                          │
│  • Handles keywords, operators, literals, identifiers           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                PARSER (Recursive Descent)                       │
│  • Builds Abstract Syntax Tree (AST)                           │
│  • Validates SQL grammar                                        │
│  • Supports SELECT, INSERT, UPDATE, DELETE, CREATE, DROP       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    QUERY EXECUTOR                               │
│  • Interprets AST nodes                                         │
│  • Evaluates expressions                                        │
│  • Performs JOINs, aggregations                                 │
│  • Applies WHERE filters, ORDER BY, LIMIT                       │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
┌─────────────────────────┐     ┌─────────────────────────┐
│    INDEX MANAGER        │     │    STORAGE ENGINE       │
│  • B-tree indexes       │     │  • Table data files     │
│  • O(log n) lookups     │     │  • Schema catalog       │
│  • Range queries        │     │  • Row ID management    │
└─────────────────────────┘     └─────────────────────────┘
              │                               │
              └───────────────┬───────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     FILE SYSTEM                                 │
│  • JSON-based persistence                                       │
│  • One file per table + catalog + indexes                       │
└─────────────────────────────────────────────────────────────────┘
```

### Why These Design Choices?

1. **Recursive Descent Parser**: Chosen for its clarity and maintainability. Each grammar rule maps directly to a parsing function, making the code self-documenting.

2. **B-Tree Indexing**: Industry-standard data structure for databases. Provides O(log n) lookups and supports range queries efficiently.

3. **JSON Storage**: While not as performant as binary formats, JSON is human-readable and debuggable - perfect for demonstrating concepts.

4. **Expression Evaluator Pattern**: Clean separation between parsing and execution. Makes it easy to add new operators and functions.

## 🚀 Quick Start

### 1. Run the Interactive REPL

```bash
cd pesapal-challenge
python -m minidb
```

You'll see the MiniDB banner and prompt:

```
╔══════════════════════════════════════════════════════════════╗
║   ███╗   ███╗██╗███╗   ██╗██╗██████╗ ██████╗                ║
║   ████╗ ████║██║████╗  ██║██║██╔══██╗██╔══██╗               ║
║   ...                                                        ║
╚══════════════════════════════════════════════════════════════╝

minidb> 
```

### 2. Try Some SQL Commands

```sql
-- Create a table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    age INTEGER,
    active BOOLEAN DEFAULT TRUE
);

-- Insert data
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30, TRUE);
INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25, TRUE);
INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 35, FALSE);

-- Query with conditions
SELECT name, age FROM users WHERE age > 25 ORDER BY age DESC;

-- Aggregations
SELECT COUNT(*) as total, AVG(age) as avg_age FROM users WHERE active = TRUE;

-- Update
UPDATE users SET age = 31 WHERE name = 'Alice';

-- Delete
DELETE FROM users WHERE active = FALSE;
```

### 3. Run the Demo Web App

```bash
cd demo_app
pip install flask
python app.py
```

Open http://localhost:5000 to see the expense tracker in action!

### 4. Run Tests

```bash
python -m pytest minidb/tests/test_minidb.py -v
# Or simply:
python minidb/tests/test_minidb.py
```

## 📖 SQL Reference

### Supported Statements

| Statement | Example |
|-----------|---------|
| CREATE TABLE | `CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))` |
| DROP TABLE | `DROP TABLE users` |
| INSERT | `INSERT INTO users VALUES (1, 'Alice')` |
| SELECT | `SELECT * FROM users WHERE id = 1` |
| UPDATE | `UPDATE users SET name = 'Bob' WHERE id = 1` |
| DELETE | `DELETE FROM users WHERE id = 1` |
| CREATE INDEX | `CREATE INDEX idx_name ON users(name)` |
| SHOW TABLES | `SHOW TABLES` |
| DESCRIBE | `DESCRIBE users` |
| TRUNCATE | `TRUNCATE TABLE users` |

### Supported Data Types

| Type | Description | Example |
|------|-------------|---------|
| INTEGER | Whole numbers | `42`, `-17` |
| FLOAT | Decimal numbers | `3.14`, `-2.5` |
| VARCHAR(n) | String with max length n | `'Hello'` |
| TEXT | Unlimited string | `'Long text...'` |
| BOOLEAN | TRUE or FALSE | `TRUE`, `FALSE` |
| DATE | Date value | `'2024-01-15'` |
| TIMESTAMP | Date and time | `'2024-01-15 10:30:00'` |

### WHERE Operators

| Operator | Example |
|----------|---------|
| `=`, `!=`, `<>` | `WHERE id = 1` |
| `<`, `>`, `<=`, `>=` | `WHERE age >= 18` |
| `AND`, `OR`, `NOT` | `WHERE age > 18 AND active = TRUE` |
| `LIKE` | `WHERE name LIKE 'A%'` |
| `IN` | `WHERE status IN ('active', 'pending')` |
| `BETWEEN` | `WHERE age BETWEEN 18 AND 65` |
| `IS NULL`, `IS NOT NULL` | `WHERE email IS NOT NULL` |

### JOIN Syntax

```sql
-- INNER JOIN
SELECT e.name, d.name as department
FROM employees e
JOIN departments d ON e.dept_id = d.id;

-- LEFT JOIN
SELECT e.name, d.name as department
FROM employees e
LEFT JOIN departments d ON e.dept_id = d.id;
```

### Aggregations

```sql
SELECT 
    category,
    COUNT(*) as count,
    SUM(amount) as total,
    AVG(amount) as average,
    MIN(amount) as minimum,
    MAX(amount) as maximum
FROM expenses
GROUP BY category
HAVING total > 100
ORDER BY total DESC;
```

## 🧪 Demo Application

The demo app is a **fully functional expense tracker** that showcases:

- **Dashboard**: Aggregated stats using `SUM()`, `COUNT()`, `AVG()`
- **Expense List**: `JOIN` query linking expenses to categories
- **Add/Edit/Delete**: Full CRUD operations
- **Categories**: `LEFT JOIN` with `GROUP BY` for expense counts
- **Live SQL Display**: Shows actual SQL being executed

### Screenshots

The demo shows SQL queries being executed in real-time, demonstrating:

```sql
-- Dashboard query
SELECT c.name, c.color, SUM(e.amount) as total
FROM expenses e
JOIN categories c ON e.category_id = c.id
GROUP BY c.name, c.color
ORDER BY total DESC;

-- Categories with expense counts
SELECT c.id, c.name, c.color, c.icon, COUNT(e.id) as expense_count
FROM categories c
LEFT JOIN expenses e ON c.id = e.category_id
GROUP BY c.id, c.name, c.color, c.icon
ORDER BY c.name;
```

## 🔧 Implementation Details

### B-Tree Index

The B-tree implementation supports:
- Configurable branching factor (order)
- Duplicate keys (for non-unique indexes)
- Range queries: `WHERE price BETWEEN 10 AND 50`
- Auto-balancing on insert/delete

```python
# Internal structure
class BTreeNode:
    keys: List[Any]           # Key values
    values: List[List[int]]   # Lists of row IDs per key
    children: List[int]       # Child node IDs
    is_leaf: bool
```

### Query Execution Pipeline

1. **Tokenization**: SQL string → Token stream
2. **Parsing**: Tokens → Abstract Syntax Tree (AST)
3. **Execution**: AST → Result set
4. **Expression Evaluation**: Handles nested operations, type coercion

### Storage Format

Each table is stored as a JSON file:
```json
{
  "rows": {
    "1": {"id": 1, "name": "Alice", "age": 30},
    "2": {"id": 2, "name": "Bob", "age": 25}
  },
  "next_row_id": 3
}
```

## 📊 Test Coverage

The test suite covers:

| Category | Tests |
|----------|-------|
| Table Operations | CREATE, DROP, SHOW, DESCRIBE |
| CRUD | INSERT, SELECT, UPDATE, DELETE |
| WHERE Clause | All operators, AND/OR/NOT |
| ORDER BY | ASC, DESC, LIMIT, OFFSET |
| Aggregations | COUNT, SUM, AVG, MIN, MAX, GROUP BY |
| JOINs | INNER, LEFT, with aggregations |
| Indexing | CREATE INDEX, UNIQUE constraints |
| Data Types | All supported types |

Run tests: `python minidb/tests/test_minidb.py`

## 🎓 What I Learned

Building this RDBMS taught me:

1. **Parser Design**: How lexers and parsers work together to process language
2. **Data Structures**: Practical application of B-trees for indexing
3. **Query Optimization**: How JOIN order and indexing affect performance
4. **Type Systems**: Implementing type validation and coercion
5. **Architecture**: Clean separation of concerns in complex systems

## 📞 Contact

**Erick Aboge**  
📧 abogeerick@gmail.com  
🔗 [LinkedIn](https://www.linkedin.com/in/erick-aboge-3a09572a6/)  
🌐 [Portfolio](https://erick-aboge-portfolio.vercel.app/)  
💻 [GitHub](https://github.com/Abogeerick)

---

*Built with ❤️ for the Pesapal Junior Developer Challenge '26*
