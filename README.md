# MiniDB

A relational database management system built from scratch in Python. No external database libraries are used - the SQL parser, query executor, B-tree indexing, and storage engine are all custom implementations.

**Author:** Erick Aboge  
**Email:** abogeerick@gmail.com  
**Live Demo:** [https://your-app-runner-url.awsapprunner.com](https://your-app-runner-url.awsapprunner.com)

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

# Run the demo web application locally
pip install flask
cd demo_app
python app.py
# Open http://localhost:5000
```

---

## Live Demo

The expense tracker demo is deployed on AWS App Runner and can be accessed at:

**[https://your-app-runner-url.awsapprunner.com](https://your-app-runner-url.awsapprunner.com)**

### Demo Features

| Page | Description | SQL Features Demonstrated |
|------|-------------|---------------------------|
| **Dashboard** | Overview with statistics and charts | `SUM()`, `COUNT()`, `AVG()`, `GROUP BY` |
| **Expenses** | List, add, edit, delete expenses | `INSERT`, `UPDATE`, `DELETE`, `JOIN` |
| **Categories** | Manage expense categories | `LEFT JOIN`, `COUNT()`, `GROUP BY` |

### How to Use the Demo

1. **Dashboard** - View aggregated spending statistics. The SQL queries powering the dashboard are displayed in a terminal-style panel.

2. **Add Expense** - Click "Add Expense" to create a new entry. Select a category from the dropdown (populated via SQL query) and enter the amount and date.

3. **Categories** - View all categories with expense counts. The count uses a LEFT JOIN to include categories with zero expenses.

4. **View SQL** - Each page displays the actual SQL queries being executed by MiniDB in real-time.

---

## AWS Deployment

The application is deployed using AWS App Runner with the following configuration:

### Deployment Steps

1. **Push to GitHub**
   ```bash
   git push origin main
   ```

2. **Create App Runner Service**
   - Go to AWS Console > App Runner > Create Service
   - Source: GitHub repository
   - Branch: `main`
   - Build settings:
     - Runtime: Python 3
     - Build command: `pip install -r requirements.txt`
     - Start command: `cd demo_app && gunicorn --bind 0.0.0.0:8080 app:app`
     - Port: 8080

3. **Deploy**
   - Click "Create & Deploy"
   - Wait 3-5 minutes for the build
   - Access via the provided `.awsapprunner.com` URL

### AWS Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    AWS App Runner                        │
│  ┌─────────────────────────────────────────────────┐    │
│  │              Container Instance                  │    │
│  │  ┌─────────────┐    ┌────────────────────────┐  │    │
│  │  │   Gunicorn  │───>│      Flask App         │  │    │
│  │  │   (WSGI)    │    │  ┌──────────────────┐  │  │    │
│  │  └─────────────┘    │  │     MiniDB       │  │  │    │
│  │                     │  │  (Custom RDBMS)  │  │  │    │
│  │                     │  └──────────────────┘  │  │    │
│  │                     │           │            │  │    │
│  │                     │           ▼            │  │    │
│  │                     │  ┌──────────────────┐  │  │    │
│  │                     │  │  JSON Storage    │  │  │    │
│  │                     │  │  (ephemeral)     │  │  │    │
│  │                     │  └──────────────────┘  │  │    │
│  │                     └────────────────────────┘  │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Files for Deployment

- `Dockerfile` - Container configuration
- `requirements.txt` - Python dependencies
- `render.yaml` - Alternative deployment to Render.com

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
```

### Data Manipulation

```sql
INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30, TRUE);
SELECT * FROM users WHERE age > 25 ORDER BY age DESC LIMIT 10;
UPDATE users SET age = 31 WHERE id = 1;
DELETE FROM users WHERE active = FALSE;
```

### Joins and Aggregations

```sql
SELECT e.name, d.name AS department
FROM employees e
JOIN departments d ON e.dept_id = d.id;

SELECT category, COUNT(*), SUM(amount), AVG(amount)
FROM expenses
GROUP BY category
HAVING COUNT(*) > 5;
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
| `TIMESTAMP` | Date and time |

---

## Architecture

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

---

## API Usage

```python
from minidb import Database

db = Database('./data')

db.execute('''
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL
    )
''')

db.execute("INSERT INTO users VALUES (1, 'Alice')")

result = db.execute("SELECT * FROM users WHERE id = 1")
for row in result.rows:
    print(row)  # {'id': 1, 'name': 'Alice'}
```

---

## Testing

```bash
python minidb/tests/test_minidb.py
```

Expected output: **49 tests, all passing**

Test coverage includes:
- Table operations (CREATE, DROP, SHOW, DESCRIBE)
- All CRUD operations
- WHERE clause with all operators
- ORDER BY, LIMIT, OFFSET
- Aggregate functions and GROUP BY
- INNER and LEFT JOINs
- Index creation and unique constraints
- All data types

---

## Design Decisions

**Recursive Descent Parser** - Each grammar rule maps directly to a function, making the code readable and debuggable.

**B-Tree Indexing** - Standard choice for databases. O(log n) lookups and efficient range scans.

**JSON Storage** - Simple and debuggable for demonstration purposes.

---

## Limitations

- No transactions or ACID guarantees
- Single-threaded (no concurrent access)
- No query optimization
- Ephemeral storage on cloud deployment
- No subqueries or ALTER TABLE

---

## Contact

**Erick Aboge**  
Email: abogeerick@gmail.com  
LinkedIn: [linkedin.com/in/erick-aboge-3a09572a6](https://www.linkedin.com/in/erick-aboge-3a09572a6/)  
GitHub: [github.com/Abogeerick](https://github.com/Abogeerick)
