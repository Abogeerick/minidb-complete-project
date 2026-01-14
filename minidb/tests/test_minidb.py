#!/usr/bin/env python3
"""
Comprehensive Test Suite for MiniDB

Tests all major features:
- Table operations (CREATE, DROP)
- CRUD operations (INSERT, SELECT, UPDATE, DELETE)
- Data types and validation
- Indexing with B-tree
- JOIN operations
- Aggregations (COUNT, SUM, AVG, MIN, MAX)
- WHERE clause with various operators
- ORDER BY, LIMIT, OFFSET

Run: python -m pytest tests/test_minidb.py -v
Or:  python tests/test_minidb.py
"""

import os
import sys
import shutil
import tempfile
import unittest
from datetime import date, datetime

# Add parent directories to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from minidb import Database


class TestDatabase(unittest.TestCase):
    """Test database creation and basic operations"""
    
    def setUp(self):
        """Create a temporary database for each test"""
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
    
    def tearDown(self):
        """Clean up temporary database"""
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_create_table(self):
        """Test CREATE TABLE statement"""
        result = self.db.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE
            )
        """)
        self.assertIn("created", result.message.lower())
        self.assertIn("users", self.db.tables())
    
    def test_drop_table(self):
        """Test DROP TABLE statement"""
        self.db.execute("CREATE TABLE temp (id INTEGER)")
        self.assertIn("temp", self.db.tables())
        
        result = self.db.execute("DROP TABLE temp")
        self.assertIn("dropped", result.message.lower())
        self.assertNotIn("temp", self.db.tables())
    
    def test_show_tables(self):
        """Test SHOW TABLES statement"""
        self.db.execute("CREATE TABLE table1 (id INTEGER)")
        self.db.execute("CREATE TABLE table2 (id INTEGER)")
        
        result = self.db.execute("SHOW TABLES")
        table_names = [row['table_name'] for row in result.rows]
        self.assertIn("table1", table_names)
        self.assertIn("table2", table_names)
    
    def test_describe_table(self):
        """Test DESCRIBE statement"""
        self.db.execute("""
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                price FLOAT,
                in_stock BOOLEAN DEFAULT TRUE
            )
        """)
        
        result = self.db.execute("DESCRIBE products")
        columns = {row['column_name']: row for row in result.rows}
        
        self.assertIn('id', columns)
        self.assertEqual(columns['id']['key'], 'PRI')
        self.assertIn('name', columns)
        self.assertEqual(columns['name']['nullable'], 'NO')


class TestCRUD(unittest.TestCase):
    """Test INSERT, SELECT, UPDATE, DELETE operations"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
        self.db.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age INTEGER,
                active BOOLEAN DEFAULT TRUE
            )
        """)
    
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_insert_single_row(self):
        """Test INSERT with single row"""
        result = self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        self.assertEqual(result.affected_rows, 1)
    
    def test_insert_with_columns(self):
        """Test INSERT with explicit column list"""
        result = self.db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        self.assertEqual(result.affected_rows, 1)
    
    def test_insert_multiple_rows(self):
        """Test INSERT with multiple value lists"""
        result = self.db.execute("""
            INSERT INTO users VALUES 
                (1, 'Alice', 30, TRUE),
                (2, 'Bob', 25, TRUE),
                (3, 'Charlie', 35, FALSE)
        """)
        self.assertEqual(result.affected_rows, 3)
    
    def test_select_all(self):
        """Test SELECT * FROM table"""
        self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        self.db.execute("INSERT INTO users VALUES (2, 'Bob', 25, FALSE)")
        
        result = self.db.execute("SELECT * FROM users")
        self.assertEqual(len(result.rows), 2)
    
    def test_select_columns(self):
        """Test SELECT with specific columns"""
        self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        
        result = self.db.execute("SELECT name, age FROM users")
        self.assertEqual(result.columns, ['name', 'age'])
        self.assertEqual(result.rows[0]['name'], 'Alice')
        self.assertEqual(result.rows[0]['age'], 30)
    
    def test_select_with_alias(self):
        """Test SELECT with column aliases"""
        self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        
        result = self.db.execute("SELECT name AS user_name, age AS user_age FROM users")
        self.assertEqual(result.rows[0]['user_name'], 'Alice')
    
    def test_update(self):
        """Test UPDATE statement"""
        self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        
        result = self.db.execute("UPDATE users SET age = 31 WHERE id = 1")
        self.assertEqual(result.affected_rows, 1)
        
        select_result = self.db.execute("SELECT age FROM users WHERE id = 1")
        self.assertEqual(select_result.rows[0]['age'], 31)
    
    def test_update_multiple_columns(self):
        """Test UPDATE with multiple columns"""
        self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        
        self.db.execute("UPDATE users SET name = 'Alicia', age = 31 WHERE id = 1")
        
        result = self.db.execute("SELECT name, age FROM users WHERE id = 1")
        self.assertEqual(result.rows[0]['name'], 'Alicia')
        self.assertEqual(result.rows[0]['age'], 31)
    
    def test_delete(self):
        """Test DELETE statement"""
        self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        self.db.execute("INSERT INTO users VALUES (2, 'Bob', 25, FALSE)")
        
        result = self.db.execute("DELETE FROM users WHERE id = 1")
        self.assertEqual(result.affected_rows, 1)
        
        select_result = self.db.execute("SELECT * FROM users")
        self.assertEqual(len(select_result.rows), 1)
        self.assertEqual(select_result.rows[0]['name'], 'Bob')
    
    def test_delete_all(self):
        """Test DELETE without WHERE (delete all)"""
        self.db.execute("INSERT INTO users VALUES (1, 'Alice', 30, TRUE)")
        self.db.execute("INSERT INTO users VALUES (2, 'Bob', 25, FALSE)")
        
        result = self.db.execute("DELETE FROM users")
        self.assertEqual(result.affected_rows, 2)


class TestWhereClause(unittest.TestCase):
    """Test WHERE clause with various operators"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
        self.db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(100), price FLOAT, category VARCHAR(50))")
        self.db.execute("INSERT INTO products VALUES (1, 'Apple', 1.50, 'Fruit')")
        self.db.execute("INSERT INTO products VALUES (2, 'Banana', 0.75, 'Fruit')")
        self.db.execute("INSERT INTO products VALUES (3, 'Milk', 3.00, 'Dairy')")
        self.db.execute("INSERT INTO products VALUES (4, 'Bread', 2.50, 'Bakery')")
        self.db.execute("INSERT INTO products VALUES (5, 'Cheese', 5.00, 'Dairy')")
    
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_equals(self):
        """Test = operator"""
        result = self.db.execute("SELECT * FROM products WHERE category = 'Fruit'")
        self.assertEqual(len(result.rows), 2)
    
    def test_not_equals(self):
        """Test != operator"""
        result = self.db.execute("SELECT * FROM products WHERE category != 'Fruit'")
        self.assertEqual(len(result.rows), 3)
    
    def test_greater_than(self):
        """Test > operator"""
        result = self.db.execute("SELECT * FROM products WHERE price > 2.00")
        self.assertEqual(len(result.rows), 3)
    
    def test_less_than(self):
        """Test < operator"""
        result = self.db.execute("SELECT * FROM products WHERE price < 2.00")
        self.assertEqual(len(result.rows), 2)
    
    def test_greater_equals(self):
        """Test >= operator"""
        result = self.db.execute("SELECT * FROM products WHERE price >= 2.50")
        self.assertEqual(len(result.rows), 3)
    
    def test_less_equals(self):
        """Test <= operator"""
        result = self.db.execute("SELECT * FROM products WHERE price <= 2.50")
        self.assertEqual(len(result.rows), 3)
    
    def test_and(self):
        """Test AND operator"""
        result = self.db.execute("SELECT * FROM products WHERE category = 'Dairy' AND price > 3.00")
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0]['name'], 'Cheese')
    
    def test_or(self):
        """Test OR operator"""
        result = self.db.execute("SELECT * FROM products WHERE category = 'Fruit' OR category = 'Dairy'")
        self.assertEqual(len(result.rows), 4)
    
    def test_like_percent(self):
        """Test LIKE with % wildcard"""
        result = self.db.execute("SELECT * FROM products WHERE name LIKE 'B%'")
        self.assertEqual(len(result.rows), 2)  # Banana, Bread
    
    def test_like_underscore(self):
        """Test LIKE with _ wildcard"""
        result = self.db.execute("SELECT * FROM products WHERE name LIKE 'Bre__'")
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0]['name'], 'Bread')
    
    def test_between(self):
        """Test BETWEEN operator"""
        result = self.db.execute("SELECT * FROM products WHERE price BETWEEN 1.00 AND 3.00")
        self.assertEqual(len(result.rows), 3)  # Apple 1.50, Milk 3.00, Bread 2.50
    
    def test_in(self):
        """Test IN operator"""
        result = self.db.execute("SELECT * FROM products WHERE category IN ('Fruit', 'Dairy')")
        self.assertEqual(len(result.rows), 4)


class TestOrderByLimitOffset(unittest.TestCase):
    """Test ORDER BY, LIMIT, and OFFSET"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
        self.db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name VARCHAR(50), value INTEGER)")
        for i in range(1, 11):
            self.db.execute(f"INSERT INTO items VALUES ({i}, 'Item {i}', {i * 10})")
    
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_order_by_asc(self):
        """Test ORDER BY ASC"""
        result = self.db.execute("SELECT * FROM items ORDER BY value ASC")
        self.assertEqual(result.rows[0]['value'], 10)
        self.assertEqual(result.rows[-1]['value'], 100)
    
    def test_order_by_desc(self):
        """Test ORDER BY DESC"""
        result = self.db.execute("SELECT * FROM items ORDER BY value DESC")
        self.assertEqual(result.rows[0]['value'], 100)
        self.assertEqual(result.rows[-1]['value'], 10)
    
    def test_limit(self):
        """Test LIMIT"""
        result = self.db.execute("SELECT * FROM items LIMIT 5")
        self.assertEqual(len(result.rows), 5)
    
    def test_offset(self):
        """Test LIMIT with OFFSET"""
        result = self.db.execute("SELECT * FROM items ORDER BY id ASC LIMIT 3 OFFSET 5")
        self.assertEqual(len(result.rows), 3)
        self.assertEqual(result.rows[0]['id'], 6)


class TestAggregations(unittest.TestCase):
    """Test aggregate functions"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
        self.db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product VARCHAR(50), quantity INTEGER, price FLOAT)")
        self.db.execute("INSERT INTO sales VALUES (1, 'Widget', 10, 5.00)")
        self.db.execute("INSERT INTO sales VALUES (2, 'Widget', 5, 5.00)")
        self.db.execute("INSERT INTO sales VALUES (3, 'Gadget', 3, 15.00)")
        self.db.execute("INSERT INTO sales VALUES (4, 'Gadget', 7, 15.00)")
    
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_count(self):
        """Test COUNT function"""
        result = self.db.execute("SELECT COUNT(*) as total FROM sales")
        self.assertEqual(result.rows[0]['total'], 4)
    
    def test_sum(self):
        """Test SUM function"""
        result = self.db.execute("SELECT SUM(quantity) as total_qty FROM sales")
        self.assertEqual(result.rows[0]['total_qty'], 25)
    
    def test_avg(self):
        """Test AVG function"""
        result = self.db.execute("SELECT AVG(quantity) as avg_qty FROM sales")
        self.assertAlmostEqual(result.rows[0]['avg_qty'], 6.25)
    
    def test_min(self):
        """Test MIN function"""
        result = self.db.execute("SELECT MIN(quantity) as min_qty FROM sales")
        self.assertEqual(result.rows[0]['min_qty'], 3)
    
    def test_max(self):
        """Test MAX function"""
        result = self.db.execute("SELECT MAX(quantity) as max_qty FROM sales")
        self.assertEqual(result.rows[0]['max_qty'], 10)
    
    def test_group_by(self):
        """Test GROUP BY with aggregate"""
        result = self.db.execute("""
            SELECT product, SUM(quantity) as total
            FROM sales
            GROUP BY product
            ORDER BY total DESC
        """)
        self.assertEqual(len(result.rows), 2)
        self.assertEqual(result.rows[0]['product'], 'Widget')
        self.assertEqual(result.rows[0]['total'], 15)


class TestJoins(unittest.TestCase):
    """Test JOIN operations"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
        
        # Create tables
        self.db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name VARCHAR(50))")
        self.db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, name VARCHAR(50), dept_id INTEGER)")
        
        # Insert data
        self.db.execute("INSERT INTO departments VALUES (1, 'Engineering')")
        self.db.execute("INSERT INTO departments VALUES (2, 'Marketing')")
        self.db.execute("INSERT INTO departments VALUES (3, 'HR')")
        
        self.db.execute("INSERT INTO employees VALUES (1, 'Alice', 1)")
        self.db.execute("INSERT INTO employees VALUES (2, 'Bob', 1)")
        self.db.execute("INSERT INTO employees VALUES (3, 'Charlie', 2)")
        self.db.execute("INSERT INTO employees VALUES (4, 'Diana', NULL)")
    
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_inner_join(self):
        """Test INNER JOIN"""
        result = self.db.execute("""
            SELECT e.name, d.name as dept
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
        """)
        self.assertEqual(len(result.rows), 3)  # Diana excluded (NULL dept_id)
    
    def test_left_join(self):
        """Test LEFT JOIN"""
        result = self.db.execute("""
            SELECT e.name, d.name as dept
            FROM employees e
            LEFT JOIN departments d ON e.dept_id = d.id
        """)
        self.assertEqual(len(result.rows), 4)  # Diana included with NULL dept
    
    def test_join_with_where(self):
        """Test JOIN with WHERE clause"""
        result = self.db.execute("""
            SELECT e.name
            FROM employees e
            JOIN departments d ON e.dept_id = d.id
            WHERE d.name = 'Engineering'
        """)
        self.assertEqual(len(result.rows), 2)
        names = [row['name'] for row in result.rows]
        self.assertIn('Alice', names)
        self.assertIn('Bob', names)
    
    def test_join_with_aggregation(self):
        """Test JOIN with GROUP BY and COUNT"""
        result = self.db.execute("""
            SELECT d.name, COUNT(e.id) as emp_count
            FROM departments d
            LEFT JOIN employees e ON d.id = e.dept_id
            GROUP BY d.name
            ORDER BY emp_count DESC
        """)
        self.assertEqual(len(result.rows), 3)
        self.assertEqual(result.rows[0]['name'], 'Engineering')
        self.assertEqual(result.rows[0]['emp_count'], 2)


class TestIndexing(unittest.TestCase):
    """Test B-tree index operations"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
        self.db.execute("CREATE TABLE indexed_table (id INTEGER PRIMARY KEY, email VARCHAR(255), name VARCHAR(100))")
    
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_create_index(self):
        """Test CREATE INDEX"""
        result = self.db.execute("CREATE INDEX idx_email ON indexed_table(email)")
        self.assertIn("created", result.message.lower())
    
    def test_unique_index(self):
        """Test CREATE UNIQUE INDEX"""
        result = self.db.execute("CREATE UNIQUE INDEX idx_email ON indexed_table(email)")
        self.assertIn("created", result.message.lower())
    
    def test_primary_key_index(self):
        """Test that PRIMARY KEY automatically creates an index"""
        # Insert some data
        self.db.execute("INSERT INTO indexed_table VALUES (1, 'alice@test.com', 'Alice')")
        self.db.execute("INSERT INTO indexed_table VALUES (2, 'bob@test.com', 'Bob')")
        
        # Query should use index (internally)
        result = self.db.execute("SELECT * FROM indexed_table WHERE id = 1")
        self.assertEqual(len(result.rows), 1)
        self.assertEqual(result.rows[0]['name'], 'Alice')
    
    def test_unique_constraint_violation(self):
        """Test that UNIQUE constraint prevents duplicates"""
        self.db.execute("CREATE TABLE unique_test (id INTEGER PRIMARY KEY, email VARCHAR(255) UNIQUE)")
        self.db.execute("INSERT INTO unique_test VALUES (1, 'test@test.com')")
        
        # Should raise error on duplicate email
        with self.assertRaises(ValueError):
            self.db.execute("INSERT INTO unique_test VALUES (2, 'test@test.com')")


class TestDataTypes(unittest.TestCase):
    """Test various data types"""
    
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.db = Database(self.test_dir)
    
    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.test_dir, ignore_errors=True)
    
    def test_integer(self):
        """Test INTEGER type"""
        self.db.execute("CREATE TABLE int_test (id INTEGER PRIMARY KEY, value INTEGER)")
        self.db.execute("INSERT INTO int_test VALUES (1, 42)")
        
        result = self.db.execute("SELECT value FROM int_test")
        self.assertEqual(result.rows[0]['value'], 42)
        self.assertIsInstance(result.rows[0]['value'], int)
    
    def test_float(self):
        """Test FLOAT type"""
        self.db.execute("CREATE TABLE float_test (id INTEGER PRIMARY KEY, value FLOAT)")
        self.db.execute("INSERT INTO float_test VALUES (1, 3.14159)")
        
        result = self.db.execute("SELECT value FROM float_test")
        self.assertAlmostEqual(result.rows[0]['value'], 3.14159)
    
    def test_varchar(self):
        """Test VARCHAR type with size limit"""
        self.db.execute("CREATE TABLE varchar_test (id INTEGER PRIMARY KEY, name VARCHAR(10))")
        self.db.execute("INSERT INTO varchar_test VALUES (1, 'Short')")
        
        result = self.db.execute("SELECT name FROM varchar_test")
        self.assertEqual(result.rows[0]['name'], 'Short')
        
        # Test size limit violation
        with self.assertRaises(ValueError):
            self.db.execute("INSERT INTO varchar_test VALUES (2, 'This is way too long')")
    
    def test_boolean(self):
        """Test BOOLEAN type"""
        self.db.execute("CREATE TABLE bool_test (id INTEGER PRIMARY KEY, flag BOOLEAN)")
        self.db.execute("INSERT INTO bool_test VALUES (1, TRUE)")
        self.db.execute("INSERT INTO bool_test VALUES (2, FALSE)")
        
        result = self.db.execute("SELECT * FROM bool_test ORDER BY id")
        self.assertTrue(result.rows[0]['flag'])
        self.assertFalse(result.rows[1]['flag'])
    
    def test_date(self):
        """Test DATE type"""
        self.db.execute("CREATE TABLE date_test (id INTEGER PRIMARY KEY, created DATE)")
        self.db.execute("INSERT INTO date_test VALUES (1, '2024-01-15')")
        
        result = self.db.execute("SELECT created FROM date_test")
        self.assertEqual(result.rows[0]['created'], date(2024, 1, 15))


def run_tests():
    """Run all tests"""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestDatabase))
    suite.addTests(loader.loadTestsFromTestCase(TestCRUD))
    suite.addTests(loader.loadTestsFromTestCase(TestWhereClause))
    suite.addTests(loader.loadTestsFromTestCase(TestOrderByLimitOffset))
    suite.addTests(loader.loadTestsFromTestCase(TestAggregations))
    suite.addTests(loader.loadTestsFromTestCase(TestJoins))
    suite.addTests(loader.loadTestsFromTestCase(TestIndexing))
    suite.addTests(loader.loadTestsFromTestCase(TestDataTypes))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    sys.exit(run_tests())
