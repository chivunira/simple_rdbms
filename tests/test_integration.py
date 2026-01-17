"""
Integration tests for the RDBMS.
Tests complete workflows across multiple components.
"""

import pytest
import tempfile
import shutil

from rdbms.repl import Database


class TestEndToEndWorkflows:
    """Test complete end-to-end workflows."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield temp
        shutil.rmtree(temp)

    def test_complete_user_management_workflow(self, temp_dir):
        """Test a complete user management workflow."""
        db = Database(temp_dir)

        # 1. Create users table
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE, age INT)")
        assert 'users' in db.tables

        # 2. Insert multiple users
        db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@test.com', 30)")
        db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@test.com', 25)")
        db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@test.com', 35)")

        # 3. Query all users
        results = db.execute("SELECT * FROM users")
        assert len(results) == 3

        # 4. Update a user
        db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
        results = db.execute("SELECT age FROM users WHERE name = 'Alice'")
        assert results[0][0] == 31

        # 5. Delete a user
        db.execute("DELETE FROM users WHERE name = 'Bob'")
        results = db.execute("SELECT * FROM users")
        assert len(results) == 2

        # 6. Create index for performance
        db.execute("CREATE INDEX ON users (email)")
        assert 'email' in db.tables['users'].indexes

        # 7. Verify persistence
        db2 = Database(temp_dir)
        assert 'users' in db2.tables
        results = db2.execute("SELECT * FROM users")
        assert len(results) == 2

    def test_multi_table_with_join_workflow(self, temp_dir):
        """Test workflow with multiple related tables and JOIN."""
        db = Database(temp_dir)

        # 1. Create cities table
        db.execute("CREATE TABLE cities (id INT PRIMARY KEY, name TEXT, country TEXT)")
        db.execute("INSERT INTO cities VALUES (1, 'New York', 'USA')")
        db.execute("INSERT INTO cities VALUES (2, 'London', 'UK')")
        db.execute("INSERT INTO cities VALUES (3, 'Tokyo', 'Japan')")

        # 2. Create employees table
        db.execute("CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, city_id INT, salary FLOAT)")
        db.execute("INSERT INTO employees VALUES (1, 'Alice', 1, 75000.0)")
        db.execute("INSERT INTO employees VALUES (2, 'Bob', 2, 65000.0)")
        db.execute("INSERT INTO employees VALUES (3, 'Charlie', 1, 80000.0)")
        db.execute("INSERT INTO employees VALUES (4, 'Diana', 3, 70000.0)")

        # 3. Query with JOIN
        results = db.execute("SELECT * FROM employees JOIN cities ON employees.city_id = cities.id")
        assert len(results) == 4

        # Verify JOIN results contain data from both tables
        assert len(results[0]) == 7  # employees (4 cols) + cities (3 cols)

        # 4. Update employee salary
        db.execute("UPDATE employees SET salary = 82000.0 WHERE name = 'Charlie'")

        # 5. Filter employees by city
        results = db.execute("SELECT * FROM employees WHERE city_id = 1")
        assert len(results) == 2  # Alice and Charlie in New York

        # 6. Verify persistence of both tables
        db2 = Database(temp_dir)
        assert 'cities' in db2.tables
        assert 'employees' in db2.tables

    def test_constraint_enforcement_workflow(self, temp_dir):
        """Test that constraints are enforced throughout workflow."""
        db = Database(temp_dir)

        # 1. Create table with constraints
        db.execute("CREATE TABLE products (id INT PRIMARY KEY, sku TEXT UNIQUE, name TEXT, price FLOAT)")

        # 2. Insert valid products
        db.execute("INSERT INTO products VALUES (1, 'SKU001', 'Laptop', 999.99)")
        db.execute("INSERT INTO products VALUES (2, 'SKU002', 'Mouse', 29.99)")

        # 3. Try to violate PRIMARY KEY constraint
        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.execute("INSERT INTO products VALUES (1, 'SKU003', 'Keyboard', 79.99)")

        # 4. Try to violate UNIQUE constraint
        with pytest.raises(ValueError, match="Duplicate value for unique column"):
            db.execute("INSERT INTO products VALUES (3, 'SKU001', 'Monitor', 299.99)")

        # 5. Verify only valid inserts succeeded
        results = db.execute("SELECT * FROM products")
        assert len(results) == 2

        # 6. Update with constraint check
        db.execute("UPDATE products SET price = 899.99 WHERE id = 1")

        # Try to update to duplicate primary key (should fail)
        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.execute("UPDATE products SET id = 2 WHERE id = 1")

    def test_data_persistence_across_restarts(self, temp_dir):
        """Test that data persists correctly across database restarts."""
        # Session 1: Create and populate database
        db1 = Database(temp_dir)
        db1.execute("CREATE TABLE inventory (id INT PRIMARY KEY, item TEXT, quantity INT)")
        db1.execute("INSERT INTO inventory VALUES (1, 'Apples', 100)")
        db1.execute("INSERT INTO inventory VALUES (2, 'Bananas', 150)")
        db1.execute("INSERT INTO inventory VALUES (3, 'Oranges', 80)")

        # Verify data exists
        results = db1.execute("SELECT * FROM inventory")
        assert len(results) == 3

        # Session 2: Restart and modify
        db2 = Database(temp_dir)
        assert 'inventory' in db2.tables

        results = db2.execute("SELECT * FROM inventory")
        assert len(results) == 3

        # Modify data
        db2.execute("UPDATE inventory SET quantity = 120 WHERE item = 'Apples'")
        db2.execute("DELETE FROM inventory WHERE item = 'Bananas'")

        # Session 3: Verify modifications persisted
        db3 = Database(temp_dir)
        results = db3.execute("SELECT * FROM inventory")
        assert len(results) == 2

        # Verify updated quantity
        results = db3.execute("SELECT quantity FROM inventory WHERE item = 'Apples'")
        assert results[0][0] == 120

        # Verify banana was deleted
        results = db3.execute("SELECT * FROM inventory WHERE item = 'Bananas'")
        assert len(results) == 0

    def test_complex_query_workflow(self, temp_dir):
        """Test complex querying scenarios."""
        db = Database(temp_dir)

        # Setup: Create and populate a table
        db.execute("CREATE TABLE sales (id INT PRIMARY KEY, product TEXT, amount FLOAT, region TEXT)")
        db.execute("INSERT INTO sales VALUES (1, 'Laptop', 999.99, 'North')")
        db.execute("INSERT INTO sales VALUES (2, 'Mouse', 29.99, 'South')")
        db.execute("INSERT INTO sales VALUES (3, 'Keyboard', 79.99, 'North')")
        db.execute("INSERT INTO sales VALUES (4, 'Monitor', 299.99, 'East')")
        db.execute("INSERT INTO sales VALUES (5, 'Laptop', 999.99, 'West')")

        # Test: Select specific columns
        results = db.execute("SELECT product, amount FROM sales")
        assert len(results) == 5
        assert len(results[0]) == 2

        # Test: Filter by region
        results = db.execute("SELECT * FROM sales WHERE region = 'North'")
        assert len(results) == 2

        # Test: Create index and query
        db.execute("CREATE INDEX ON sales (region)")
        results = db.execute("SELECT * FROM sales WHERE region = 'South'")
        assert len(results) == 1
        assert results[0][1] == 'Mouse'

    def test_table_lifecycle_workflow(self, temp_dir):
        """Test complete table lifecycle: create, use, drop."""
        db = Database(temp_dir)

        # 1. Create table
        db.execute("CREATE TABLE temp_data (id INT, value TEXT)")
        assert 'temp_data' in db.tables

        # 2. Use table
        db.execute("INSERT INTO temp_data VALUES (1, 'test')")
        results = db.execute("SELECT * FROM temp_data")
        assert len(results) == 1

        # 3. Drop table
        db.execute("DROP TABLE temp_data")
        assert 'temp_data' not in db.tables

        # 4. Verify table is gone from storage
        assert not db.storage.table_exists('temp_data')

        # 5. Create new table with same name (should work)
        db.execute("CREATE TABLE temp_data (id INT, name TEXT, age INT)")
        assert 'temp_data' in db.tables
        assert len(db.tables['temp_data'].columns) == 3

    def test_error_recovery_workflow(self, temp_dir):
        """Test that system recovers properly from errors."""
        db = Database(temp_dir)

        # 1. Create table
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")

        # 2. Insert valid data
        db.execute("INSERT INTO users VALUES (1, 'Alice')")

        # 3. Try invalid insert (should fail but not break system)
        with pytest.raises(ValueError):
            db.execute("INSERT INTO users VALUES (1, 'Bob')")  # Duplicate primary key

        # 4. Verify system still works
        results = db.execute("SELECT * FROM users")
        assert len(results) == 1

        # 5. Continue with valid operations
        db.execute("INSERT INTO users VALUES (2, 'Charlie')")
        results = db.execute("SELECT * FROM users")
        assert len(results) == 2

        # 6. Try invalid update (should fail but not break system)
        with pytest.raises(ValueError):
            db.execute("UPDATE users SET id = 1 WHERE id = 2")  # Duplicate primary key

        # 7. Verify system still works
        db.execute("UPDATE users SET name = 'Bob' WHERE id = 2")
        results = db.execute("SELECT name FROM users WHERE id = 2")
        assert results[0][0] == 'Bob'

    def test_concurrent_operations_workflow(self, temp_dir):
        """Test multiple operations in sequence."""
        db = Database(temp_dir)

        # Create multiple tables
        db.execute("CREATE TABLE table1 (id INT, data TEXT)")
        db.execute("CREATE TABLE table2 (id INT, data TEXT)")
        db.execute("CREATE TABLE table3 (id INT, data TEXT)")

        # Insert into all tables
        for i in range(1, 4):
            db.execute(f"INSERT INTO table1 VALUES ({i}, 'data{i}')")
            db.execute(f"INSERT INTO table2 VALUES ({i}, 'data{i}')")
            db.execute(f"INSERT INTO table3 VALUES ({i}, 'data{i}')")

        # Verify all tables have data
        for table in ['table1', 'table2', 'table3']:
            results = db.execute(f"SELECT * FROM {table}")
            assert len(results) == 3

        # Update in multiple tables
        db.execute("UPDATE table1 SET data = 'updated' WHERE id = 1")
        db.execute("UPDATE table2 SET data = 'updated' WHERE id = 2")
        db.execute("UPDATE table3 SET data = 'updated' WHERE id = 3")

        # Delete from all tables
        db.execute("DELETE FROM table1 WHERE id = 3")
        db.execute("DELETE FROM table2 WHERE id = 3")
        db.execute("DELETE FROM table3 WHERE id = 3")

        # Verify final state
        for table in ['table1', 'table2', 'table3']:
            results = db.execute(f"SELECT * FROM {table}")
            assert len(results) == 2

    def test_indexing_integration(self, temp_dir):
        """Test that indexing integrates properly with all operations."""
        db = Database(temp_dir)

        # Create table
        db.execute("CREATE TABLE products (id INT PRIMARY KEY, category TEXT, price FLOAT)")

        # Insert data
        for i in range(1, 11):
            db.execute(f"INSERT INTO products VALUES ({i}, 'Category{i % 3}', {i * 10.0})")

        # Create index
        db.execute("CREATE INDEX ON products (category)")

        # Verify index exists
        assert 'category' in db.tables['products'].indexes

        # Query using indexed column
        results = db.execute("SELECT * FROM products WHERE category = 'Category1'")
        assert len(results) > 0

        # Update should maintain index
        db.execute("UPDATE products SET price = 999.99 WHERE id = 1")

        # Delete should maintain index
        db.execute("DELETE FROM products WHERE id = 10")

        # Index should still exist and work
        assert 'category' in db.tables['products'].indexes
        results = db.execute("SELECT * FROM products WHERE category = 'Category0'")
        assert len(results) > 0

        # Verify persistence of index
        db2 = Database(temp_dir)
        assert 'category' in db2.tables['products'].indexes