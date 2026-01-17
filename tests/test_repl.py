"""Tests for REPL module."""

import pytest
import tempfile
import shutil

from rdbms.repl import Database


class TestDatabase:
    """Test Database class."""

    @pytest.fixture
    def temp_dir(self):
        """Create temp directory."""
        temp = tempfile.mkdtemp()
        yield temp
        shutil.rmtree(temp)

    @pytest.fixture
    def db(self, temp_dir):
        """Create database instance."""
        return Database(temp_dir)

    def test_create_table(self, db):
        """Test CREATE TABLE."""
        result = db.execute("CREATE TABLE users (id INT, name TEXT)")
        assert result == "Table 'users' created"
        assert 'users' in db.tables

    def test_insert(self, db):
        """Test INSERT."""
        db.execute("CREATE TABLE users (id INT, name TEXT)")
        result = db.execute("INSERT INTO users VALUES (1, 'Alice')")
        assert result == "1 row inserted"
        assert len(db.tables['users'].rows) == 1

    def test_select(self, db):
        """Test SELECT."""
        db.execute("CREATE TABLE users (id INT, name TEXT)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")

        result = db.execute("SELECT * FROM users")
        assert len(result) == 2
        assert result[0] == [1, 'Alice']
        assert result[1] == [2, 'Bob']

    def test_select_with_where(self, db):
        """Test SELECT with WHERE."""
        db.execute("CREATE TABLE users (id INT, name TEXT)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")

        result = db.execute("SELECT * FROM users WHERE name = 'Alice'")
        assert len(result) == 1
        assert result[0] == [1, 'Alice']

    def test_update(self, db):
        """Test UPDATE."""
        db.execute("CREATE TABLE users (id INT, name TEXT, age INT)")
        db.execute("INSERT INTO users VALUES (1, 'Alice', 30)")

        result = db.execute("UPDATE users SET age = 31 WHERE name = 'Alice'")
        assert result == "1 row(s) updated"

        rows = db.execute("SELECT age FROM users WHERE name = 'Alice'")
        assert rows[0][0] == 31

    def test_delete(self, db):
        """Test DELETE."""
        db.execute("CREATE TABLE users (id INT, name TEXT)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")
        db.execute("INSERT INTO users VALUES (2, 'Bob')")

        result = db.execute("DELETE FROM users WHERE name = 'Alice'")
        assert result == "1 row(s) deleted"

        rows = db.execute("SELECT * FROM users")
        assert len(rows) == 1

    def test_join(self, db):
        """Test JOIN."""
        db.execute("CREATE TABLE users (id INT, name TEXT, city_id INT)")
        db.execute("CREATE TABLE cities (id INT, name TEXT)")

        db.execute("INSERT INTO users VALUES (1, 'Alice', 1)")
        db.execute("INSERT INTO cities VALUES (1, 'NYC')")

        result = db.execute("SELECT * FROM users JOIN cities ON users.city_id = cities.id")
        assert len(result) == 1
        assert result[0] == [1, 'Alice', 1, 1, 'NYC']

    def test_create_index(self, db):
        """Test CREATE INDEX."""
        db.execute("CREATE TABLE users (id INT, name TEXT)")
        result = db.execute("CREATE INDEX ON users (name)")
        assert result == "Index created on 'name'"
        assert 'name' in db.tables['users'].indexes

    def test_drop_table(self, db):
        """Test DROP TABLE."""
        db.execute("CREATE TABLE users (id INT, name TEXT)")
        result = db.execute("DROP TABLE users")
        assert result == "Table 'users' dropped"
        assert 'users' not in db.tables

    def test_primary_key_constraint(self, db):
        """Test PRIMARY KEY enforcement."""
        db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO users VALUES (1, 'Alice')")

        with pytest.raises(ValueError, match="Duplicate primary key"):
            db.execute("INSERT INTO users VALUES (1, 'Bob')")

    def test_unique_constraint(self, db):
        """Test UNIQUE constraint."""
        db.execute("CREATE TABLE users (id INT, email TEXT UNIQUE)")
        db.execute("INSERT INTO users VALUES (1, 'alice@test.com')")

        with pytest.raises(ValueError, match="Duplicate value for unique column"):
            db.execute("INSERT INTO users VALUES (2, 'alice@test.com')")

    def test_persistence(self, temp_dir):
        """Test data persists across Database instances."""
        db1 = Database(temp_dir)
        db1.execute("CREATE TABLE users (id INT, name TEXT)")
        db1.execute("INSERT INTO users VALUES (1, 'Alice')")

        db2 = Database(temp_dir)
        assert 'users' in db2.tables
        result = db2.execute("SELECT * FROM users")
        assert len(result) == 1
        assert result[0] == [1, 'Alice']