import pytest
from rdbms.parser import SQLParser, ParsedCommand


class TestSQLParser:
    """Test cases for SQL Parser"""

    @pytest.fixture
    def parser(self):
        """Create a parser instance"""
        return SQLParser()

    # CREATE TABLE tests
    def test_parse_create_table_simple(self, parser):
        """Test parsing simple CREATE TABLE"""
        sql = "CREATE TABLE users (id INT, name TEXT)"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'CREATE_TABLE'
        assert cmd.table_name == 'users'
        assert cmd.columns == ['id', 'name']
        assert cmd.types == ['INT', 'TEXT']

    def test_parse_create_table_multiple_columns(self, parser):
        """Test CREATE TABLE with multiple columns"""
        sql = "CREATE TABLE products (id INT, name TEXT, price FLOAT, in_stock BOOL)"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'CREATE_TABLE'
        assert cmd.table_name == 'products'
        assert cmd.columns == ['id', 'name', 'price', 'in_stock']
        assert cmd.types == ['INT', 'TEXT', 'FLOAT', 'BOOL']

    def test_parse_create_table_with_primary_key(self, parser):
        """Test CREATE TABLE with PRIMARY KEY"""
        sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'CREATE_TABLE'
        assert cmd.primary_key == 'id'

    def test_parse_create_table_with_unique(self, parser):
        """Test CREATE TABLE with UNIQUE constraint"""
        sql = "CREATE TABLE users (id INT, email TEXT UNIQUE, name TEXT)"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'CREATE_TABLE'
        assert 'email' in cmd.unique_constraints

    # INSERT tests
    def test_parse_insert_simple(self, parser):
        """Test parsing simple INSERT"""
        sql = "INSERT INTO users VALUES (1, 'Alice')"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'INSERT'
        assert cmd.table_name == 'users'
        assert cmd.values == [1, 'Alice']

    def test_parse_insert_multiple_types(self, parser):
        """Test INSERT with different data types"""
        sql = "INSERT INTO products VALUES (1, 'Widget', 9.99, true)"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'INSERT'
        assert cmd.values == [1, 'Widget', 9.99, True]

    def test_parse_insert_with_boolean_false(self, parser):
        """Test INSERT with false boolean"""
        sql = "INSERT INTO users VALUES (1, 'Alice', false)"
        cmd = parser.parse(sql)

        assert cmd.values == [1, 'Alice', False]

    def test_parse_insert_with_negative_numbers(self, parser):
        """Test INSERT with negative numbers"""
        sql = "INSERT INTO data VALUES (-10, -99.99)"
        cmd = parser.parse(sql)

        assert cmd.values == [-10, -99.99]

    # SELECT tests
    def test_parse_select_all(self, parser):
        """Test parsing SELECT *"""
        sql = "SELECT * FROM users"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'SELECT'
        assert cmd.table_name == 'users'
        assert cmd.columns is None  # * means all columns
        assert cmd.where is None

    def test_parse_select_specific_columns(self, parser):
        """Test SELECT with specific columns"""
        sql = "SELECT name, age FROM users"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'SELECT'
        assert cmd.columns == ['name', 'age']

    def test_parse_select_with_where(self, parser):
        """Test SELECT with WHERE clause"""
        sql = "SELECT * FROM users WHERE id = 1"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'SELECT'
        assert cmd.where == {'id': 1}

    def test_parse_select_where_text(self, parser):
        """Test SELECT WHERE with text value"""
        sql = "SELECT * FROM users WHERE name = 'Alice'"
        cmd = parser.parse(sql)

        assert cmd.where == {'name': 'Alice'}

    def test_parse_select_where_boolean(self, parser):
        """Test SELECT WHERE with boolean value"""
        sql = "SELECT * FROM users WHERE active = true"
        cmd = parser.parse(sql)

        assert cmd.where == {'active': True}

    def test_parse_select_columns_with_where(self, parser):
        """Test SELECT specific columns with WHERE"""
        sql = "SELECT name, age FROM users WHERE active = true"
        cmd = parser.parse(sql)

        assert cmd.columns == ['name', 'age']
        assert cmd.where == {'active': True}

    # UPDATE tests
    def test_parse_update_simple(self, parser):
        """Test parsing simple UPDATE"""
        sql = "UPDATE users SET age = 31 WHERE id = 1"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'UPDATE'
        assert cmd.table_name == 'users'
        assert cmd.set_values == {'age': 31}
        assert cmd.where == {'id': 1}

    def test_parse_update_multiple_columns(self, parser):
        """Test UPDATE with multiple columns"""
        sql = "UPDATE users SET age = 31, name = 'Alice Smith' WHERE id = 1"
        cmd = parser.parse(sql)

        assert cmd.set_values == {'age': 31, 'name': 'Alice Smith'}

    def test_parse_update_without_where(self, parser):
        """Test UPDATE without WHERE (updates all)"""
        sql = "UPDATE users SET active = false"
        cmd = parser.parse(sql)

        assert cmd.set_values == {'active': False}
        assert cmd.where is None

    # DELETE tests
    def test_parse_delete_with_where(self, parser):
        """Test parsing DELETE with WHERE"""
        sql = "DELETE FROM users WHERE id = 1"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'DELETE'
        assert cmd.table_name == 'users'
        assert cmd.where == {'id': 1}

    def test_parse_delete_without_where(self, parser):
        """Test DELETE without WHERE (deletes all)"""
        sql = "DELETE FROM users"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'DELETE'
        assert cmd.where is None

    # JOIN tests
    def test_parse_join_simple(self, parser):
        """Test parsing simple JOIN"""
        sql = "SELECT * FROM users JOIN cities ON users.city_id = cities.id"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'JOIN'
        assert cmd.table_name == 'users'
        assert cmd.join_table == 'cities'
        assert cmd.left_column == 'city_id'
        assert cmd.right_column == 'id'

    def test_parse_join_with_inner(self, parser):
        """Test parsing INNER JOIN"""
        sql = "SELECT * FROM users INNER JOIN cities ON users.city_id = cities.id"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'JOIN'
        assert cmd.join_table == 'cities'

    # Edge cases and validation
    def test_parse_case_insensitive(self, parser):
        """Test that keywords are case-insensitive"""
        sql = "select * from users"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'SELECT'

    def test_parse_extra_whitespace(self, parser):
        """Test parsing with extra whitespace"""
        sql = "SELECT   *   FROM    users   WHERE   id = 1"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'SELECT'
        assert cmd.where == {'id': 1}

    def test_parse_invalid_command(self, parser):
        """Test parsing invalid SQL command"""
        with pytest.raises(ValueError, match="Invalid SQL command"):
            parser.parse("INVALID COMMAND")

    def test_parse_empty_string(self, parser):
        """Test parsing empty string"""
        with pytest.raises(ValueError, match="Empty SQL command"):
            parser.parse("")

    def test_parse_whitespace_only(self, parser):
        """Test parsing whitespace-only string"""
        with pytest.raises(ValueError, match="Empty SQL command"):
            parser.parse("   ")

    # DROP TABLE test
    def test_parse_drop_table(self, parser):
        """Test parsing DROP TABLE"""
        sql = "DROP TABLE users"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'DROP_TABLE'
        assert cmd.table_name == 'users'

    # CREATE INDEX test
    def test_parse_create_index(self, parser):
        """Test parsing CREATE INDEX"""
        sql = "CREATE INDEX ON users (age)"
        cmd = parser.parse(sql)

        assert cmd.command_type == 'CREATE_INDEX'
        assert cmd.table_name == 'users'
        assert cmd.column_name == 'age'

    def test_parse_insert_with_empty_string(self, parser):
        """Test INSERT with empty string value"""
        sql = "INSERT INTO users VALUES (1, '')"
        cmd = parser.parse(sql)

        assert cmd.values == [1, '']