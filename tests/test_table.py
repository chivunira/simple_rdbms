import pytest
from rdbms.table import Table, DataType


class TestTableCreation:
    """Test cases for table creation and schema definition"""

    def test_create_simple_table(self):
        """Test creating a table with basic columns"""
        table = Table('users', ['id', 'name', 'email'], ['INT', 'TEXT', 'TEXT'])

        assert table.name == 'users'
        assert table.columns == ['id', 'name', 'email']
        assert table.types == ['INT', 'TEXT', 'TEXT']
        assert len(table.rows) == 0

    def test_create_table_with_mixed_types(self):
        """Test creating a table with different data types"""
        table = Table(
            'products',
            ['id', 'name', 'price', 'in_stock'],
            ['INT', 'TEXT', 'FLOAT', 'BOOL']
        )

        assert table.name == 'products'
        assert table.columns == ['id', 'name', 'price', 'in_stock']
        assert table.types == ['INT', 'TEXT', 'FLOAT', 'BOOL']

    def test_mismatched_columns_and_types(self):
        """Test that creating a table with mismatched columns and types raises error"""
        with pytest.raises(ValueError, match="Number of columns must match number of types"):
            Table('invalid', ['id', 'name'], ['INT'])

    def test_invalid_column_type(self):
        """Test that invalid column type raises error"""
        with pytest.raises(ValueError, match="Invalid data type"):
            Table('invalid', ['id', 'name'], ['INT', 'INVALID_TYPE'])

    def test_duplicate_column_names(self):
        """Test that duplicate column names raise error"""
        with pytest.raises(ValueError, match="Duplicate column name"):
            Table('invalid', ['id', 'name', 'id'], ['INT', 'TEXT', 'INT'])

    def test_empty_table_name(self):
        """Test that empty table name raises error"""
        with pytest.raises(ValueError, match="Table name cannot be empty"):
            Table('', ['id'], ['INT'])

    def test_no_columns(self):
        """Test that table with no columns raises error"""
        with pytest.raises(ValueError, match="Table must have at least one column"):
            Table('empty', [], [])


class TestDataTypeValidation:
    """Test cases for data type validation"""

    def test_validate_int(self):
        """Test INT type validation"""
        table = Table('test', ['id'], ['INT'])

        assert table.validate_value(0, 'INT') is True
        assert table.validate_value(42, 'INT') is True
        assert table.validate_value(-100, 'INT') is True
        assert table.validate_value('123', 'INT') is False
        assert table.validate_value(3.14, 'INT') is False
        assert table.validate_value(True, 'INT') is False

    def test_validate_text(self):
        """Test TEXT type validation"""
        table = Table('test', ['name'], ['TEXT'])

        assert table.validate_value('hello', 'TEXT') is True
        assert table.validate_value('', 'TEXT') is True
        assert table.validate_value('123', 'TEXT') is True
        assert table.validate_value(123, 'TEXT') is False
        assert table.validate_value(None, 'TEXT') is False

    def test_validate_float(self):
        """Test FLOAT type validation"""
        table = Table('test', ['price'], ['FLOAT'])

        assert table.validate_value(3.14, 'FLOAT') is True
        assert table.validate_value(0.0, 'FLOAT') is True
        assert table.validate_value(-99.99, 'FLOAT') is True
        assert table.validate_value(42, 'FLOAT') is False  # int is not float
        assert table.validate_value('3.14', 'FLOAT') is False

    def test_validate_bool(self):
        """Test BOOL type validation"""
        table = Table('test', ['active'], ['BOOL'])

        assert table.validate_value(True, 'BOOL') is True
        assert table.validate_value(False, 'BOOL') is True
        assert table.validate_value(1, 'BOOL') is False  # int is not bool
        assert table.validate_value('true', 'BOOL') is False
        assert table.validate_value(None, 'BOOL') is False

    def test_validate_row(self):
        """Test validating an entire row"""
        table = Table('users', ['id', 'name', 'age', 'active'], ['INT', 'TEXT', 'INT', 'BOOL'])

        # Valid row
        assert table.validate_row([1, 'Alice', 25, True]) is True

        # Invalid - wrong type for age
        assert table.validate_row([1, 'Alice', '25', True]) is False

        # Invalid - wrong type for active
        assert table.validate_row([1, 'Alice', 25, 'yes']) is False

        # Invalid - wrong number of values
        assert table.validate_row([1, 'Alice']) is False
        assert table.validate_row([1, 'Alice', 25, True, 'extra']) is False


class TestTableState:
    """Test table state methods"""

    def test_to_dict(self):
        """Test converting table to dictionary for storage"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        # Manually add some rows for testing
        table.rows = [[1, 'Alice'], [2, 'Bob'], [3, 'Chivu']]

        data = table.to_dict()

        assert data['name'] == 'users'
        assert data['columns'] == ['id', 'name']
        assert data['types'] == ['INT', 'TEXT']
        assert data['rows'] == [[1, 'Alice'], [2, 'Bob'], [3, 'Chivu']]
        assert 'primary_key' in data
        assert 'unique_constraints' in data
        assert 'indexes' in data

    def test_from_dict(self):
        """Test creating table from dictionary"""
        data = {
            'name': 'products',
            'columns': ['id', 'name', 'price'],
            'types': ['INT', 'TEXT', 'FLOAT'],
            'rows': [[1, 'Widget', 9.99], [2, 'Gadget', 19.99]],
            'primary_key': None,
            'unique_constraints': [],
            'indexes': {}
        }

        table = Table.from_dict(data)

        assert table.name == 'products'
        assert table.columns == ['id', 'name', 'price']
        assert table.types == ['INT', 'TEXT', 'FLOAT']
        assert table.rows == [[1, 'Widget', 9.99], [2, 'Gadget', 19.99]]

    def test_get_column_index(self):
        """Test getting column index by name"""
        table = Table('users', ['id', 'name', 'email'], ['INT', 'TEXT', 'TEXT'])

        assert table.get_column_index('id') == 0
        assert table.get_column_index('name') == 1
        assert table.get_column_index('email') == 2

        with pytest.raises(ValueError, match="Column 'age' does not exist"):
            table.get_column_index('age')