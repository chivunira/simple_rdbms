import pytest
from rdbms.table import Table


class TestInsertOperations:
    """Test cases for INSERT operations"""

    def test_insert_single_row(self):
        """Test inserting a single valid row"""
        table = Table('users', ['id', 'name', 'email'], ['INT', 'TEXT', 'TEXT'])

        result = table.insert([1, 'Alice', 'alice@example.com'])

        assert result is True
        assert len(table.rows) == 1
        assert table.rows[0] == [1, 'Alice', 'alice@example.com']

    def test_insert_multiple_rows(self):
        """Test inserting multiple rows"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])
        table.insert([3, 'Charlie'])

        assert len(table.rows) == 3
        assert table.rows[0] == [1, 'Alice']
        assert table.rows[1] == [2, 'Bob']
        assert table.rows[2] == [3, 'Charlie']

    def test_insert_with_mixed_types(self):
        """Test inserting row with different data types"""
        table = Table(
            'products',
            ['id', 'name', 'price', 'in_stock'],
            ['INT', 'TEXT', 'FLOAT', 'BOOL']
        )

        result = table.insert([1, 'Widget', 9.99, True])

        assert result is True
        assert len(table.rows) == 1
        assert table.rows[0] == [1, 'Widget', 9.99, True]

    def test_insert_wrong_column_count(self):
        """Test that inserting row with wrong number of columns fails"""
        table = Table('users', ['id', 'name', 'email'], ['INT', 'TEXT', 'TEXT'])

        # Too few columns
        with pytest.raises(ValueError, match="Expected 3 values, got 2"):
            table.insert([1, 'Alice'])

        # Too many columns
        with pytest.raises(ValueError, match="Expected 3 values, got 4"):
            table.insert([1, 'Alice', 'alice@example.com', 'extra'])

        assert len(table.rows) == 0

    def test_insert_wrong_type_int(self):
        """Test that inserting wrong type for INT column fails"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        # String instead of int
        with pytest.raises(ValueError, match="Invalid type for column 'id'"):
            table.insert(['1', 'Alice'])

        # Float instead of int
        with pytest.raises(ValueError, match="Invalid type for column 'id'"):
            table.insert([1.5, 'Alice'])

        # Boolean instead of int
        with pytest.raises(ValueError, match="Invalid type for column 'id'"):
            table.insert([True, 'Alice'])

        assert len(table.rows) == 0

    def test_insert_wrong_type_text(self):
        """Test that inserting wrong type for TEXT column fails"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        # Int instead of text
        with pytest.raises(ValueError, match="Invalid type for column 'name'"):
            table.insert([1, 123])

        assert len(table.rows) == 0

    def test_insert_wrong_type_float(self):
        """Test that inserting wrong type for FLOAT column fails"""
        table = Table('products', ['id', 'price'], ['INT', 'FLOAT'])

        # String instead of float
        with pytest.raises(ValueError, match="Invalid type for column 'price'"):
            table.insert([1, '9.99'])

        # Int instead of float (should fail - strict typing)
        with pytest.raises(ValueError, match="Invalid type for column 'price'"):
            table.insert([1, 10])

        assert len(table.rows) == 0

    def test_insert_wrong_type_bool(self):
        """Test that inserting wrong type for BOOL column fails"""
        table = Table('users', ['id', 'active'], ['INT', 'BOOL'])

        # String instead of bool
        with pytest.raises(ValueError, match="Invalid type for column 'active'"):
            table.insert([1, 'true'])

        # Int instead of bool
        with pytest.raises(ValueError, match="Invalid type for column 'active'"):
            table.insert([1, 1])

        assert len(table.rows) == 0

    def test_insert_preserves_order(self):
        """Test that insert preserves insertion order"""
        table = Table('numbers', ['value'], ['INT'])

        for i in range(10):
            table.insert([i])

        assert len(table.rows) == 10
        for i in range(10):
            assert table.rows[i] == [i]

    def test_insert_empty_string_in_text(self):
        """Test that empty string is valid for TEXT type"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        result = table.insert([1, ''])

        assert result is True
        assert table.rows[0] == [1, '']

    def test_insert_zero_values(self):
        """Test inserting zero values for numeric types"""
        table = Table('data', ['int_val', 'float_val'], ['INT', 'FLOAT'])

        result = table.insert([0, 0.0])

        assert result is True
        assert table.rows[0] == [0, 0.0]

    def test_insert_negative_values(self):
        """Test inserting negative values"""
        table = Table('data', ['int_val', 'float_val'], ['INT', 'FLOAT'])

        result = table.insert([-100, -99.99])

        assert result is True
        assert table.rows[0] == [-100, -99.99]

    def test_insert_returns_true_on_success(self):
        """Test that insert returns True on successful insertion"""
        table = Table('users', ['id'], ['INT'])

        result = table.insert([1])

        assert result is True

    def test_insert_multiple_sequential(self):
        """Test multiple sequential inserts work correctly"""
        table = Table('logs', ['id', 'message'], ['INT', 'TEXT'])

        assert table.insert([1, 'First log']) is True
        assert table.insert([2, 'Second log']) is True
        assert table.insert([3, 'Third log']) is True

        assert len(table.rows) == 3
        assert table.rows[2] == [3, 'Third log']