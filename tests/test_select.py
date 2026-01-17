import pytest
from rdbms.table import Table


class TestSelectOperations:
    """Test cases for SELECT operations"""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table with test data"""
        table = Table('users', ['id', 'name', 'age', 'active'], ['INT', 'TEXT', 'INT', 'BOOL'])
        table.insert([1, 'Alice', 30, True])
        table.insert([2, 'Bob', 25, True])
        table.insert([3, 'Charlie', 35, False])
        table.insert([4, 'Diana', 28, True])
        table.insert([5, 'Eve', 32, False])
        return table

    def test_select_all_columns_all_rows(self, sample_table):
        """Test SELECT * (all columns, all rows)"""
        results = sample_table.select()

        assert len(results) == 5
        assert results[0] == [1, 'Alice', 30, True]
        assert results[1] == [2, 'Bob', 25, True]
        assert results[4] == [5, 'Eve', 32, False]

    def test_select_specific_columns(self, sample_table):
        """Test SELECT with specific columns"""
        results = sample_table.select(columns=['name', 'age'])

        assert len(results) == 5
        assert results[0] == ['Alice', 30]
        assert results[1] == ['Bob', 25]
        assert results[2] == ['Charlie', 35]

    def test_select_single_column(self, sample_table):
        """Test SELECT with single column"""
        results = sample_table.select(columns=['name'])

        assert len(results) == 5
        assert results[0] == ['Alice']
        assert results[1] == ['Bob']
        assert results[2] == ['Charlie']

    def test_select_columns_different_order(self, sample_table):
        """Test SELECT with columns in different order than schema"""
        results = sample_table.select(columns=['age', 'name', 'id'])

        assert len(results) == 5
        assert results[0] == [30, 'Alice', 1]
        assert results[1] == [25, 'Bob', 2]

    def test_select_with_where_equals(self, sample_table):
        """Test SELECT with WHERE clause (equality)"""
        results = sample_table.select(where={'name': 'Alice'})

        assert len(results) == 1
        assert results[0] == [1, 'Alice', 30, True]

    def test_select_with_where_int(self, sample_table):
        """Test SELECT with WHERE on INT column"""
        results = sample_table.select(where={'age': 25})

        assert len(results) == 1
        assert results[0] == [2, 'Bob', 25, True]

    def test_select_with_where_bool(self, sample_table):
        """Test SELECT with WHERE on BOOL column"""
        results = sample_table.select(where={'active': True})

        assert len(results) == 3
        assert results[0] == [1, 'Alice', 30, True]
        assert results[1] == [2, 'Bob', 25, True]
        assert results[2] == [4, 'Diana', 28, True]

    def test_select_with_where_no_matches(self, sample_table):
        """Test SELECT with WHERE that matches no rows"""
        results = sample_table.select(where={'name': 'NonExistent'})

        assert len(results) == 0
        assert results == []

    def test_select_columns_with_where(self, sample_table):
        """Test SELECT specific columns WITH WHERE"""
        results = sample_table.select(columns=['name', 'age'], where={'active': True})

        assert len(results) == 3
        assert results[0] == ['Alice', 30]
        assert results[1] == ['Bob', 25]
        assert results[2] == ['Diana', 28]

    def test_select_empty_table(self):
        """Test SELECT on empty table"""
        table = Table('empty', ['id', 'name'], ['INT', 'TEXT'])

        results = table.select()

        assert len(results) == 0
        assert results == []

    def test_select_invalid_column(self, sample_table):
        """Test SELECT with non-existent column raises error"""
        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            sample_table.select(columns=['invalid'])

    def test_select_where_invalid_column(self, sample_table):
        """Test SELECT with WHERE on non-existent column raises error"""
        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            sample_table.select(where={'invalid': 'value'})

    def test_select_all_columns_returns_copy(self, sample_table):
        """Test that SELECT returns a copy, not reference to original rows"""
        results = sample_table.select()

        # Modify result
        results[0][1] = 'Modified'

        # Original should be unchanged
        assert sample_table.rows[0][1] == 'Alice'

    def test_select_with_multiple_where_conditions(self, sample_table):
        """Test SELECT with multiple WHERE conditions (AND logic)"""
        # Add more test data
        sample_table.insert([6, 'Frank', 30, True])

        results = sample_table.select(where={'age': 30, 'active': True})

        assert len(results) == 2
        # Both Alice and Frank are 30 and active
        assert results[0] == [1, 'Alice', 30, True]
        assert results[1] == [6, 'Frank', 30, True]

    def test_select_single_column_with_where(self, sample_table):
        """Test SELECT single column with WHERE"""
        results = sample_table.select(columns=['name'], where={'active': False})

        assert len(results) == 2
        assert results[0] == ['Charlie']
        assert results[1] == ['Eve']

    def test_select_preserves_row_order(self):
        """Test that SELECT preserves insertion order"""
        table = Table('numbers', ['value'], ['INT'])

        for i in range(10, 0, -1):  # Insert in reverse order
            table.insert([i])

        results = table.select()

        # Should be in insertion order (10, 9, 8, ..., 1)
        assert results[0] == [10]
        assert results[9] == [1]

    def test_select_where_with_zero_value(self):
        """Test SELECT WHERE with zero value"""
        table = Table('scores', ['id', 'score'], ['INT', 'INT'])
        table.insert([1, 100])
        table.insert([2, 0])
        table.insert([3, 50])

        results = table.select(where={'score': 0})

        assert len(results) == 1
        assert results[0] == [2, 0]

    def test_select_where_with_empty_string(self):
        """Test SELECT WHERE with empty string"""
        table = Table('data', ['id', 'text'], ['INT', 'TEXT'])
        table.insert([1, 'hello'])
        table.insert([2, ''])
        table.insert([3, 'world'])

        results = table.select(where={'text': ''})

        assert len(results) == 1
        assert results[0] == [2, '']