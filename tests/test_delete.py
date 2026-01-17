import pytest
from rdbms.table import Table


class TestDeleteOperations:
    """Test cases for DELETE operations"""

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

    def test_delete_single_row(self, sample_table):
        """Test DELETE single row"""
        rows_deleted = sample_table.delete(where={'name': 'Alice'})

        assert rows_deleted == 1
        assert len(sample_table.rows) == 4

        # Verify Alice is gone
        results = sample_table.select(where={'name': 'Alice'})
        assert len(results) == 0

        # Verify others remain
        results = sample_table.select()
        assert len(results) == 4

    def test_delete_multiple_rows(self, sample_table):
        """Test DELETE multiple rows"""
        rows_deleted = sample_table.delete(where={'active': True})

        assert rows_deleted == 3  # Alice, Bob, Diana
        assert len(sample_table.rows) == 2

        # Verify only inactive users remain
        results = sample_table.select()
        assert len(results) == 2
        assert results[0][1] == 'Charlie'
        assert results[1][1] == 'Eve'

    def test_delete_no_matching_rows(self, sample_table):
        """Test DELETE with WHERE that matches no rows"""
        rows_deleted = sample_table.delete(where={'name': 'NonExistent'})

        assert rows_deleted == 0
        assert len(sample_table.rows) == 5

    def test_delete_all_rows_no_where(self, sample_table):
        """Test DELETE without WHERE (deletes all rows)"""
        rows_deleted = sample_table.delete()

        assert rows_deleted == 5
        assert len(sample_table.rows) == 0

        # Verify table is empty
        results = sample_table.select()
        assert len(results) == 0

    def test_delete_with_multiple_where_conditions(self, sample_table):
        """Test DELETE with multiple WHERE conditions"""
        rows_deleted = sample_table.delete(where={'active': True, 'age': 28})

        assert rows_deleted == 1  # Only Diana matches
        assert len(sample_table.rows) == 4

        # Verify Diana is gone
        results = sample_table.select(where={'name': 'Diana'})
        assert len(results) == 0

    def test_delete_invalid_column_in_where(self, sample_table):
        """Test DELETE with invalid column name in WHERE raises error"""
        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            sample_table.delete(where={'invalid': 'value'})

    def test_delete_preserves_remaining_row_order(self, sample_table):
        """Test that DELETE preserves order of remaining rows"""
        # Delete Bob (2nd row)
        sample_table.delete(where={'name': 'Bob'})

        results = sample_table.select(columns=['name'])

        # Remaining rows should be in original order
        assert results[0] == ['Alice']
        assert results[1] == ['Charlie']
        assert results[2] == ['Diana']
        assert results[3] == ['Eve']

    def test_delete_first_row(self, sample_table):
        """Test DELETE first row"""
        rows_deleted = sample_table.delete(where={'id': 1})

        assert rows_deleted == 1
        assert len(sample_table.rows) == 4
        assert sample_table.rows[0][0] == 2  # Bob is now first

    def test_delete_last_row(self, sample_table):
        """Test DELETE last row"""
        rows_deleted = sample_table.delete(where={'id': 5})

        assert rows_deleted == 1
        assert len(sample_table.rows) == 4
        assert sample_table.rows[-1][0] == 4  # Diana is now last

    def test_delete_middle_rows(self, sample_table):
        """Test DELETE middle rows"""
        rows_deleted = sample_table.delete(where={'age': 30})

        # Delete Alice (age 30)
        assert rows_deleted == 1
        assert len(sample_table.rows) == 4

    def test_delete_with_zero_value(self):
        """Test DELETE with zero value in WHERE"""
        table = Table('scores', ['id', 'score'], ['INT', 'INT'])
        table.insert([1, 100])
        table.insert([2, 0])
        table.insert([3, 50])

        rows_deleted = table.delete(where={'score': 0})

        assert rows_deleted == 1
        assert len(table.rows) == 2

    def test_delete_with_empty_string(self):
        """Test DELETE with empty string in WHERE"""
        table = Table('data', ['id', 'text'], ['INT', 'TEXT'])
        table.insert([1, 'hello'])
        table.insert([2, ''])
        table.insert([3, 'world'])

        rows_deleted = table.delete(where={'text': ''})

        assert rows_deleted == 1
        assert len(table.rows) == 2

    def test_delete_returns_count(self, sample_table):
        """Test that DELETE returns correct count of deleted rows"""
        count = sample_table.delete(where={'active': False})

        assert count == 2  # Charlie and Eve

        # Delete all remaining
        count = sample_table.delete()

        assert count == 3  # Alice, Bob, Diana

    def test_delete_from_empty_table(self):
        """Test DELETE from empty table"""
        table = Table('empty', ['id', 'name'], ['INT', 'TEXT'])

        rows_deleted = table.delete(where={'id': 1})

        assert rows_deleted == 0
        assert len(table.rows) == 0

    def test_delete_all_then_insert(self, sample_table):
        """Test that table works correctly after deleting all rows"""
        # Delete all
        sample_table.delete()
        assert len(sample_table.rows) == 0

        # Insert new data
        sample_table.insert([10, 'New User', 40, True])

        assert len(sample_table.rows) == 1
        results = sample_table.select()
        assert results[0] == [10, 'New User', 40, True]

    def test_delete_consecutive_operations(self, sample_table):
        """Test multiple consecutive DELETE operations"""
        # First delete
        count1 = sample_table.delete(where={'id': 1})
        assert count1 == 1
        assert len(sample_table.rows) == 4

        # Second delete
        count2 = sample_table.delete(where={'id': 2})
        assert count2 == 1
        assert len(sample_table.rows) == 3

        # Third delete
        count3 = sample_table.delete(where={'active': False})
        assert count3 == 2
        assert len(sample_table.rows) == 1