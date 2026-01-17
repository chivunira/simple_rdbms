import pytest
from rdbms.table import Table


class TestUpdateOperations:
    """Test cases for UPDATE operations"""

    @pytest.fixture
    def sample_table(self):
        """Create a sample table with test data"""
        table = Table('users', ['id', 'name', 'age', 'active'], ['INT', 'TEXT', 'INT', 'BOOL'])
        table.insert([1, 'Alice', 30, True])
        table.insert([2, 'Bob', 25, True])
        table.insert([3, 'Charlie', 35, False])
        table.insert([4, 'Diana', 28, True])
        return table

    def test_update_single_column_single_row(self, sample_table):
        """Test UPDATE single column for one row"""
        rows_updated = sample_table.update(
            set_values={'age': 31},
            where={'name': 'Alice'}
        )

        assert rows_updated == 1

        # Verify the update
        results = sample_table.select(where={'name': 'Alice'})
        assert results[0] == [1, 'Alice', 31, True]

        # Verify other rows unchanged
        results = sample_table.select(where={'name': 'Bob'})
        assert results[0] == [2, 'Bob', 25, True]

    def test_update_multiple_columns(self, sample_table):
        """Test UPDATE multiple columns at once"""
        rows_updated = sample_table.update(
            set_values={'age': 26, 'active': False},
            where={'name': 'Bob'}
        )

        assert rows_updated == 1

        results = sample_table.select(where={'name': 'Bob'})
        assert results[0] == [2, 'Bob', 26, False]

    def test_update_multiple_rows(self, sample_table):
        """Test UPDATE affecting multiple rows"""
        rows_updated = sample_table.update(
            set_values={'active': False},
            where={'active': True}
        )

        assert rows_updated == 3  # Alice, Bob, Diana

        # Verify all were updated
        results = sample_table.select(where={'active': False})
        assert len(results) == 4  # All 4 rows now have active=False

    def test_update_no_matching_rows(self, sample_table):
        """Test UPDATE with WHERE that matches no rows"""
        rows_updated = sample_table.update(
            set_values={'age': 100},
            where={'name': 'NonExistent'}
        )

        assert rows_updated == 0

        # Verify nothing changed
        assert len(sample_table.rows) == 4

    def test_update_all_rows_no_where(self, sample_table):
        """Test UPDATE without WHERE (updates all rows)"""
        rows_updated = sample_table.update(
            set_values={'active': False}
        )

        assert rows_updated == 4

        # Verify all rows updated
        results = sample_table.select()
        for row in results:
            assert row[3] == False  # active column

    def test_update_with_multiple_where_conditions(self, sample_table):
        """Test UPDATE with multiple WHERE conditions"""
        rows_updated = sample_table.update(
            set_values={'age': 29},
            where={'active': True, 'age': 28}
        )

        assert rows_updated == 1  # Only Diana matches

        results = sample_table.select(where={'name': 'Diana'})
        assert results[0] == [4, 'Diana', 29, True]

    def test_update_invalid_column_in_set(self, sample_table):
        """Test UPDATE with invalid column name in SET raises error"""
        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            sample_table.update(
                set_values={'invalid': 'value'},
                where={'name': 'Alice'}
            )

    def test_update_invalid_column_in_where(self, sample_table):
        """Test UPDATE with invalid column name in WHERE raises error"""
        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            sample_table.update(
                set_values={'age': 30},
                where={'invalid': 'value'}
            )

    def test_update_wrong_type(self, sample_table):
        """Test UPDATE with wrong data type raises error"""
        with pytest.raises(ValueError, match="Invalid type for column 'age'"):
            sample_table.update(
                set_values={'age': 'thirty'},
                where={'name': 'Alice'}
            )

    def test_update_preserves_other_columns(self, sample_table):
        """Test that UPDATE only changes specified columns"""
        original = sample_table.select(where={'name': 'Alice'})[0]

        sample_table.update(
            set_values={'age': 31},
            where={'name': 'Alice'}
        )

        updated = sample_table.select(where={'name': 'Alice'})[0]

        # Only age should change
        assert updated[0] == original[0]  # id unchanged
        assert updated[1] == original[1]  # name unchanged
        assert updated[2] == 31  # age changed
        assert updated[3] == original[3]  # active unchanged

    def test_update_to_zero(self):
        """Test UPDATE to zero value"""
        table = Table('scores', ['id', 'score'], ['INT', 'INT'])
        table.insert([1, 100])
        table.insert([2, 50])

        rows_updated = table.update(
            set_values={'score': 0},
            where={'id': 1}
        )

        assert rows_updated == 1
        results = table.select(where={'id': 1})
        assert results[0] == [1, 0]

    def test_update_to_empty_string(self):
        """Test UPDATE to empty string"""
        table = Table('data', ['id', 'text'], ['INT', 'TEXT'])
        table.insert([1, 'hello'])
        table.insert([2, 'world'])

        rows_updated = table.update(
            set_values={'text': ''},
            where={'id': 1}
        )

        assert rows_updated == 1
        results = table.select(where={'id': 1})
        assert results[0] == [1, '']

    def test_update_returns_count(self, sample_table):
        """Test that UPDATE returns correct count of updated rows"""
        # Update 2 rows
        count = sample_table.update(
            set_values={'age': 40},
            where={'active': False}
        )

        assert count == 1  # Only Charlie is inactive

        # Update all rows
        count = sample_table.update(
            set_values={'active': True}
        )

        assert count == 4

    def test_update_empty_set_values(self, sample_table):
        """Test UPDATE with no columns to set raises error"""
        with pytest.raises(ValueError, match="Must specify at least one column to update"):
            sample_table.update(
                set_values={},
                where={'name': 'Alice'}
            )

    def test_update_same_value(self, sample_table):
        """Test UPDATE to same value (should still count as update)"""
        rows_updated = sample_table.update(
            set_values={'age': 30},  # Alice already has age 30
            where={'name': 'Alice'}
        )

        assert rows_updated == 1

        results = sample_table.select(where={'name': 'Alice'})
        assert results[0] == [1, 'Alice', 30, True]