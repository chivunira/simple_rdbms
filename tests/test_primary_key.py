import pytest
from rdbms.table import Table


class TestPrimaryKeyConstraints:
    """Test cases for PRIMARY KEY constraints"""

    def test_create_table_with_primary_key(self):
        """Test creating table with primary key"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        assert table.primary_key == 'id'

    def test_create_table_primary_key_invalid_column(self):
        """Test creating table with primary key on non-existent column"""
        with pytest.raises(ValueError, match="Primary key column 'invalid' does not exist"):
            Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='invalid')

    def test_insert_with_primary_key(self):
        """Test inserting rows with primary key"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])

        assert len(table.rows) == 2

    def test_insert_duplicate_primary_key(self):
        """Test that inserting duplicate primary key fails"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])

        with pytest.raises(ValueError, match="Duplicate primary key value: 1"):
            table.insert([1, 'Bob'])

        # Verify only one row exists
        assert len(table.rows) == 1

    def test_insert_duplicate_primary_key_after_multiple_inserts(self):
        """Test duplicate primary key detection after multiple inserts"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])
        table.insert([3, 'Charlie'])

        with pytest.raises(ValueError, match="Duplicate primary key value: 2"):
            table.insert([2, 'Diana'])

        assert len(table.rows) == 3

    def test_primary_key_allows_different_values(self):
        """Test that different primary key values are allowed"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])
        table.insert([3, 'Charlie'])
        table.insert([100, 'Diana'])
        table.insert([999, 'Eve'])

        assert len(table.rows) == 5

    def test_primary_key_with_text_column(self):
        """Test primary key on TEXT column"""
        table = Table('users', ['username', 'email'], ['TEXT', 'TEXT'], primary_key='username')

        table.insert(['alice', 'alice@example.com'])
        table.insert(['bob', 'bob@example.com'])

        with pytest.raises(ValueError, match="Duplicate primary key value: alice"):
            table.insert(['alice', 'another@example.com'])

    def test_update_does_not_violate_primary_key(self):
        """Test UPDATE that doesn't change primary key"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'], primary_key='id')

        table.insert([1, 'Alice', 30])
        table.insert([2, 'Bob', 25])

        # Update non-primary key column is fine
        table.update(set_values={'age': 31}, where={'id': 1})

        assert len(table.rows) == 2

    def test_update_primary_key_to_new_value(self):
        """Test UPDATE that changes primary key to new value"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])

        # Change id from 1 to 10 (should work, 10 is not taken)
        table.update(set_values={'id': 10}, where={'id': 1})

        results = table.select(where={'name': 'Alice'})
        assert results[0][0] == 10

    def test_update_primary_key_to_duplicate_value(self):
        """Test UPDATE that would create duplicate primary key"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])

        # Try to change id from 1 to 2 (should fail, 2 already exists)
        with pytest.raises(ValueError, match="Duplicate primary key value: 2"):
            table.update(set_values={'id': 2}, where={'id': 1})

    def test_delete_and_reinsert_primary_key(self):
        """Test that deleted primary key can be reused"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])
        table.delete(where={'id': 1})

        # Should be able to insert id=1 again
        table.insert([1, 'Bob'])

        assert len(table.rows) == 1
        results = table.select()
        assert results[0] == [1, 'Bob']

    def test_primary_key_zero_value(self):
        """Test primary key with zero value"""
        table = Table('data', ['id', 'value'], ['INT', 'TEXT'], primary_key='id')

        table.insert([0, 'zero'])

        with pytest.raises(ValueError, match="Duplicate primary key value: 0"):
            table.insert([0, 'another'])

    def test_primary_key_negative_value(self):
        """Test primary key with negative value"""
        table = Table('data', ['id', 'value'], ['INT', 'TEXT'], primary_key='id')

        table.insert([-1, 'negative'])
        table.insert([-2, 'more negative'])

        with pytest.raises(ValueError, match="Duplicate primary key value: -1"):
            table.insert([-1, 'duplicate'])

    def test_primary_key_empty_string(self):
        """Test primary key with empty string"""
        table = Table('users', ['username', 'email'], ['TEXT', 'TEXT'], primary_key='username')

        table.insert(['', 'empty@example.com'])

        with pytest.raises(ValueError, match="Duplicate primary key value: "):
            table.insert(['', 'another@example.com'])

    def test_table_without_primary_key_allows_duplicates(self):
        """Test that table without primary key allows duplicate values"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])  # No primary key

        table.insert([1, 'Alice'])
        table.insert([1, 'Bob'])  # Same id, should be allowed
        table.insert([1, 'Charlie'])

        assert len(table.rows) == 3

    def test_primary_key_persists_in_dict(self):
        """Test that primary key is saved in to_dict"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        data = table.to_dict()

        assert data['primary_key'] == 'id'

    def test_primary_key_loads_from_dict(self):
        """Test that primary key is restored from from_dict"""
        data = {
            'name': 'users',
            'columns': ['id', 'name'],
            'types': ['INT', 'TEXT'],
            'rows': [[1, 'Alice'], [2, 'Bob']],
            'primary_key': 'id',
            'unique_constraints': [],
            'indexes': {}
        }

        table = Table.from_dict(data)

        assert table.primary_key == 'id'

        # Verify constraint is enforced
        with pytest.raises(ValueError, match="Duplicate primary key value: 1"):
            table.insert([1, 'Charlie'])

    def test_update_multiple_rows_same_primary_key(self):
        """Test UPDATE that would make multiple rows have same primary key"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')

        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])
        table.insert([3, 'Charlie'])

        # Try to update all ids to 1 (should fail)
        with pytest.raises(ValueError, match="Duplicate primary key value: 1"):
            table.update(set_values={'id': 1})