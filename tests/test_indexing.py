import pytest
from rdbms.table import Table


class TestIndexing:
    """Test cases for indexing"""

    def test_create_index_on_column(self):
        """Test creating an index on a column"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 30])
        table.insert([2, 'Bob', 25])
        table.insert([3, 'Charlie', 35])

        table.create_index('age')

        assert 'age' in table.indexes
        assert len(table.indexes['age']) > 0

    def test_create_index_invalid_column(self):
        """Test creating index on non-existent column raises error"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            table.create_index('invalid')

    def test_index_contains_correct_mappings(self):
        """Test that index maps values to row positions"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 30])
        table.insert([2, 'Bob', 25])
        table.insert([3, 'Charlie', 30])  # Same age as Alice

        table.create_index('age')

        # Age 30 should map to rows 0 and 2 (Alice and Charlie)
        assert 30 in table.indexes['age']
        assert set(table.indexes['age'][30]) == {0, 2}

        # Age 25 should map to row 1 (Bob)
        assert 25 in table.indexes['age']
        assert table.indexes['age'][25] == [1]

    def test_index_on_text_column(self):
        """Test creating index on TEXT column"""
        table = Table('users', ['id', 'name', 'city'], ['INT', 'TEXT', 'TEXT'])
        table.insert([1, 'Alice', 'NYC'])
        table.insert([2, 'Bob', 'LA'])
        table.insert([3, 'Charlie', 'NYC'])

        table.create_index('city')

        assert 'NYC' in table.indexes['city']
        assert set(table.indexes['city']['NYC']) == {0, 2}

    def test_insert_updates_index(self):
        """Test that INSERT updates existing indexes"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.create_index('age')

        table.insert([1, 'Alice', 30])
        assert 30 in table.indexes['age']
        assert table.indexes['age'][30] == [0]

        table.insert([2, 'Bob', 25])
        assert 25 in table.indexes['age']
        assert table.indexes['age'][25] == [1]

        table.insert([3, 'Charlie', 30])
        assert set(table.indexes['age'][30]) == {0, 2}

    def test_delete_updates_index(self):
        """Test that DELETE updates existing indexes"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 30])
        table.insert([2, 'Bob', 25])
        table.insert([3, 'Charlie', 30])

        table.create_index('age')

        # Delete Bob
        table.delete(where={'name': 'Bob'})

        # Rebuild index to reflect new row positions
        table.create_index('age')

        # Age 30 should now map to rows 0 and 1 (positions shifted)
        assert set(table.indexes['age'][30]) == {0, 1}

        # Age 25 should not exist anymore
        assert 25 not in table.indexes['age']

    def test_update_updates_index(self):
        """Test that UPDATE updates existing indexes"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 30])
        table.insert([2, 'Bob', 25])
        table.insert([3, 'Charlie', 35])

        table.create_index('age')

        # Update Bob's age from 25 to 40
        table.update(set_values={'age': 40}, where={'name': 'Bob'})

        # Rebuild index
        table.create_index('age')

        # Age 25 should no longer exist
        assert 25 not in table.indexes['age']

        # Age 40 should map to Bob's row
        assert 40 in table.indexes['age']
        assert table.indexes['age'][40] == [1]

    def test_multiple_indexes_on_different_columns(self):
        """Test creating multiple indexes on different columns"""
        table = Table('users', ['id', 'name', 'age', 'city'], ['INT', 'TEXT', 'INT', 'TEXT'])
        table.insert([1, 'Alice', 30, 'NYC'])
        table.insert([2, 'Bob', 25, 'LA'])

        table.create_index('age')
        table.create_index('city')

        assert 'age' in table.indexes
        assert 'city' in table.indexes
        assert len(table.indexes) == 2

    def test_recreate_index_overwrites(self):
        """Test that creating an index twice overwrites the old one"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 30])

        table.create_index('age')
        original_count = len(table.indexes['age'][30])

        # Add more data and recreate index
        table.insert([2, 'Bob', 30])
        table.create_index('age')

        # Index should be updated with both rows
        assert len(table.indexes['age'][30]) == 2
        assert original_count == 1
        assert set(table.indexes['age'][30]) == {0, 1}

    def test_index_with_primary_key(self):
        """Test that primary key automatically creates an index"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'], primary_key='id')
        table.insert([1, 'Alice'])
        table.insert([2, 'Bob'])
        table.insert([3, 'Charlie'])

        # Primary key should have an index
        table.create_index('id')

        assert 'id' in table.indexes
        assert table.indexes['id'][1] == [0]
        assert table.indexes['id'][2] == [1]
        assert table.indexes['id'][3] == [2]

    def test_index_persists_in_dict(self):
        """Test that indexes are saved in to_dict"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 30])
        table.insert([2, 'Bob', 25])

        table.create_index('age')

        data = table.to_dict()

        assert 'indexes' in data
        assert 'age' in data['indexes']

    def test_index_loads_from_dict(self):
        """Test that indexes are restored from from_dict"""
        data = {
            'name': 'users',
            'columns': ['id', 'name', 'age'],
            'types': ['INT', 'TEXT', 'INT'],
            'rows': [[1, 'Alice', 30], [2, 'Bob', 25]],
            'primary_key': None,
            'unique_constraints': [],
            'indexes': {'age': {30: [0], 25: [1]}}
        }

        table = Table.from_dict(data)

        assert 'age' in table.indexes
        assert table.indexes['age'][30] == [0]
        assert table.indexes['age'][25] == [1]

    def test_index_with_duplicate_values(self):
        """Test index correctly handles duplicate values"""
        table = Table('products', ['id', 'name', 'category'], ['INT', 'TEXT', 'TEXT'])
        table.insert([1, 'Widget', 'Tools'])
        table.insert([2, 'Gadget', 'Electronics'])
        table.insert([3, 'Hammer', 'Tools'])
        table.insert([4, 'Phone', 'Electronics'])
        table.insert([5, 'Screwdriver', 'Tools'])

        table.create_index('category')

        # Tools should map to rows 0, 2, 4
        assert set(table.indexes['category']['Tools']) == {0, 2, 4}

        # Electronics should map to rows 1, 3
        assert set(table.indexes['category']['Electronics']) == {1, 3}

    def test_index_empty_table(self):
        """Test creating index on empty table"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        table.create_index('name')

        assert 'name' in table.indexes
        assert len(table.indexes['name']) == 0

    def test_drop_index(self):
        """Test dropping an index"""
        table = Table('users', ['id', 'name', 'age'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 30])

        table.create_index('age')
        assert 'age' in table.indexes

        table.drop_index('age')
        assert 'age' not in table.indexes

    def test_drop_nonexistent_index(self):
        """Test dropping a non-existent index raises error"""
        table = Table('users', ['id', 'name'], ['INT', 'TEXT'])

        with pytest.raises(ValueError, match="Index on column 'name' does not exist"):
            table.drop_index('name')