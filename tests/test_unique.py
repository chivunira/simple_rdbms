import pytest
from rdbms.table import Table


class TestUniqueConstraints:
    """Test cases for UNIQUE constraints"""

    def test_create_table_with_unique_constraint(self):
        """Test creating table with unique constraint"""
        table = Table('users', ['id', 'email', 'name'], ['INT', 'TEXT', 'TEXT'],
                      unique_constraints=['email'])

        assert 'email' in table.unique_constraints

    def test_create_table_multiple_unique_constraints(self):
        """Test creating table with multiple unique constraints"""
        table = Table('users', ['id', 'email', 'username'], ['INT', 'TEXT', 'TEXT'],
                      unique_constraints=['email', 'username'])

        assert 'email' in table.unique_constraints
        assert 'username' in table.unique_constraints
        assert len(table.unique_constraints) == 2

    def test_unique_constraint_invalid_column(self):
        """Test creating table with unique constraint on non-existent column"""
        with pytest.raises(ValueError, match="Unique constraint column 'invalid' does not exist"):
            Table('users', ['id', 'name'], ['INT', 'TEXT'], unique_constraints=['invalid'])

    def test_insert_with_unique_constraint(self):
        """Test inserting rows with unique constraint"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'], unique_constraints=['email'])

        table.insert([1, 'alice@example.com'])
        table.insert([2, 'bob@example.com'])

        assert len(table.rows) == 2

    def test_insert_duplicate_unique_value(self):
        """Test that inserting duplicate unique value fails"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'], unique_constraints=['email'])

        table.insert([1, 'alice@example.com'])

        with pytest.raises(ValueError, match="Duplicate value for unique column 'email': alice@example.com"):
            table.insert([2, 'alice@example.com'])

        assert len(table.rows) == 1

    def test_unique_constraint_allows_different_values(self):
        """Test that different values are allowed in unique column"""
        table = Table('users', ['id', 'username'], ['INT', 'TEXT'], unique_constraints=['username'])

        table.insert([1, 'alice'])
        table.insert([2, 'bob'])
        table.insert([3, 'charlie'])

        assert len(table.rows) == 3

    def test_multiple_unique_constraints_enforced(self):
        """Test that multiple unique constraints are all enforced"""
        table = Table('users', ['id', 'email', 'username'], ['INT', 'TEXT', 'TEXT'],
                      unique_constraints=['email', 'username'])

        table.insert([1, 'alice@example.com', 'alice'])

        # Duplicate email should fail
        with pytest.raises(ValueError, match="Duplicate value for unique column 'email'"):
            table.insert([2, 'alice@example.com', 'bob'])

        # Duplicate username should fail
        with pytest.raises(ValueError, match="Duplicate value for unique column 'username'"):
            table.insert([2, 'bob@example.com', 'alice'])

    def test_unique_and_primary_key_together(self):
        """Test table with both primary key and unique constraints"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'],
                      primary_key='id', unique_constraints=['email'])

        table.insert([1, 'alice@example.com'])

        # Duplicate id should fail (primary key)
        with pytest.raises(ValueError, match="Duplicate primary key value: 1"):
            table.insert([1, 'bob@example.com'])

        # Duplicate email should fail (unique)
        with pytest.raises(ValueError, match="Duplicate value for unique column 'email'"):
            table.insert([2, 'alice@example.com'])

    def test_update_does_not_violate_unique(self):
        """Test UPDATE that doesn't violate unique constraint"""
        table = Table('users', ['id', 'email', 'name'], ['INT', 'TEXT', 'TEXT'],
                      unique_constraints=['email'])

        table.insert([1, 'alice@example.com', 'Alice'])
        table.insert([2, 'bob@example.com', 'Bob'])

        # Update non-unique column is fine
        table.update(set_values={'name': 'Alice Smith'}, where={'id': 1})

        assert len(table.rows) == 2

    def test_update_unique_column_to_new_value(self):
        """Test UPDATE that changes unique column to new value"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'], unique_constraints=['email'])

        table.insert([1, 'alice@example.com'])
        table.insert([2, 'bob@example.com'])

        # Change email to new value (should work)
        table.update(set_values={'email': 'alice.new@example.com'}, where={'id': 1})

        results = table.select(where={'id': 1})
        assert results[0][1] == 'alice.new@example.com'

    def test_update_unique_column_to_duplicate_value(self):
        """Test UPDATE that would violate unique constraint"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'], unique_constraints=['email'])

        table.insert([1, 'alice@example.com'])
        table.insert([2, 'bob@example.com'])

        # Try to change Alice's email to Bob's (should fail)
        with pytest.raises(ValueError, match="Duplicate value for unique column 'email'"):
            table.update(set_values={'email': 'bob@example.com'}, where={'id': 1})

    def test_delete_and_reinsert_unique_value(self):
        """Test that deleted unique value can be reused"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'], unique_constraints=['email'])

        table.insert([1, 'alice@example.com'])
        table.delete(where={'id': 1})

        # Should be able to insert the same email again
        table.insert([2, 'alice@example.com'])

        assert len(table.rows) == 1

    def test_unique_constraint_with_int_column(self):
        """Test unique constraint on INT column"""
        table = Table('users', ['id', 'badge_number', 'name'], ['INT', 'INT', 'TEXT'],
                      unique_constraints=['badge_number'])

        table.insert([1, 100, 'Alice'])
        table.insert([2, 200, 'Bob'])

        with pytest.raises(ValueError, match="Duplicate value for unique column 'badge_number': 100"):
            table.insert([3, 100, 'Charlie'])

    def test_unique_constraint_empty_string(self):
        """Test unique constraint with empty string"""
        table = Table('users', ['id', 'username'], ['INT', 'TEXT'], unique_constraints=['username'])

        table.insert([1, ''])

        # Second empty string should fail
        with pytest.raises(ValueError, match="Duplicate value for unique column 'username': "):
            table.insert([2, ''])

    def test_unique_constraint_zero_value(self):
        """Test unique constraint with zero value"""
        table = Table('data', ['id', 'code'], ['INT', 'INT'], unique_constraints=['code'])

        table.insert([1, 0])

        with pytest.raises(ValueError, match="Duplicate value for unique column 'code': 0"):
            table.insert([2, 0])

    def test_table_without_unique_allows_duplicates(self):
        """Test that table without unique constraint allows duplicates"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'])  # No unique constraint

        table.insert([1, 'alice@example.com'])
        table.insert([2, 'alice@example.com'])

        assert len(table.rows) == 2

    def test_unique_persists_in_dict(self):
        """Test that unique constraints are saved in to_dict"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'], unique_constraints=['email'])

        data = table.to_dict()

        assert 'unique_constraints' in data
        assert 'email' in data['unique_constraints']

    def test_unique_loads_from_dict(self):
        """Test that unique constraints are restored from from_dict"""
        data = {
            'name': 'users',
            'columns': ['id', 'email'],
            'types': ['INT', 'TEXT'],
            'rows': [[1, 'alice@example.com']],
            'primary_key': None,
            'unique_constraints': ['email'],
            'indexes': {}
        }

        table = Table.from_dict(data)

        assert 'email' in table.unique_constraints

        # Verify constraint is enforced
        with pytest.raises(ValueError, match="Duplicate value for unique column 'email'"):
            table.insert([2, 'alice@example.com'])

    def test_update_multiple_rows_same_unique_value(self):
        """Test UPDATE that would make multiple rows have same unique value"""
        table = Table('users', ['id', 'email'], ['INT', 'TEXT'], unique_constraints=['email'])

        table.insert([1, 'alice@example.com'])
        table.insert([2, 'bob@example.com'])
        table.insert([3, 'charlie@example.com'])

        # Try to update all emails to the same value (should fail)
        with pytest.raises(ValueError, match="Duplicate value for unique column 'email'"):
            table.update(set_values={'email': 'same@example.com'})