import pytest
from rdbms.table import Table


class TestJoinOperations:
    """Test cases for JOIN operations"""

    @pytest.fixture
    def users_table(self):
        """Create a users table"""
        table = Table('users', ['id', 'name', 'city_id'], ['INT', 'TEXT', 'INT'])
        table.insert([1, 'Alice', 1])
        table.insert([2, 'Bob', 2])
        table.insert([3, 'Charlie', 1])
        table.insert([4, 'Diana', 3])
        return table

    @pytest.fixture
    def cities_table(self):
        """Create a cities table"""
        table = Table('cities', ['id', 'name'], ['INT', 'TEXT'])
        table.insert([1, 'NYC'])
        table.insert([2, 'LA'])
        table.insert([3, 'Chicago'])
        return table

    def test_simple_inner_join(self, users_table, cities_table):
        """Test basic INNER JOIN"""
        results = users_table.join(cities_table, 'city_id', 'id')

        # Should have 4 results (all users have matching cities)
        assert len(results) == 4

        # Each result should have columns from both tables
        # [user.id, user.name, user.city_id, city.id, city.name]
        assert len(results[0]) == 5

    def test_join_result_values(self, users_table, cities_table):
        """Test that JOIN produces correct combined rows"""
        results = users_table.join(cities_table, 'city_id', 'id')

        # Alice (id=1) from NYC (city_id=1)
        alice_row = [r for r in results if r[1] == 'Alice'][0]
        assert alice_row == [1, 'Alice', 1, 1, 'NYC']

        # Bob (id=2) from LA (city_id=2)
        bob_row = [r for r in results if r[1] == 'Bob'][0]
        assert bob_row == [2, 'Bob', 2, 2, 'LA']

    def test_join_multiple_matches(self, users_table, cities_table):
        """Test JOIN when multiple rows from left table match same right row"""
        results = users_table.join(cities_table, 'city_id', 'id')

        # Both Alice and Charlie are from NYC (city_id=1)
        nyc_users = [r for r in results if r[4] == 'NYC']
        assert len(nyc_users) == 2

        names = {r[1] for r in nyc_users}
        assert names == {'Alice', 'Charlie'}

    def test_join_no_matches(self):
        """Test JOIN when no rows match"""
        table1 = Table('table1', ['id', 'ref_id'], ['INT', 'INT'])
        table1.insert([1, 99])  # ref_id 99 doesn't exist in table2

        table2 = Table('table2', ['id', 'name'], ['INT', 'TEXT'])
        table2.insert([1, 'Item1'])

        results = table1.join(table2, 'ref_id', 'id')

        # No matches, empty result
        assert len(results) == 0

    def test_join_partial_matches(self):
        """Test JOIN when only some rows match"""
        table1 = Table('orders', ['id', 'user_id'], ['INT', 'INT'])
        table1.insert([1, 1])  # Matches
        table1.insert([2, 2])  # Matches
        table1.insert([3, 99])  # No match

        table2 = Table('users', ['id', 'name'], ['INT', 'TEXT'])
        table2.insert([1, 'Alice'])
        table2.insert([2, 'Bob'])

        results = table1.join(table2, 'user_id', 'id')

        # Only 2 matches (orders 1 and 2)
        assert len(results) == 2
        assert results[0] == [1, 1, 1, 'Alice']
        assert results[1] == [2, 2, 2, 'Bob']

    def test_join_with_text_columns(self):
        """Test JOIN on TEXT columns"""
        table1 = Table('employees', ['id', 'dept_code'], ['INT', 'TEXT'])
        table1.insert([1, 'ENG'])
        table1.insert([2, 'HR'])
        table1.insert([3, 'ENG'])

        table2 = Table('departments', ['code', 'name'], ['TEXT', 'TEXT'])
        table2.insert(['ENG', 'Engineering'])
        table2.insert(['HR', 'Human Resources'])

        results = table1.join(table2, 'dept_code', 'code')

        assert len(results) == 3

        # Check Engineering department has 2 employees
        eng_employees = [r for r in results if r[3] == 'Engineering']
        assert len(eng_employees) == 2

    def test_join_invalid_left_column(self, users_table, cities_table):
        """Test JOIN with invalid left table column"""
        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            users_table.join(cities_table, 'invalid', 'id')

    def test_join_invalid_right_column(self, users_table, cities_table):
        """Test JOIN with invalid right table column"""
        with pytest.raises(ValueError, match="Column 'invalid' does not exist"):
            users_table.join(cities_table, 'city_id', 'invalid')

    def test_join_preserves_order(self):
        """Test that JOIN preserves order from left table"""
        table1 = Table('t1', ['id', 'ref'], ['INT', 'INT'])
        table1.insert([3, 1])
        table1.insert([1, 1])
        table1.insert([2, 1])

        table2 = Table('t2', ['id', 'name'], ['INT', 'TEXT'])
        table2.insert([1, 'Item'])

        results = table1.join(table2, 'ref', 'id')

        # Should maintain left table order
        assert results[0][0] == 3
        assert results[1][0] == 1
        assert results[2][0] == 2

    def test_join_empty_left_table(self, cities_table):
        """Test JOIN with empty left table"""
        empty_table = Table('empty', ['id', 'city_id'], ['INT', 'INT'])

        results = empty_table.join(cities_table, 'city_id', 'id')

        assert len(results) == 0

    def test_join_empty_right_table(self, users_table):
        """Test JOIN with empty right table"""
        empty_table = Table('empty', ['id', 'name'], ['INT', 'TEXT'])

        results = users_table.join(empty_table, 'city_id', 'id')

        assert len(results) == 0

    def test_join_both_empty_tables(self):
        """Test JOIN with both tables empty"""
        table1 = Table('t1', ['id', 'ref'], ['INT', 'INT'])
        table2 = Table('t2', ['id', 'name'], ['INT', 'TEXT'])

        results = table1.join(table2, 'ref', 'id')

        assert len(results) == 0

    def test_join_same_column_names(self):
        """Test JOIN when both tables have same column names"""
        table1 = Table('t1', ['id', 'value'], ['INT', 'INT'])
        table1.insert([1, 100])
        table1.insert([2, 200])

        table2 = Table('t2', ['id', 'description'], ['INT', 'TEXT'])
        table2.insert([1, 'First'])
        table2.insert([2, 'Second'])

        results = table1.join(table2, 'id', 'id')

        # Both id columns should be present
        assert len(results) == 2
        assert results[0] == [1, 100, 1, 'First']
        assert results[1] == [2, 200, 2, 'Second']

    def test_join_with_duplicate_values_in_right_table(self):
        """Test JOIN when right table has duplicate values in join column"""
        table1 = Table('orders', ['id', 'product_id'], ['INT', 'INT'])
        table1.insert([1, 100])

        table2 = Table('products', ['id', 'name'], ['INT', 'TEXT'])
        table2.insert([100, 'Widget'])
        table2.insert([100, 'Widget Deluxe'])  # Duplicate product_id

        results = table1.join(table2, 'product_id', 'id')

        # Should match both rows from right table
        assert len(results) == 2
        assert results[0] == [1, 100, 100, 'Widget']
        assert results[1] == [1, 100, 100, 'Widget Deluxe']

    def test_join_three_column_tables(self):
        """Test JOIN with tables having multiple columns"""
        table1 = Table('students', ['id', 'name', 'class_id'], ['INT', 'TEXT', 'INT'])
        table1.insert([1, 'Alice', 101])
        table1.insert([2, 'Bob', 102])

        table2 = Table('classes', ['id', 'name', 'teacher'], ['INT', 'TEXT', 'TEXT'])
        table2.insert([101, 'Math', 'Prof. Smith'])
        table2.insert([102, 'Science', 'Prof. Jones'])

        results = table1.join(table2, 'class_id', 'id')

        assert len(results) == 2
        # [student.id, student.name, student.class_id, class.id, class.name, class.teacher]
        assert len(results[0]) == 6
        assert results[0] == [1, 'Alice', 101, 101, 'Math', 'Prof. Smith']

    def test_join_with_zero_values(self):
        """Test JOIN with zero values in join columns"""
        table1 = Table('t1', ['id', 'ref'], ['INT', 'INT'])
        table1.insert([1, 0])

        table2 = Table('t2', ['id', 'name'], ['INT', 'TEXT'])
        table2.insert([0, 'Zero'])

        results = table1.join(table2, 'ref', 'id')

        assert len(results) == 1
        assert results[0] == [1, 0, 0, 'Zero']