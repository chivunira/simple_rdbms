import pytest
import os
import json
import tempfile
import shutil
from rdbms.storage import Storage


class TestStorage:
    """
    Test cases for the Storage layer
    """

    @pytest.fixture
    def temp_db_dir(self):
        """
        Create a temporary directory for database files
        """
        temp_dir = tempfile.mkdtemp()
        yield temp_dir

        # Cleanup after test
        shutil.rmtree(temp_dir)

    def test_create_storage(self, temp_db_dir):
        """
        Test saving table data to file
        """
        storage = Storage(temp_db_dir)

        table_data = {
            'name': 'users',
            'columns': ['id', 'name', 'email'],
            'types': ['INT', 'TEXT', 'TEXT'],
            'rows': [
                [1, 'John', 'john@chivucodes.com'],
                [2, 'Bob', 'bob@chivucodes.com']
            ],
            'primary_key': 'id',
            'indexes': {}
        }

        storage.save_table('users', table_data)

        # Verify file was created
        table_file = os.path.join(temp_db_dir, 'users.json')
        assert os.path.exists(table_file)

        # Verify content is correct
        with open(table_file, 'r') as f:
            saved_data = json.load(f)

        assert saved_data == table_data

    def test_load_table_data(self, temp_db_dir):
        """
        Test loading table data from file
        """
        storage = Storage(temp_db_dir)

        # First save some data
        table_data = {
            'name': 'products',
            'columns': ['id', 'name', 'price'],
            'types': ['INT', 'TEXT', 'FLOAT'],
            'rows': [
                [1, 'Laptop', 999.99],
                [2, 'Phone', 499.49]
            ],
            'primary_key': 'id',
            'indexes': {}
        }

        storage.save_table('products', table_data)

        # Load it back
        loaded_data = storage.load_table('products')

        assert loaded_data == table_data

    def test_load_nonexistent_table(self, temp_db_dir):
        """
        Test loading a table that does not exist
        """
        storage = Storage(temp_db_dir)

        loaded_data = storage.load_table('nonexistent')

        assert loaded_data is None

    def test_list_tables(self, temp_db_dir):
        """
        Test listing all tables in storage
        """
        storage = Storage(temp_db_dir)

        # Save multiple tables
        storage.save_table('users', {'name': 'users', 'columns': [], 'types': [], 'rows': []})
        storage.save_table('products', {'name': 'products', 'columns': [], 'types': [], 'rows': []})
        storage.save_table('orders', {'name': 'orders', 'columns': [], 'types': [], 'rows': []})

        tables = storage.list_tables()

        assert set(tables) == {'users', 'products', 'orders'}

    def test_delete_table(self, temp_db_dir):
        """
        Test deleting a table
        """
        storage = Storage(temp_db_dir)

        # Save a table
        storage.save_table('temp_table', {'name': 'temp_table', 'columns': [], 'types': [], 'rows': []})

        # Verify it exists
        assert 'temp_table' in storage.list_tables()

        # Delete it
        result = storage.delete_table('temp_table')

        assert result is True
        assert 'temp_table' not in storage.list_tables()

    def test_delete_nonexistent_table(self, temp_db_dir):
        """
        Test deleting a table that does not exist
        """
        storage = Storage(temp_db_dir)

        result = storage.delete_table('nonexistent_table')

        assert result is False