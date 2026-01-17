import os
import json
from typing import Dict, List, Optional, Any


class Storage:
    """
    Handles persistence of database tables to disk using JSON files.
    Each table is stored as a separate JSON file.
    """

    def __init__(self, db_path: str = 'data'):
        """
        Initialize storage with a database directory path

        Args:
            db_path: Directory path where JSON files will be stored.
        """
        self.db_path = db_path

        # Create directory if it doesn't exist
        os.makedirs(self.db_path, exist_ok=True)

    def save_table(self, table_name: str, table_data: Dict[str, Any]) -> bool:
        """
        Save table data to a JSON file.

        Args:
            table_name: Name of the table.
            table_data: Dictionary containing table schema and data.

        Returns:
            True if successful, False otherwise.
        """
        try:
            table_file = os.path.join(self.db_path, f'{table_name}.json')

            with open(table_file, 'w') as f:
                json.dump(table_data, f, indent=2)

            return True

        except Exception as e:
            print(f"Error saving table {table_name}: {e}")
            return False

    def load_table(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Load table data from a JSON file.

        Args:
            table_name: Name of the table being loaded

        Returns:
            Dictionary containing table data, or None if the table does not exist.
        """
        try:
            table_file = os.path.join(self.db_path, f'{table_name}.json')

            if not os.path.exists(table_file):
                return None

            with open(table_file, 'r') as f:
                table_data = json.load(f)

            return table_data

        except Exception as e:
            print(f"Error loading table {table_name}: {e}")
            return None

    def list_tables(self) -> List[str]:
        """
        List all tables in database

        Returns:
            List of table names.
        """
        try:
            # Get all .json files in the database directory
            json_files = [f for f in os.listdir(self.db_path) if f.endswith('.json')]

            # Extract table names by removing the .json extension
            table_names = [f[:-5] for f in json_files]

            return table_names

        except Exception as e:
            print(f"Error listing tables: {e}")
            return []

    def delete_table(self, table_name: str) -> bool:
        """
        Delete a table file

        Args:
            table_name: Name of the table to delete

        Returns:
            True if successful, False otherwise.
        """
        try:
            table_file = os.path.join(self.db_path, f'{table_name}.json')

            if not os.path.exists(table_file):
                return False

            os.remove(table_file)
            return True

        except Exception as e:
            print(f"Error deleting table {table_name}: {e}")
            return False

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists

        Args:
            table_name: Name of the table

        Returns:
            True if table exists, False otherwise.
        """
        table_file = os.path.join(self.db_path, f'{table_name}.json')
        return os.path.exists(table_file)
