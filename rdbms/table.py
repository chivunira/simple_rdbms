from typing import List, Any, Dict, Optional
from enum import Enum


class DataType(Enum):
    """ Supported data types for table columns. """
    INT = "INT"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"


class Table:
    """
    Represents a db table with schema and data
    Handles column definitions, type validation and data storage
    """

    VALID_TYPES = {'INT', 'TEXT', 'FLOAT', 'BOOL'}

    def __init__(self, name: str, columns: List[str], types: List[str]):
        """
        Initialize a new table

        Args:
            name: Name of the table
            columns: List of column names
            types: List of data types corresponding to columns

        Raises:
            ValueError: If validation fails
        """
        # Validate table name
        if not name or name.strip() == '':
            raise ValueError("Table name cannot be empty")

        # Validate columns exist
        if len(columns) == 0:
            raise ValueError("Table must have at least one column")

        # Validate columns and types match
        if len(columns) != len(types):
            raise ValueError("Number of columns must match number of types")

        # Validate no duplicate column names
        if len(columns) != len(set(columns)):
            raise ValueError("Duplicate column name found")

        # Validate all types are valid
        for col_type in types:
            if col_type not in self.VALID_TYPES:
                raise ValueError(f"Invalid data type: {col_type}. Must be one of {self.VALID_TYPES}")

        self.name = name
        self.columns = columns
        self.types = types
        self.rows: List[List[Any]] = []

        # Additional schema attributes
        self.primary_key: Optional[str] = None
        self.unique_constraints: List[str] = []
        self.indexes: Dict[str, Dict[Any, List[int]]] = {}

    def validate_value(self, value: Any, expected_type: str) -> bool:
        """
        Validate that a value matches the expected type

        Args:
            value: The value to validate
            expected_type: The expected data type as a string

        Returns:
            True if value matches expected type, False otherwise
        """
        if expected_type == 'INT':
            return isinstance(value, int) and not isinstance(value, bool)

        elif expected_type == 'TEXT':
            return isinstance(value, str)

        elif expected_type == 'FLOAT':
            return isinstance(value, float) and not isinstance(value, bool)

        elif expected_type == 'BOOL':
            return isinstance(value, bool)

        return False

    def validate_row(self, row: List[Any]) -> bool:
        """
        Validate a row has correct number of values and correct types

        Args:
            row: List of values representing a row

        Returns:
            True if row is valid, False otherwise
        """
        # Check correct number of values
        if len(row) != len(self.columns):
            return False

        # Check each value matches its column type
        for value, col_type in zip(row, self.types):
            if not self.validate_value(value, col_type):
                return False

        return True

    def get_column_index(self, column_name: str) -> int:
        """
        Get the index of a column by name

        Args:
            column_name: Name of the column

        Returns:
            Index of the column

        Raises:
            ValueError: If column does not exist
        """
        try:
            return self.columns.index(column_name)
        except ValueError:
            raise ValueError(f"Column '{column_name}' does not exist in table '{self.name}'")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert table to dictionary format for storage

        Returns:
            Dictionary representation of the table
        """
        return {
            'name': self.name,
            'columns': self.columns,
            'types': self.types,
            'rows': self.rows,
            'primary_key': self.primary_key,
            'unique_constraints': self.unique_constraints,
            'indexes': self.indexes
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Table':
        """
        Create a table from dictionary format

        Args:
            data: Dictionary representation of the table

        Returns:
            Table instance
        """
        table = cls(data['name'], data['columns'], data['types'])
        table.rows = data.get('rows', [])
        table.primary_key = data.get('primary_key')
        table.unique_constraints = data.get('unique_constraints', [])
        table.indexes = data.get('indexes', {})

        return table

    def __repr__(self) -> str:
        """
        String representation of the table
        """
        return f"Table(name='{self.name}', columns={self.columns}, rows={len(self.rows)})"
    