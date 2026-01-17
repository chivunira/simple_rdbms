from typing import List, Any, Dict, Optional
from enum import Enum


class DataType(Enum):
    """Supported data types in the RDBMS"""
    INT = "INT"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    BOOL = "BOOL"


class Table:
    """
    Represents a database table with schema and data.
    Handles column definitions, type validation, and data storage.
    """

    VALID_TYPES = {'INT', 'TEXT', 'FLOAT', 'BOOL'}

    def __init__(self, name: str, columns: List[str], types: List[str],
                 primary_key: Optional[str] = None,
                 unique_constraints: Optional[List[str]] = None):
        """
        Initialize a new table.

        Args:
            name: Name of the table
            columns: List of column names
            types: List of column types (must match VALID_TYPES)
            primary_key: Name of the primary key column (optional)
            unique_constraints: List of column names with unique constraints (optional)

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

        # Validate primary key column exists
        if primary_key is not None:
            if primary_key not in columns:
                raise ValueError(f"Primary key column '{primary_key}' does not exist in table")

        self.primary_key = primary_key

        # Validate unique constraint columns exist
        if unique_constraints is None:
            unique_constraints = []

        for col in unique_constraints:
            if col not in columns:
                raise ValueError(f"Unique constraint column '{col}' does not exist in table")

        self.unique_constraints: List[str] = unique_constraints
        self.indexes: Dict[str, Dict[Any, List[int]]] = {}

    def validate_value(self, value: Any, expected_type: str) -> bool:
        """
        Validate that a value matches the expected type.

        Args:
            value: The value to validate
            expected_type: Expected type string (INT, TEXT, FLOAT, BOOL)

        Returns:
            True if value matches type, False otherwise
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
        Validate that a row has correct number of values and correct types.

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
        Get the index of a column by name.

        Args:
            column_name: Name of the column

        Returns:
            Index of the column

        Raises:
            ValueError: If column doesn't exist
        """
        try:
            return self.columns.index(column_name)
        except ValueError:
            raise ValueError(f"Column '{column_name}' does not exist in table '{self.name}'")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert table to dictionary format for storage.

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
        Create a table from dictionary format.

        Args:
            data: Dictionary containing table data

        Returns:
            Table instance
        """
        table = cls(
            data['name'],
            data['columns'],
            data['types'],
            primary_key=data.get('primary_key'),
            unique_constraints=data.get('unique_constraints', [])
        )
        table.rows = data.get('rows', [])
        table.indexes = data.get('indexes', {})

        return table

    def insert(self, row: List[Any]) -> bool:
        """
        Insert a new row into the table.

        Args:
            row: List of values to insert

        Returns:
            True if insertion successful

        Raises:
            ValueError: If row validation fails
        """
        # Validate row length
        if len(row) != len(self.columns):
            raise ValueError(
                f"Expected {len(self.columns)} values, got {len(row)}"
            )

        # Validate each value type
        for i, (value, col_type, col_name) in enumerate(zip(row, self.types, self.columns)):
            if not self.validate_value(value, col_type):
                raise ValueError(
                    f"Invalid type for column '{col_name}': expected {col_type}, "
                    f"got {type(value).__name__}"
                )

        # Check primary key constraint
        if self.primary_key is not None:
            pk_idx = self.get_column_index(self.primary_key)
            pk_value = row[pk_idx]

            # Check for duplicate primary key
            for existing_row in self.rows:
                if existing_row[pk_idx] == pk_value:
                    raise ValueError(f"Duplicate primary key value: {pk_value}")

        # Check unique constraints
        for unique_col in self.unique_constraints:
            col_idx = self.get_column_index(unique_col)
            value = row[col_idx]

            # Check for duplicate unique value
            for existing_row in self.rows:
                if existing_row[col_idx] == value:
                    raise ValueError(f"Duplicate value for unique column '{unique_col}': {value}")

        # Add the row
        row_position = len(self.rows)
        self.rows.append(row)

        # Update indexes
        for col_name, index in self.indexes.items():
            col_idx = self.get_column_index(col_name)
            value = row[col_idx]

            if value not in index:
                index[value] = []
            index[value].append(row_position)

        return True

    def select(self, columns: Optional[List[str]] = None,
               where: Optional[Dict[str, Any]] = None) -> List[List[Any]]:
        """
        Select rows from the table.

        Args:
            columns: List of column names to return (None = all columns)
            where: Dictionary of column:value pairs for filtering (AND logic)

        Returns:
            List of rows matching the criteria

        Raises:
            ValueError: If column names are invalid
        """
        # If no columns specified, return all columns
        if columns is None:
            columns = self.columns.copy()

        # Validate all requested columns exist
        column_indices = []
        for col in columns:
            column_indices.append(self.get_column_index(col))

        # Filter rows based on WHERE conditions
        filtered_rows = []

        for row in self.rows:
            # Check if row matches WHERE conditions
            if where is not None:
                match = True
                for where_col, where_val in where.items():
                    col_idx = self.get_column_index(where_col)
                    if row[col_idx] != where_val:
                        match = False
                        break

                if not match:
                    continue

            # Extract requested columns from the row
            result_row = [row[idx] for idx in column_indices]
            filtered_rows.append(result_row)

        return filtered_rows

    def update(self, set_values: Dict[str, Any],
               where: Optional[Dict[str, Any]] = None) -> int:
        """
        Update rows in the table.

        Args:
            set_values: Dictionary of column:value pairs to update
            where: Dictionary of column:value pairs for filtering (None = update all)

        Returns:
            Number of rows updated

        Raises:
            ValueError: If validation fails
        """
        # Validate we have something to update
        if not set_values:
            raise ValueError("Must specify at least one column to update")

        # Validate all columns exist and types are correct
        update_indices = {}
        for col_name, new_value in set_values.items():
            col_idx = self.get_column_index(col_name)
            col_type = self.types[col_idx]

            # Validate type
            if not self.validate_value(new_value, col_type):
                raise ValueError(
                    f"Invalid type for column '{col_name}': expected {col_type}, "
                    f"got {type(new_value).__name__}"
                )

            update_indices[col_idx] = new_value

        # Validate WHERE columns if provided
        if where is not None:
            for where_col in where.keys():
                self.get_column_index(where_col)  # Just validate it exists

        # Check if updating primary key
        pk_idx = None
        if self.primary_key is not None and self.primary_key in set_values:
            pk_idx = self.get_column_index(self.primary_key)
            new_pk_value = set_values[self.primary_key]

            # Check for duplicate primary key in existing rows
            for row in self.rows:
                # Skip rows that will be updated
                should_update = True
                if where is not None:
                    for where_col, where_val in where.items():
                        col_idx = self.get_column_index(where_col)
                        if row[col_idx] != where_val:
                            should_update = False
                            break

                # If this row won't be updated and has the new PK value, it's a duplicate
                if not should_update and row[pk_idx] == new_pk_value:
                    raise ValueError(f"Duplicate primary key value: {new_pk_value}")

        # Check if updating unique constraint columns
        unique_checks = {}
        for unique_col in self.unique_constraints:
            if unique_col in set_values:
                col_idx = self.get_column_index(unique_col)
                new_value = set_values[unique_col]
                unique_checks[col_idx] = (unique_col, new_value)

        # Update matching rows
        rows_updated = 0
        updated_pk_values = set()
        updated_unique_values = {col_idx: set() for col_idx in unique_checks.keys()}

        for row in self.rows:
            # Check if row matches WHERE conditions
            if where is not None:
                match = True
                for where_col, where_val in where.items():
                    col_idx = self.get_column_index(where_col)
                    if row[col_idx] != where_val:
                        match = False
                        break

                if not match:
                    continue

            # If updating primary key, check for duplicates among updated rows
            if pk_idx is not None:
                new_pk_value = update_indices[pk_idx]
                if new_pk_value in updated_pk_values:
                    raise ValueError(f"Duplicate primary key value: {new_pk_value}")
                updated_pk_values.add(new_pk_value)

            # If updating unique columns, check for duplicates
            for col_idx, (col_name, new_value) in unique_checks.items():
                # Check against rows that won't be updated
                for other_row in self.rows:
                    should_update_other = True
                    if where is not None:
                        for where_col, where_val in where.items():
                            other_col_idx = self.get_column_index(where_col)
                            if other_row[other_col_idx] != where_val:
                                should_update_other = False
                                break

                    if not should_update_other and other_row[col_idx] == new_value:
                        raise ValueError(f"Duplicate value for unique column '{col_name}': {new_value}")

                # Check against already updated rows
                if new_value in updated_unique_values[col_idx]:
                    raise ValueError(f"Duplicate value for unique column '{col_name}': {new_value}")
                updated_unique_values[col_idx].add(new_value)

            # Update the row
            for col_idx, new_value in update_indices.items():
                row[col_idx] = new_value

            rows_updated += 1

        return rows_updated

    def delete(self, where: Optional[Dict[str, Any]] = None) -> int:
        """
        Delete rows from the table.

        Args:
            where: Dictionary of column:value pairs for filtering (None = delete all)

        Returns:
            Number of rows deleted

        Raises:
            ValueError: If column names are invalid
        """
        # Validate WHERE columns if provided
        if where is not None:
            for where_col in where.keys():
                self.get_column_index(where_col)  # Just validate it exists

        # Find rows to delete
        rows_to_keep = []
        rows_deleted = 0

        for row in self.rows:
            # Check if row matches WHERE conditions
            should_delete = True

            if where is not None:
                match = True
                for where_col, where_val in where.items():
                    col_idx = self.get_column_index(where_col)
                    if row[col_idx] != where_val:
                        match = False
                        break

                should_delete = match

            if should_delete:
                rows_deleted += 1
            else:
                rows_to_keep.append(row)

        # Replace rows with filtered list
        self.rows = rows_to_keep

        # Rebuild all indexes (simpler than tracking changes)
        for col_name in list(self.indexes.keys()):
            self.create_index(col_name)

        return rows_deleted

    def create_index(self, column_name: str) -> None:
        """
        Create a hash-based index on a column for fast lookups.

        Args:
            column_name: Name of the column to index

        Raises:
            ValueError: If column doesn't exist
        """
        col_idx = self.get_column_index(column_name)

        # Build the index: value -> list of row positions
        index: Dict[Any, List[int]] = {}

        for row_pos, row in enumerate(self.rows):
            value = row[col_idx]

            if value not in index:
                index[value] = []
            index[value].append(row_pos)

        self.indexes[column_name] = index

    def drop_index(self, column_name: str) -> None:
        """
        Drop an index on a column.

        Args:
            column_name: Name of the column

        Raises:
            ValueError: If index doesn't exist
        """
        if column_name not in self.indexes:
            raise ValueError(f"Index on column '{column_name}' does not exist")

        del self.indexes[column_name]

    def join(self, right_table: 'Table', left_column: str, right_column: str) -> List[List[Any]]:
        """
        Perform an INNER JOIN with another table.

        Args:
            right_table: The table to join with
            left_column: Column name from this table to join on
            right_column: Column name from right table to join on

        Returns:
            List of combined rows where join condition matches

        Raises:
            ValueError: If column names are invalid
        """
        # Validate columns exist
        left_col_idx = self.get_column_index(left_column)
        right_col_idx = right_table.get_column_index(right_column)

        # Perform nested loop join (simple but works)
        results = []

        for left_row in self.rows:
            left_value = left_row[left_col_idx]

            for right_row in right_table.rows:
                right_value = right_row[right_col_idx]

                # Check if values match
                if left_value == right_value:
                    # Combine rows: all columns from left + all columns from right
                    combined_row = left_row + right_row
                    results.append(combined_row)

        return results

    def __repr__(self) -> str:
        """String representation of the table"""
        return f"Table(name='{self.name}', columns={self.columns}, rows={len(self.rows)})"