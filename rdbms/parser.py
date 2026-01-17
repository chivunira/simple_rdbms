import re
from typing import Optional, List, Dict, Any
from dataclasses import dataclass


@dataclass
class ParsedCommand:
    """Represents a parsed SQL command"""
    command_type: str
    table_name: Optional[str] = None
    columns: Optional[List[str]] = None
    types: Optional[List[str]] = None
    values: Optional[List[Any]] = None
    where: Optional[Dict[str, Any]] = None
    set_values: Optional[Dict[str, Any]] = None
    primary_key: Optional[str] = None
    unique_constraints: Optional[List[str]] = None
    join_table: Optional[str] = None
    left_column: Optional[str] = None
    right_column: Optional[str] = None
    column_name: Optional[str] = None


class SQLParser:
    """
    Parses SQL-like commands into structured ParsedCommand objects.
    Supports: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, JOIN, DROP TABLE, CREATE INDEX
    """

    def parse(self, sql: str) -> ParsedCommand:
        """
        Parse a SQL command string.

        Args:
            sql: SQL command string

        Returns:
            ParsedCommand object

        Raises:
            ValueError: If SQL is invalid
        """
        # Clean up the SQL
        sql = sql.strip()

        if not sql:
            raise ValueError("Empty SQL command")

        # Normalize whitespace and make case-insensitive for keywords
        sql = ' '.join(sql.split())
        sql_upper = sql.upper()

        # Determine command type and parse accordingly
        if sql_upper.startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql_upper.startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql_upper.startswith('SELECT'):
            return self._parse_select(sql)
        elif sql_upper.startswith('UPDATE'):
            return self._parse_update(sql)
        elif sql_upper.startswith('DELETE FROM'):
            return self._parse_delete(sql)
        elif sql_upper.startswith('DROP TABLE'):
            return self._parse_drop_table(sql)
        elif sql_upper.startswith('CREATE INDEX'):
            return self._parse_create_index(sql)
        else:
            raise ValueError("Invalid SQL command")

    def _parse_create_table(self, sql: str) -> ParsedCommand:
        """Parse CREATE TABLE command"""
        # Pattern: CREATE TABLE table_name (col1 TYPE1 [PRIMARY KEY] [UNIQUE], col2 TYPE2, ...)
        match = re.match(r'CREATE TABLE (\w+) \((.*)\)', sql, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid CREATE TABLE syntax")

        table_name = match.group(1)
        columns_def = match.group(2)

        columns = []
        types = []
        primary_key = None
        unique_constraints = []

        # Split by comma (but be careful with nested stuff)
        col_defs = [c.strip() for c in columns_def.split(',')]

        for col_def in col_defs:
            parts = col_def.split()

            if len(parts) < 2:
                raise ValueError(f"Invalid column definition: {col_def}")

            col_name = parts[0]
            col_type = parts[1].upper()

            columns.append(col_name)
            types.append(col_type)

            # Check for PRIMARY KEY
            if 'PRIMARY' in col_def.upper() and 'KEY' in col_def.upper():
                primary_key = col_name

            # Check for UNIQUE
            if 'UNIQUE' in col_def.upper():
                unique_constraints.append(col_name)

        return ParsedCommand(
            command_type='CREATE_TABLE',
            table_name=table_name,
            columns=columns,
            types=types,
            primary_key=primary_key,
            unique_constraints=unique_constraints
        )

    def _parse_insert(self, sql: str) -> ParsedCommand:
        """Parse INSERT command"""
        # Pattern: INSERT INTO table_name VALUES (val1, val2, ...)
        match = re.match(r'INSERT INTO (\w+) VALUES \((.*)\)', sql, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid INSERT syntax")

        table_name = match.group(1)
        values_str = match.group(2)

        # Parse values
        values = self._parse_values(values_str)

        return ParsedCommand(
            command_type='INSERT',
            table_name=table_name,
            values=values
        )

    def _parse_select(self, sql: str) -> ParsedCommand:
        """Parse SELECT command (including JOIN)"""
        sql_upper = sql.upper()

        # Check if it's a JOIN
        if 'JOIN' in sql_upper:
            return self._parse_join(sql)

        # Pattern: SELECT columns FROM table [WHERE condition]
        # Extract columns
        select_match = re.match(r'SELECT (.*?) FROM (\w+)(.*)', sql, re.IGNORECASE)

        if not select_match:
            raise ValueError("Invalid SELECT syntax")

        columns_str = select_match.group(1).strip()
        table_name = select_match.group(2)
        rest = select_match.group(3).strip()

        # Parse columns
        if columns_str == '*':
            columns = None  # All columns
        else:
            columns = [c.strip() for c in columns_str.split(',')]

        # Parse WHERE clause if present
        where = None
        if rest.upper().startswith('WHERE'):
            where_clause = rest[5:].strip()  # Remove 'WHERE'
            where = self._parse_where(where_clause)

        return ParsedCommand(
            command_type='SELECT',
            table_name=table_name,
            columns=columns,
            where=where
        )

    def _parse_update(self, sql: str) -> ParsedCommand:
        """Parse UPDATE command"""
        # Pattern: UPDATE table SET col1=val1, col2=val2 [WHERE condition]
        match = re.match(r'UPDATE (\w+) SET (.*?)(?:WHERE (.*))?$', sql, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid UPDATE syntax")

        table_name = match.group(1)
        set_clause = match.group(2).strip()
        where_clause = match.group(3)

        # Parse SET clause
        set_values = {}
        set_parts = [s.strip() for s in set_clause.split(',')]

        for part in set_parts:
            if '=' not in part:
                raise ValueError(f"Invalid SET clause: {part}")

            col, val = part.split('=', 1)
            col = col.strip()
            val = val.strip()

            set_values[col] = self._parse_value(val)

        # Parse WHERE clause if present
        where = None
        if where_clause:
            where = self._parse_where(where_clause.strip())

        return ParsedCommand(
            command_type='UPDATE',
            table_name=table_name,
            set_values=set_values,
            where=where
        )

    def _parse_delete(self, sql: str) -> ParsedCommand:
        """Parse DELETE command"""
        # Pattern: DELETE FROM table [WHERE condition]
        match = re.match(r'DELETE FROM (\w+)(?:\s+WHERE (.*))?$', sql, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid DELETE syntax")

        table_name = match.group(1)
        where_clause = match.group(2)

        # Parse WHERE clause if present
        where = None
        if where_clause:
            where = self._parse_where(where_clause.strip())

        return ParsedCommand(
            command_type='DELETE',
            table_name=table_name,
            where=where
        )

    def _parse_join(self, sql: str) -> ParsedCommand:
        """Parse JOIN command"""
        # Pattern: SELECT * FROM table1 [INNER] JOIN table2 ON table1.col1 = table2.col2
        pattern = r'SELECT \* FROM (\w+) (?:INNER )?JOIN (\w+) ON (\w+)\.(\w+) = (\w+)\.(\w+)'
        match = re.match(pattern, sql, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid JOIN syntax")

        left_table = match.group(1)
        right_table = match.group(2)

        # The ON clause: table1.col1 = table2.col2
        left_table_in_on = match.group(3)
        left_col = match.group(4)
        right_table_in_on = match.group(5)
        right_col = match.group(6)

        return ParsedCommand(
            command_type='JOIN',
            table_name=left_table,
            join_table=right_table,
            left_column=left_col,
            right_column=right_col
        )

    def _parse_drop_table(self, sql: str) -> ParsedCommand:
        """Parse DROP TABLE command"""
        match = re.match(r'DROP TABLE (\w+)', sql, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid DROP TABLE syntax")

        return ParsedCommand(
            command_type='DROP_TABLE',
            table_name=match.group(1)
        )

    def _parse_create_index(self, sql: str) -> ParsedCommand:
        """Parse CREATE INDEX command"""
        # Pattern: CREATE INDEX ON table_name (column_name)
        match = re.match(r'CREATE INDEX ON (\w+) \((\w+)\)', sql, re.IGNORECASE)

        if not match:
            raise ValueError("Invalid CREATE INDEX syntax")

        return ParsedCommand(
            command_type='CREATE_INDEX',
            table_name=match.group(1),
            column_name=match.group(2)
        )

    def _parse_where(self, where_clause: str) -> Dict[str, Any]:
        """Parse WHERE clause into dictionary"""
        # Simple implementation: only supports col = value (no AND/OR for now)
        where = {}

        # For now, just support single condition
        if '=' not in where_clause:
            raise ValueError(f"Invalid WHERE clause: {where_clause}")

        col, val = where_clause.split('=', 1)
        col = col.strip()
        val = val.strip()

        where[col] = self._parse_value(val)

        return where

    def _parse_values(self, values_str: str) -> List[Any]:
        """Parse comma-separated values"""
        values = []

        # Split by comma, but respect quoted strings
        current = ''
        in_quotes = False

        for char in values_str:
            if char == "'" and (not current or current[-1] != '\\'):
                in_quotes = not in_quotes
                current += char
            elif char == ',' and not in_quotes:
                values.append(self._parse_value(current.strip()))
                current = ''
            else:
                current += char

        # Don't forget the last value
        if current.strip():
            values.append(self._parse_value(current.strip()))

        return values

    def _parse_value(self, val_str: str) -> Any:
        """Parse a single value (int, float, bool, or string)"""
        val_str = val_str.strip()

        # Boolean
        if val_str.lower() == 'true':
            return True
        elif val_str.lower() == 'false':
            return False

        # String (quoted)
        if val_str.startswith("'") and val_str.endswith("'"):
            return val_str[1:-1]  # Remove quotes

        # Try integer
        try:
            return int(val_str)
        except ValueError:
            pass

        # Try float
        try:
            return float(val_str)
        except ValueError:
            pass

        # If nothing works, treat as string
        return val_str