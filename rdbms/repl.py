"""Simple REPL interface for the RDBMS."""

from typing import Dict, Optional, Any, List

from rdbms.parser import SQLParser, ParsedCommand
from rdbms.table import Table
from rdbms.storage import Storage


class Database:
    """Database manager that handles multiple tables."""

    def __init__(self, db_path: str = 'data'):
        self.storage = Storage(db_path)
        self.tables: Dict[str, Table] = {}
        self.parser = SQLParser()
        self._load_tables()

    def _load_tables(self) -> None:
        """Load all existing tables from storage."""
        for table_name in self.storage.list_tables():
            table_data = self.storage.load_table(table_name)
            if table_data:
                self.tables[table_name] = Table.from_dict(table_data)

    def _save_table(self, table_name: str) -> bool:
        """Save a table to storage."""
        if table_name in self.tables:
            return self.storage.save_table(table_name, self.tables[table_name].to_dict())
        return False

    def execute(self, sql: str) -> Optional[Any]:
        """Execute a SQL command and return the result."""
        cmd = self.parser.parse(sql)

        if cmd.command_type == 'CREATE_TABLE':
            if cmd.table_name in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' already exists")
            table = Table(cmd.table_name, cmd.columns, cmd.types,
                         primary_key=cmd.primary_key,
                         unique_constraints=cmd.unique_constraints or [])
            self.tables[cmd.table_name] = table
            self._save_table(cmd.table_name)
            return f"Table '{cmd.table_name}' created"

        elif cmd.command_type == 'DROP_TABLE':
            if cmd.table_name not in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' does not exist")
            del self.tables[cmd.table_name]
            self.storage.delete_table(cmd.table_name)
            return f"Table '{cmd.table_name}' dropped"

        elif cmd.command_type == 'INSERT':
            if cmd.table_name not in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' does not exist")
            self.tables[cmd.table_name].insert(cmd.values)
            self._save_table(cmd.table_name)
            return f"1 row inserted"

        elif cmd.command_type == 'SELECT':
            if cmd.table_name not in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' does not exist")
            return self.tables[cmd.table_name].select(columns=cmd.columns, where=cmd.where)

        elif cmd.command_type == 'UPDATE':
            if cmd.table_name not in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' does not exist")
            rows = self.tables[cmd.table_name].update(set_values=cmd.set_values, where=cmd.where)
            self._save_table(cmd.table_name)
            return f"{rows} row(s) updated"

        elif cmd.command_type == 'DELETE':
            if cmd.table_name not in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' does not exist")
            rows = self.tables[cmd.table_name].delete(where=cmd.where)
            self._save_table(cmd.table_name)
            return f"{rows} row(s) deleted"

        elif cmd.command_type == 'JOIN':
            if cmd.table_name not in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' does not exist")
            if cmd.join_table not in self.tables:
                raise ValueError(f"Table '{cmd.join_table}' does not exist")
            return self.tables[cmd.table_name].join(
                self.tables[cmd.join_table], cmd.left_column, cmd.right_column)

        elif cmd.command_type == 'CREATE_INDEX':
            if cmd.table_name not in self.tables:
                raise ValueError(f"Table '{cmd.table_name}' does not exist")
            self.tables[cmd.table_name].create_index(cmd.column_name)
            self._save_table(cmd.table_name)
            return f"Index created on '{cmd.column_name}'"

        else:
            raise ValueError(f"Unsupported command: {cmd.command_type}")


class REPL:
    """Interactive REPL interface."""

    def __init__(self, db_path: str = 'data'):
        self.db = Database(db_path)

    def run(self) -> None:
        """Start the REPL loop."""
        print("Simple RDBMS - Type SQL commands or .exit to quit")
        print("-" * 50)

        while True:
            try:
                sql = input("rdbms> ").strip()

                if not sql:
                    continue

                if sql == '.exit':
                    print("Goodbye!")
                    break

                if sql == '.tables':
                    tables = list(self.db.tables.keys())
                    print(f"Tables: {', '.join(tables) if tables else 'None'}")
                    continue

                result = self.db.execute(sql)

                if isinstance(result, list):
                    # Query result
                    if result:
                        for row in result:
                            print(row)
                    else:
                        print("No results")
                else:
                    # Status message
                    print(result)

            except KeyboardInterrupt:
                print("\nUse .exit to quit")
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")


if __name__ == '__main__':
    repl = REPL()
    repl.run()