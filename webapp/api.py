"""Simple FastAPI web application for the RDBMS."""

import sys
from pathlib import Path

# Add parent directory to path so we can import rdbms
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Any, Dict

from rdbms.repl import Database

app = FastAPI(title="Simple RDBMS API")

# Initialize database
db = Database('data')


# Pydantic models for request/response
class TableCreate(BaseModel):
    name: str
    columns: List[str]
    types: List[str]
    primary_key: Optional[str] = None
    unique_constraints: Optional[List[str]] = None


class RowInsert(BaseModel):
    table_name: str
    values: List[Any]


class RowUpdate(BaseModel):
    table_name: str
    set_values: Dict[str, Any]
    where: Optional[Dict[str, Any]] = None


class RowDelete(BaseModel):
    table_name: str
    where: Optional[Dict[str, Any]] = None


class QueryRequest(BaseModel):
    table_name: str
    columns: Optional[List[str]] = None
    where: Optional[Dict[str, Any]] = None


@app.get("/")
def read_root():
    """Root endpoint."""
    return {
        "message": "Simple RDBMS API",
        "endpoints": {
            "tables": "/tables",
            "create_table": "/tables (POST)",
            "insert": "/rows (POST)",
            "query": "/query (POST)",
            "update": "/rows (PUT)",
            "delete": "/rows (DELETE)"
        }
    }


@app.get("/tables")
def list_tables():
    """List all tables."""
    return {"tables": list(db.tables.keys())}


@app.post("/tables")
def create_table(table: TableCreate):
    """Create a new table."""
    try:
        sql = f"CREATE TABLE {table.name} ("
        col_defs = []

        for col, col_type in zip(table.columns, table.types):
            col_def = f"{col} {col_type}"

            if table.primary_key and col == table.primary_key:
                col_def += " PRIMARY KEY"

            if table.unique_constraints and col in table.unique_constraints:
                col_def += " UNIQUE"

            col_defs.append(col_def)

        sql += ", ".join(col_defs) + ")"

        result = db.execute(sql)
        return {"message": result, "table_name": table.name}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/tables/{table_name}")
def drop_table(table_name: str):
    """Drop a table."""
    try:
        result = db.execute(f"DROP TABLE {table_name}")
        return {"message": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/rows")
def insert_row(row: RowInsert):
    """Insert a row into a table."""
    try:
        # Format values for SQL
        formatted_values = []
        for val in row.values:
            if isinstance(val, str):
                formatted_values.append(f"'{val}'")
            elif isinstance(val, bool):
                formatted_values.append('true' if val else 'false')
            else:
                formatted_values.append(str(val))

        sql = f"INSERT INTO {row.table_name} VALUES ({', '.join(formatted_values)})"
        result = db.execute(sql)
        return {"message": result}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/query")
def query_table(query: QueryRequest):
    """Query a table with optional filtering."""
    try:
        # Build SELECT statement
        columns = ", ".join(query.columns) if query.columns else "*"
        sql = f"SELECT {columns} FROM {query.table_name}"

        if query.where:
            # Only support single WHERE condition for simplicity
            col, val = list(query.where.items())[0]
            if isinstance(val, str):
                sql += f" WHERE {col} = '{val}'"
            elif isinstance(val, bool):
                sql += f" WHERE {col} = {'true' if val else 'false'}"
            else:
                sql += f" WHERE {col} = {val}"

        result = db.execute(sql)

        # Get column names for response
        table = db.tables[query.table_name]
        if query.columns:
            headers = query.columns
        else:
            headers = table.columns

        # Format as list of dicts
        rows = []
        for row in result:
            rows.append(dict(zip(headers, row)))

        return {"data": rows, "count": len(rows)}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.put("/rows")
def update_rows(update: RowUpdate):
    """Update rows in a table."""
    try:
        # Build SET clause
        set_parts = []
        for col, val in update.set_values.items():
            if isinstance(val, str):
                set_parts.append(f"{col} = '{val}'")
            elif isinstance(val, bool):
                set_parts.append(f"{col} = {'true' if val else 'false'}")
            else:
                set_parts.append(f"{col} = {val}")

        sql = f"UPDATE {update.table_name} SET {', '.join(set_parts)}"

        if update.where:
            col, val = list(update.where.items())[0]
            if isinstance(val, str):
                sql += f" WHERE {col} = '{val}'"
            elif isinstance(val, bool):
                sql += f" WHERE {col} = {'true' if val else 'false'}"
            else:
                sql += f" WHERE {col} = {val}"

        result = db.execute(sql)
        return {"message": result}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.delete("/rows")
def delete_rows(delete: RowDelete):
    """Delete rows from a table."""
    try:
        sql = f"DELETE FROM {delete.table_name}"

        if delete.where:
            col, val = list(delete.where.items())[0]
            if isinstance(val, str):
                sql += f" WHERE {col} = '{val}'"
            elif isinstance(val, bool):
                sql += f" WHERE {col} = {'true' if val else 'false'}"
            else:
                sql += f" WHERE {col} = {val}"

        result = db.execute(sql)
        return {"message": result}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/tables/{table_name}")
def get_table_info(table_name: str):
    """Get table schema information."""
    if table_name not in db.tables:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    table = db.tables[table_name]
    return {
        "name": table.name,
        "columns": table.columns,
        "types": table.types,
        "primary_key": table.primary_key,
        "unique_constraints": table.unique_constraints,
        "row_count": len(table.rows)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)