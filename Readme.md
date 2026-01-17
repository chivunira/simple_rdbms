# Simple RDBMS - A Lightweight Relational Database Management System

A educational implementation of a relational database management system built from scratch in Python, featuring SQL-like query support, REPL interface, and REST API.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture & Design](#architecture--design)
- [Project Structure](#project-structure)
- [Installation & Setup](#installation--setup)
- [Usage Guide](#usage-guide)
  - [REPL Interface](#repl-interface)
  - [REST API](#rest-api)
- [Testing](#testing)
- [Future Enhancements](#future-enhancements)
- [Contributing](#contributing)

## Overview

This project implements a functional relational database management system with the following capabilities:

- **SQL Parser**: Parses SQL-like commands into executable operations
- **Storage Engine**: JSON-based persistence layer for data storage
- **Table Engine**: Handles CRUD operations with constraint enforcement
- **REPL Interface**: Interactive command-line interface for database operations
- **REST API**: FastAPI-based web service for remote database access

**Purpose**: Educational tool for understanding database internals and systems programming.

## Features

### Implemented Features

#### SQL Operations
- `CREATE TABLE` with column definitions and constraints
- `INSERT INTO` with type validation
- `SELECT` with column projection and WHERE filtering
- `UPDATE` with conditional updates
- `DELETE` with conditional deletion
- `DROP TABLE` for table removal
- `CREATE INDEX` for query optimization
- `JOIN` operations (INNER JOIN)

#### Data Types
- `INT` - Integer values
- `TEXT` - String values
- `FLOAT` - Floating-point numbers
- `BOOL` - Boolean values (true/false)

#### Constraints
- `PRIMARY KEY` - Unique identifier with automatic enforcement
- `UNIQUE` - Ensures column values are unique
- Type validation on all operations
- Referential integrity checks

#### Advanced Features
- **Hash-based Indexing**: O(1) lookups for indexed columns
- **Persistent Storage**: JSON file-based storage with automatic save/load
- **REPL Interface**: Interactive SQL command execution
- **REST API**: Full-featured web API with Swagger documentation
- **Error Handling**: Comprehensive validation and error messages

---

## Architecture & Design

### System Components

```
┌─────────────────────────────────────────────────────────┐
│                   Client Interfaces                     │
│         ┌──────────────┐      ┌──────────────┐          │
│         │  REPL CLI    │      │  REST API    │          │
│         │  (Terminal)  │      │  (FastAPI)   │          │
│         └──────┬───────┘      └───────┬──────┘          │
└────────────────┼──────────────────────┼─────────────────┘
                 │                      │
                 ▼                      ▼
         ┌───────────────────────────────────────┐
         │        Database Engine                │
         │   (Command Execution & Validation)    │
         └───────────────┬───────────────────────┘
                         │
            ┌────────────┼────────────┐
            ▼            ▼            ▼
    ┌───────────┐  ┌──────────┐  ┌──────────┐
    │  Parser   │  │  Table   │  │ Storage  │
    │  (SQL)    │  │ (CRUD)   │  │ (JSON)   │
    └───────────┘  └──────────┘  └──────────┘
```

### Design Decisions

#### 1. **Parser Design**
- **Approach**: Regex-based SQL parsing
- **Rationale**: Simple to implement, sufficient for basic SQL operations
- **Trade-off**: Limited support for complex queries (no nested queries, no AND/OR in WHERE)

#### 2. **Storage Design**
- **Approach**: JSON file per table
- **Rationale**: Human-readable, easy debugging, Python-native support
- **Trade-off**: Not optimized for large datasets, no transaction logging

#### 3. **Indexing Strategy**
- **Approach**: Hash-based indexes (Python dictionaries)
- **Rationale**: O(1) lookups, simple implementation
- **Trade-off**: No support for range queries, memory overhead

#### 4. **Constraint Enforcement**
- **Approach**: In-memory validation during operations
- **Rationale**: Immediate feedback, prevents invalid states
- **Trade-off**: Performance overhead on large operations

#### 5. **Type System**
- **Approach**: Strict type checking with Python type validation
- **Rationale**: Prevents data corruption, clear error messages
- **Trade-off**: No automatic type coercion

## Project Structure

```
simple_rdbms/
│
├── rdbms/                      # Core database package
│   ├── __init__.py
│   ├── parser.py               # SQL parser (regex-based)
│   ├── storage.py              # JSON storage layer
│   ├── table.py                # Table engine with CRUD operations
│   └── repl.py                 # REPL interface & database engine
│
├── tests/                      # Test suite
│   ├── test_parser.py          # Parser tests
│   ├── test_storage.py         # Storage tests
│   ├── test_table.py           # Table tests
│   ├── test_insert.py          # INSERT operation tests
│   ├── test_select.py          # SELECT operation tests
│   ├── test_update.py          # UPDATE operation tests
│   ├── test_delete.py          # DELETE operation tests
│   ├── test_join.py            # JOIN operation tests
│   ├── test_primary_key.py     # PRIMARY KEY constraint tests
│   ├── test_unique.py          # UNIQUE constraint tests
│   ├── test_indexing.py        # Indexing tests
│   ├── test_repl.py            # REPL interface tests
│   └── test_integration.py     # End-to-end integration tests
│
├── webapp/                     # FastAPI web application
│   └── api.py                  # REST API implementation
│
├── data/                       # Database files (auto-generated)
│   └── *.json                  # Table data files
│
├── .gitignore                  # Git ignore rules
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## Installation & Setup

### Prerequisites

- Python 3.9 or higher
- pip (Python package manager)

### Step 1: Clone the Repository

```bash
git clone <your-repo-url>
cd simple_rdbms
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python -m venv .venv

# Activate virtual environment
# On Windows:
.venv\Scripts\activate

# On macOS/Linux:
source .venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

## Usage Guide

### REPL Interface

The REPL (Read-Eval-Print Loop) provides an interactive command-line interface for executing SQL commands.

#### Starting the REPL

```bash
# Run the script directly
python -m rdbms.repl
```

#### Basic Commands

```sql
-- Create a table
rdbms> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT UNIQUE, age INT)
Table 'users' created

-- Insert data
rdbms> INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 30)
1 row inserted

rdbms> INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 25)
1 row inserted

-- Query all data
rdbms> SELECT * FROM users
[1, 'Alice', 'alice@example.com', 30]
[2, 'Bob', 'bob@example.com', 25]

-- Query specific columns
rdbms> SELECT name, age FROM users
['Alice', 30]
['Bob', 25]

-- Query with filtering
rdbms> SELECT * FROM users WHERE name = 'Alice'
[1, 'Alice', 'alice@example.com', 30]

-- Update data
rdbms> UPDATE users SET age = 31 WHERE name = 'Alice'
1 row(s) updated

-- Delete data
rdbms> DELETE FROM users WHERE name = 'Bob'
1 row(s) deleted

-- Create an index
rdbms> CREATE INDEX ON users (email)
Index created on 'email'

-- Drop a table
rdbms> DROP TABLE users
Table 'users' dropped
```

#### Special Commands

```bash
.tables   # List all tables
.exit     # Exit the REPL
```

#### Example Workflow: Employee Management

```sql
-- 1. Create tables
rdbms> CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)
rdbms> CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT, salary FLOAT)

-- 2. Insert data
rdbms> INSERT INTO departments VALUES (1, 'Engineering')
rdbms> INSERT INTO departments VALUES (2, 'Sales')

rdbms> INSERT INTO employees VALUES (1, 'Alice', 1, 75000.0)
rdbms> INSERT INTO employees VALUES (2, 'Bob', 2, 65000.0)
rdbms> INSERT INTO employees VALUES (3, 'Charlie', 1, 80000.0)

-- 3. Query with JOIN
rdbms> SELECT * FROM employees JOIN departments ON employees.dept_id = departments.id
[1, 'Alice', 1, 75000.0, 1, 'Engineering']
[2, 'Bob', 2, 65000.0, 2, 'Sales']
[3, 'Charlie', 1, 80000.0, 1, 'Engineering']

-- 4. Update salaries
rdbms> UPDATE employees SET salary = 78000.0 WHERE name = 'Alice'

-- 5. List all tables
rdbms> .tables
Tables: departments, employees

-- 6. Exit
rdbms> .exit
Goodbye!
```

### REST API

The FastAPI-based REST API provides programmatic access to the database over HTTP.

#### Starting the API Server

```bash
cd webapp
python api.py
```

The server will start at: **http://localhost:8000**

#### Interactive Documentation

FastAPI automatically generates interactive API documentation:

- **Swagger UI**: http://localhost:8000/docs

#### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/` | API information |
| `GET` | `/tables` | List all tables |
| `POST` | `/tables` | Create a new table |
| `GET` | `/tables/{name}` | Get table schema information |
| `DELETE` | `/tables/{name}` | Drop a table |
| `POST` | `/rows` | Insert a row |
| `POST` | `/query` | Query table data |
| `PUT` | `/rows` | Update rows |
| `DELETE` | `/rows` | Delete rows |

#### Testing with Swagger UI

1. **Open Swagger UI**: Navigate to http://localhost:8000/docs

2. **Create a Table**:
   - Click on `POST /tables`
   - Click "Try it out"
   - Enter request body:
   ```json
   {
     "name": "products",
     "columns": ["id", "name", "price", "in_stock"],
     "types": ["INT", "TEXT", "FLOAT", "BOOL"],
     "primary_key": "id"
   }
   ```
   - Click "Execute"

3. **Insert Data**:
   - Click on `POST /rows`
   - Click "Try it out"
   - Enter request body:
   ```json
   {
     "table_name": "products",
     "values": [1, "Laptop", 999.99, true]
   }
   ```
   - Click "Execute"

4. **Query Data**:
   - Click on `POST /query`
   - Click "Try it out"
   - Enter request body:
   ```json
   {
     "table_name": "products"
   }
   ```
   - Click "Execute"

5. **Update Data**:
   - Click on `PUT /rows`
   - Click "Try it out"
   - Enter request body:
   ```json
   {
     "table_name": "products",
     "set_values": {"price": 899.99},
     "where": {"id": 1}
   }
   ```
   - Click "Execute"

#### Testing with cURL

```bash
# List tables
curl http://localhost:8000/tables

# Create table
curl -X POST http://localhost:8000/tables \
  -H "Content-Type: application/json" \
  -d '{
    "name": "users",
    "columns": ["id", "name", "email"],
    "types": ["INT", "TEXT", "TEXT"],
    "primary_key": "id"
  }'

# Insert row
curl -X POST http://localhost:8000/rows \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "users",
    "values": [1, "Alice", "alice@example.com"]
  }'

# Query data
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "users"
  }'

# Update row
curl -X PUT http://localhost:8000/rows \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "users",
    "set_values": {"email": "alice.new@example.com"},
    "where": {"id": 1}
  }'

# Delete row
curl -X DELETE http://localhost:8000/rows \
  -H "Content-Type: application/json" \
  -d '{
    "table_name": "users",
    "where": {"id": 1}
  }'
```

#### Response Format

**Success Response:**
```json
{
  "message": "Table 'users' created",
  "table_name": "users"
}
```

**Query Response:**
```json
{
  "data": [
    {"id": 1, "name": "Alice", "email": "alice@example.com"},
    {"id": 2, "name": "Bob", "email": "bob@example.com"}
  ],
  "count": 2
}
```

**Error Response:**
```json
{
  "detail": "Table 'users' already exists"
}
```

## Testing

### Running Tests

```bash
# Run all tests
pytest -v

# Run specific test file
pytest tests/test_repl.py -v

# Run integration tests
pytest tests/test_integration.py -v

# Run with coverage
pytest --cov=rdbms tests/
```

### Test Coverage

The project includes comprehensive test coverage:

- **Unit Tests**: 100+ tests covering individual components
  - Parser tests (SQL parsing)
  - Storage tests (file I/O)
  - Table tests (CRUD operations)
  - Constraint tests (PRIMARY KEY, UNIQUE)
  - Index tests (creation, maintenance)

- **Integration Tests**: 10 end-to-end workflow tests
  - Multi-table operations
  - JOIN operations
  - Data persistence
  - Error recovery
  - Constraint enforcement

## Future Enhancements

### Recommended Additions

These features would make excellent contributions or learning projects:

#### 1. **Query Enhancements**
- [ ] Complex WHERE clauses (AND, OR, NOT)
- [ ] Comparison operators (<, >, <=, >=, !=)
- [ ] LIKE operator for pattern matching
- [ ] ORDER BY for sorting results
- [ ] LIMIT and OFFSET for pagination
- [ ] GROUP BY and aggregate functions (COUNT, SUM, AVG)

#### 2. **Advanced SQL Features**
- [ ] LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN
- [ ] Subqueries and nested SELECT statements
- [ ] DISTINCT keyword
- [ ] Multiple column WHERE conditions
- [ ] IN operator for multiple values

#### 3. **Performance Optimizations**
- [ ] B-tree indexes for range queries
- [ ] Query optimization and execution plans
- [ ] Caching layer for frequently accessed data
- [ ] Binary storage format (instead of JSON)
- [ ] Lazy loading for large tables

#### 4. **Database Features**
- [ ] Transactions (BEGIN, COMMIT, ROLLBACK)
- [ ] ACID compliance
- [ ] Foreign key constraints
- [ ] CASCADE operations
- [ ] Views (virtual tables)
- [ ] Stored procedures

#### 5. **Storage & Reliability**
- [ ] Write-ahead logging (WAL)
- [ ] Backup and restore functionality
- [ ] Data compression
- [ ] Multi-version concurrency control (MVCC)

#### 6. **Developer Experience**
- [ ] Query auto-complete in REPL
- [ ] Syntax highlighting
- [ ] Query execution time display
- [ ] EXPLAIN command for query plans
- [ ] Schema migration tools

#### 7. **API Enhancements**
- [ ] Authentication and authorization
- [ ] Rate limiting
- [ ] WebSocket support for real-time updates
- [ ] Batch operations endpoint
- [ ] GraphQL interface

## Contributing

Contributions are welcome! Here's how you can help:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/your-feature`
3. **Write tests** for your changes
4. **Ensure all tests pass**: `pytest -v`
5. **Commit your changes**: `git commit -m "Add your feature"`
6. **Push to your fork**: `git push origin feature/your-feature`
7. **Create a Pull Request**

### Development Guidelines

- Write tests for new features
- Update documentation for API changes
- Keep commits atomic and well-described

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|----------------|-------|
| INSERT | O(n) | n = constraint checks |
| SELECT (no index) | O(n) | Full table scan |
| SELECT (with index) | O(1) | Hash index lookup |
| UPDATE | O(n) | Must find matching rows |
| DELETE | O(n) | Must find matching rows |
| JOIN | O(n*m) | Nested loop join |
| CREATE INDEX | O(n) | Must scan all rows |


## Known Limitations

These are intentional design decisions for simplicity:

1. **WHERE Clauses**: Only support simple equality (no AND/OR)
2. **JOIN Operations**: Only INNER JOIN with single condition
3. **Transactions**: Not supported
4. **Concurrency**: Single-threaded, no multi-user support
5. **Storage**: JSON format not optimized for large datasets
6. **Type Coercion**: Strict typing, no automatic conversions


## Acknowledgments

Built as a learning project to understand:
- Database internals and architecture
- SQL parsing and execution
- Storage engine design
- Index structures and optimization
- RESTful API design with FastAPI

**Note**: This is an educational implementation. For production use, consider established databases like PostgreSQL, MySQL, or SQLite.


**Happy Coding!! **