# Shop Inventory ETL

This project is an ETL (Extract, Transform, Load) pipeline for managing shop inventory data using Python.

## Features
- Extracts data from CSV files (catalogs, products)
- Transforms and cleans inventory data
- Loads data into a SQLite database (`etl_data.db`)
- Logging of ETL operations (`etl.log`)

## Project Structure
- `etl/` - ETL pipeline source code
  - `app.py` - Main entry point
  - `etl_pipeline.py` - ETL logic
  - `db.py` - Database operations
  - `logger.py` - Logging setup
- `data/` - Source CSV files
- `etl_data.db` - SQLite database
- `etl.log` - Log file

## Getting Started
1. Install dependencies (using [uv](https://github.com/astral-sh/uv)):
   ```bash
   uv pip install -r requirements.txt
   ```
2. Run the ETL pipeline:
   ```bash
   uv run streamlit run etl/app.py
   ```

## Requirements
- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (for dependency management)
- See `pyproject.toml` for dependencies

## License
MIT
