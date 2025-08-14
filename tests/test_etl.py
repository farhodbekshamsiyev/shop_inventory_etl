import os

import pandas as pd
import pytest

from etl.db import get_connection, init_db
from etl.etl_pipeline import run_etl


@pytest.fixture(autouse=True)
def clean_db():
    # Ensure DB is clean before each test
    if os.path.exists("etl_data.db"):
        os.remove("etl_data.db")
    init_db()
    yield
    if os.path.exists("etl_data.db"):
        os.remove("etl_data.db")


def create_test_csvs(tmp_path):
    catalogs_csv = tmp_path / "catalogs.csv"
    products_csv = tmp_path / "products.csv"
    catalogs_df = pd.DataFrame({
        "catalog_id": [1, 2],
        "name": ["electronics", "clothing"],
        "created_at": ["2024-01-01", "2024-01-02"],
        "updated_at": ["2024-01-03", "2024-01-04"]
    })
    products_df = pd.DataFrame({
        "product_id": [10, 11],
        "name": [" phone ", " SHIRT "],
        "price": [100, None],
        "catalog_id": [1, 2],
        "created_at": ["2024-01-05", "2024-01-06"],
        "updated_at": ["2024-01-07", "2024-01-08"]
    })
    catalogs_df.to_csv(catalogs_csv, index=False)
    products_df.to_csv(products_csv, index=False)
    return catalogs_csv, products_csv


def test_etl_loads_catalogs_and_products(tmp_path):
    catalogs_csv, products_csv = create_test_csvs(tmp_path)
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    catalogs = pd.read_sql("SELECT * FROM catalogs", conn)
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert len(catalogs) == 2
    assert len(products) == 2


def test_product_name_normalization(tmp_path):
    catalogs_csv, products_csv = create_test_csvs(tmp_path)
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert products.loc[0, "name"] == "Phone"


def test_missing_price_filled(tmp_path):
    catalogs_csv, products_csv = create_test_csvs(tmp_path)
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert pd.notna(products.loc[1, "price"])
