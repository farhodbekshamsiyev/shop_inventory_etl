import pandas as pd

from etl.db import get_connection
from etl.etl_pipeline import run_etl


def create_test_csvs(tmp_path, catalogs=None, products=None):
    catalogs_csv = tmp_path / "catalogs.csv"
    products_csv = tmp_path / "products.csv"
    catalogs_df = pd.DataFrame(catalogs or {
        "catalog_id": [1, 2],
        "name": ["electronics", "clothing"],
        "created_at": ["2024-01-01", "2024-01-02"],
        "updated_at": ["2024-01-03", "2024-01-04"]
    })
    products_df = pd.DataFrame(products or {
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
    """Test that ETL loads both catalogs and products tables with correct row counts."""
    catalogs_csv, products_csv = create_test_csvs(tmp_path)
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    catalogs = pd.read_sql("SELECT * FROM catalogs", conn)
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert len(catalogs) == 2
    assert len(products) == 2


def test_product_name_normalization(tmp_path):
    """Test that product names are normalized (stripped and title-cased)."""
    catalogs_csv, products_csv = create_test_csvs(tmp_path)
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert products.loc[0, "name"] == "Phone"
    assert products.loc[1, "name"] == "Shirt"


def test_missing_price_filled(tmp_path):
    """Test that missing product prices are filled with the average price."""
    catalogs_csv, products_csv = create_test_csvs(tmp_path)
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert pd.notna(products.loc[1, "price"])
    avg_price = products["price"].mean()
    assert products.loc[1, "price"] == avg_price


def test_etl_with_empty_catalogs(tmp_path):
    """Test ETL with empty catalogs CSV."""
    catalogs_csv, products_csv = create_test_csvs(tmp_path, catalogs={})
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    catalogs = pd.read_sql("SELECT * FROM catalogs", conn)
    conn.close()
    assert catalogs.empty


def test_etl_with_empty_products(tmp_path):
    """Test ETL with empty products CSV."""
    catalogs_csv, products_csv = create_test_csvs(tmp_path, products={})
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert products.empty


def test_etl_with_invalid_price(tmp_path):
    """Test ETL handles non-numeric price values gracefully."""
    products = {
        "product_id": [10],
        "name": ["Test"],
        "price": ["not_a_number"],
        "catalog_id": [1],
        "created_at": ["2024-01-05"],
        "updated_at": ["2024-01-07"]
    }
    catalogs_csv, products_csv = create_test_csvs(tmp_path, products=products)
    run_etl(catalogs_csv, products_csv)
    conn = get_connection()
    products = pd.read_sql("SELECT * FROM products", conn)
    conn.close()
    assert pd.isna(products.loc[0, "price"])
