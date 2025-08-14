import streamlit as st
import pandas as pd
from db import get_connection
from etl_pipeline import run_etl
from logger import get_logger

logger = get_logger()

st.title("Simple ETL Pipeline")

# --- ETL ---
if st.button("Start ETL"):
    try:
        run_etl("data/catalogs.csv", "data/products.csv")
        st.success("✅ ETL completed! Data saved to SQLite.")
    except FileNotFoundError:
        st.error("❌ CSV file not found. Please check the file path.")
    except Exception as e:
        st.error(f"❌ ETL failed: {str(e)}")
        logger.error(f"ETL failed: {e}")

# --- Show Results ---
if st.button("Show Results"):
    try:
        conn = get_connection()
        query = """
        SELECT p.product_id,
               p.name AS product_name,
               p.price,
               c.name AS catalog_name,
               p.created_at,
               p.updated_at
        FROM products p
        LEFT JOIN catalogs c
        ON p.catalog_id = c.catalog_id
        """
        products = pd.read_sql(query, conn)
        catalogs = pd.read_sql("SELECT * FROM catalogs", conn)
        conn.close()

        st.subheader("Catalogs")
        st.dataframe(catalogs)

        st.subheader("Products (with Catalog Names)")
        st.dataframe(products)

    except Exception as e:
        st.error(f"❌ Failed to load results: {str(e)}")
        logger.error(f"Show results failed: {e}")

# --- Queries Section ---
st.header("📊 Data Queries")


# 1️⃣ Query products by catalog
st.subheader("1. Query products by catalog")
try:
    conn = get_connection()
    catalogs = pd.read_sql("SELECT DISTINCT name FROM catalogs", conn)
    catalog_names = catalogs['name'].tolist()
    selected_catalog = st.selectbox("Select catalog", catalog_names)

    if st.button("Get Products for Catalog"):
        query = """
        SELECT p.product_id,
               p.name AS product_name,
               p.price,
               c.name AS catalog_name
        FROM products p
        JOIN catalogs c ON p.catalog_id = c.catalog_id
        WHERE c.name = ?
        """
        df = pd.read_sql(query, conn, params=(selected_catalog,))
        st.dataframe(df)
except Exception as e:
    st.error(f"Failed to query products by catalog: {str(e)}")
finally:
    conn.close()

# 2️⃣ Get top N products by price
st.subheader("2. Top N products by price")
try:
    conn = get_connection()
    top_n = st.number_input("Enter N", min_value=1, value=5, step=1)
    if st.button("Get Top N Products"):
        query = """
        SELECT p.product_id,
               p.name AS product_name,
               p.price,
               c.name AS catalog_name
        FROM products p
        JOIN catalogs c ON p.catalog_id = c.catalog_id
        ORDER BY p.price DESC
        LIMIT ?
        """
        df = pd.read_sql(query, conn, params=(top_n,))
        st.dataframe(df)
except Exception as e:
    st.error(f"Failed to get top N products: {str(e)}")
finally:
    conn.close()

# 3️⃣ Product counts per catalog
st.subheader("3. Product counts per catalog")
try:
    conn = get_connection()
    if st.button("Get Product Counts"):
        query = """
        SELECT c.name AS catalog_name,
               COUNT(p.product_id) AS product_count
        FROM catalogs c
        LEFT JOIN products p ON c.catalog_id = p.catalog_id
        GROUP BY c.name
        ORDER BY product_count DESC
        """
        df = pd.read_sql(query, conn)
        st.dataframe(df)
except Exception as e:
    st.error(f"Failed to get product counts: {str(e)}")
finally:
    conn.close()