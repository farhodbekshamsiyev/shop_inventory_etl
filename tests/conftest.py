import os
import pytest

from etl.db import init_db


@pytest.fixture(autouse=True)
def clean_db():
    # Ensure DB is clean before each test
    if os.path.exists("etl_data.db"):
        os.remove("etl_data.db")
    init_db()
    yield
    if os.path.exists("etl_data.db"):
        os.remove("etl_data.db")

