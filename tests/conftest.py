from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_mongo_client():
    with patch("create_report.utils.MongoClient") as mock:
        yield mock


@pytest.fixture(autouse=True)
def mock_mysql_client():
    with patch("create_report.utils.create_engine") as mock:
        yield mock


@pytest.fixture(autouse=True)
def mock_pandas_dataframe():
    with patch("create_report.utils.pd.DataFrame") as mock:
        yield mock
