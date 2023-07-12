from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_audit_mongo_client():
    mock_mongo_client = patch("sample_code.dao.audit.MongoClient")
    yield mock_mongo_client.start()
    mock_mongo_client.stop()


@pytest.fixture(autouse=True)
def mock_usage_mongo_client():
    mock_mongo_client = patch("sample_code.dao.usage.MongoClient")
    yield mock_mongo_client.start()
    mock_mongo_client.stop()


@pytest.fixture(autouse=True)
def mock_usage_mongo_run_query():
    mock_mongo_client = patch("sample_code.dao.usage.UsageDAO.run_query")
    yield mock_mongo_client.start()
    mock_mongo_client.stop()


@pytest.fixture(autouse=True)
def mock_report_mysql_client():
    mock_mysql_client = patch("sample_code.dao.reporting.create_engine")
    yield mock_mysql_client.start()
    mock_mysql_client.stop()
