from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def mock_mongo_client():
    mock_mongo_client = patch("sample_code.dao._base_mongo.MongoClient")
    yield mock_mongo_client.start()
    mock_mongo_client.stop()


@pytest.fixture(autouse=True)
def mock_usage_mongo_run_agg_query():
    mock_run_query = patch("sample_code.dao.audit.AuditDAO.run_aggregation_query")
    yield mock_run_query.start()
    mock_run_query.stop()


@pytest.fixture(autouse=True)
def mock_usage_mongo_run_query():
    mock_run_query = patch("sample_code.dao.usage.UsageDAO.run_query")
    yield mock_run_query.start()
    mock_run_query.stop()


@pytest.fixture(autouse=True)
def mock_report_mysql_client():
    mock_mysql_client = patch("sample_code.dao.reporting.create_engine")
    yield mock_mysql_client.start()
    mock_mysql_client.stop()


@pytest.fixture()
def mock_main_run_compare_on_node():
    mock_compare_on_node = patch("sample_code.main.Main.run_compare_on_node")
    yield mock_compare_on_node.start()
    mock_compare_on_node.stop()
