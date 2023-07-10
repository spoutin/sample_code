"""Unit tests for the important functions on app.main"""
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
from pandas import DataFrame

from app.main import (
    init_auldata_leak_reporting_table,
    get_auldata_subscribers,
    compare_auldata,
    run_compare_on_node,
    cleanup_auldata_leak_reporting_table,
)


class TestInitAuldataLeakReportingTable(unittest.TestCase):
    """Test the init_auldata_leak_reporting_table method"""

    def test_init_auldata_leak_reporting_table(self):
        """Cross check with execute_query method calls"""
        # Create a mock MySQLClient object
        mock_client = MagicMock()

        # Call the function with the mock client
        init_auldata_leak_reporting_table(mock_client)

        # Assert that the execute_query method was called twice with the expected arguments
        self.assertEqual(mock_client.execute_query.call_count, 2)
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS",
            mock_client.execute_query.call_args_list[0][0][0],
        )
        self.assertIn(
            "CREATE INDEX idx_AUDITDATE",
            mock_client.execute_query.call_args_list[1][0][0],
        )


class TestGetAuldataSubscribers(unittest.TestCase):
    """Test the get_auldata_subscribers method"""

    def test_get_auldata_subscribers(self):
        """Cross check with run_mongo_query_agr method calls"""
        # Create a mock MongoCollection object
        mock_collection = MagicMock()

        # Set the return value of the run_mongo_query_agr method
        mock_collection.run_mongo_query_agr.return_value = "mock result"

        # Define the audit range start and end datetimes
        audit_range_start = datetime(2022, 1, 1, 0, 0, 0)
        audit_range_end = datetime(2022, 1, 1, 23, 59, 59)

        # Call the function with the mock collection and audit range start and end datetimes
        result = get_auldata_subscribers(
            mock_collection, audit_range_start, audit_range_end
        )

        # Assert that the run_mongo_query_agr method was called once with
        # the expected arguments
        self.assertEqual(mock_collection.run_mongo_query_agr.call_count, 1)
        self.assertIn(
            "$match", mock_collection.run_mongo_query_agr.call_args_list[0][0][0][0]
        )

        # Assert that the result is equal to the expected value
        self.assertEqual(result, "mock result")


class TestCompareAuldata(unittest.TestCase):
    """Test the compare_auldata method"""

    def test_compare_auldata(self):
        """Cross check with run_compare_on_node method calls"""
        # Create a mock MySQLClient object
        mock_client = MagicMock()

        # Create a mock DataFrame object
        mock_subs = DataFrame({"ban": [0, 1, 2]})

        # Create a mock run_compare_on_node function
        mock_run_compare_on_node = MagicMock()

        # Patch the run_compare_on_node function with the mock function
        with patch("app.main.run_compare_on_node", mock_run_compare_on_node):
            # Call the compare_auldata function with the mock subs and client
            compare_auldata(mock_subs, mock_client)

        # Assert that the run_compare_on_node function was called three
        # times with the expected arguments
        self.assertEqual(mock_run_compare_on_node.call_count, 3)
        self.assertEqual(mock_run_compare_on_node.call_args_list[0][0][0], "A")
        self.assertEqual(mock_run_compare_on_node.call_args_list[1][0][0], "B")
        self.assertEqual(mock_run_compare_on_node.call_args_list[2][0][0], "C")


class TestRunCompareOnNode(unittest.TestCase):
    """Test the run_compare_on_node method"""

    def test_run_compare_on_node(self):
        """Cross check with get_usage_collection method calls"""
        # Create a mock MySQLClient object
        mock_client = MagicMock()

        # Create a mock DataFrame object
        mock_data = {
            "effectiveDate": ["2022-01-01T00:00:00Z", "2022-01-02T00:00:00Z"],
            "expiryDate": ["2022-01-01T23:59:59Z", "2022-01-02T23:59:59Z"],
            "subscriberId": ["{'$eq': 'sample1'}", "{'$eq': 'sample2'}"],
        }
        mock_subs = DataFrame(mock_data)

        # Create a mock get_usage_collection function
        mock_get_usage_collection = MagicMock()

        # Set the return value of the run_mongo_query method of the mock collection
        mock_collection = MagicMock()
        mock_collection.run_mongo_query.return_value = DataFrame()
        mock_get_usage_collection.return_value = mock_collection

        # Patch the get_usage_collection function with the mock function
        with patch("app.main.get_usage_collection", mock_get_usage_collection):
            # Call the run_compare_on_node function with the node, mock subs, and mock client
            run_compare_on_node("A", mock_subs, mock_client)

        # Assert that the get_usage_collection function was called once
        # with the expected arguments
        self.assertEqual(mock_get_usage_collection.call_count, 1)
        self.assertEqual(mock_get_usage_collection.call_args_list[0][0][0], "A")

        # Assert that the execute_query method of the mock client was not called
        self.assertEqual(mock_client.execute_query.call_count, 0)

    def test_run_compare_on_node_empty(self):
        """Cross check with get_usage_collection method calls
        but with an empty Subscribers List"""
        # Create a mock MySQLClient object
        mock_client = MagicMock()

        # Create a mock empty DataFrame object
        mock_subs = DataFrame()

        # Create a mock get_usage_collection function
        mock_get_usage_collection = MagicMock()

        # Set the return value of the run_mongo_query method of the mock collection
        mock_collection = MagicMock()
        mock_collection.run_mongo_query.return_value = DataFrame()
        mock_get_usage_collection.return_value = mock_collection

        # Patch the get_usage_collection function with the mock function
        with patch("app.main.get_usage_collection", mock_get_usage_collection):
            # Call the run_compare_on_node function with the node, mock subs,
            # and mock client
            run_compare_on_node("A", mock_subs, mock_client)

        # Assert that the get_usage_collection function was not called
        self.assertEqual(mock_get_usage_collection.call_count, 0)


class TestAuldataLeakReportingTableCleanup(unittest.TestCase):
    """Test the cleanup_auldata_leak_reporting_table method"""

    def test_auldata_leak_reporting_table_cleanup(self):
        """Cross check with execute_query method calls"""
        # Create a mock MySQLClient object
        mock_client = MagicMock()

        # Call the auldata_leak_reporting_table_cleanup function with the mock client
        cleanup_auldata_leak_reporting_table(mock_client)

        # Assert that the execute_query method of the mock client was called once
        # with the expected arguments
        self.assertEqual(mock_client.execute_query.call_count, 1)
        self.assertIn("DELETE FROM", mock_client.execute_query.call_args_list[0][0][0])
