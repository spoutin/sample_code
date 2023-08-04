from unittest.mock import MagicMock

from create_report.utils import (
    get_mongo_client,
    get_mysql_client,
    run_mongo_query,
    run_mongo_query_agr,
)


def test_get_mongo_client(mock_mongo_client) -> None:
    get_mongo_client(
        server="server",
        replicaset="replicaset",
        username="username",
        password="password",
    )

    mock_mongo_client.assert_called_once_with(
        "mongodb://username:password@server",
        replicaSet="replicaset",
        authSource="admin",
        authMechanism="SCRAM-SHA-1",
        readPreference="secondary",
    )


def test_get_mysql_client(mock_mysql_client) -> None:
    get_mysql_client()

    mock_mysql_client.assert_called_once_with(
        "mysql+pymysql://testuser:testpass@127.0.0.1:3306/testdb?charset=utf8",
        pool_recycle=3600,
    )


def test_run_mongo_query(mock_pandas_dataframe) -> None:
    collection = MagicMock()
    query = {}
    projection = {}

    run_mongo_query(collection, query, projection)

    collection.find.assert_called_once_with(filter=query, projection=projection)
    mock_pandas_dataframe.assert_called_once_with(list(collection.find()))


def test_run_mongo_query_agr(mock_pandas_dataframe) -> None:
    collection = MagicMock()
    pipeline = [{}]

    run_mongo_query_agr(collection, pipeline)

    collection.aggregate.assert_called_once_with(pipeline=[{}], cursor={})
    mock_pandas_dataframe.assert_called_once_with(list(collection.aggregate()))
