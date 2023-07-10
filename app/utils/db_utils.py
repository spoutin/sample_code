"""Classes and Methods for Database clients."""
from typing import Union, Optional, Any
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from pymongo import MongoClient
from pymongo import DESCENDING
from pandas import DataFrame


class MySQLClient:
    """MySQL Client Object."""

    def __init__(self, mysql_details: dict[str, str]) -> None:
        server = mysql_details["SERVER"]
        port = mysql_details["PORT"]
        database = mysql_details["DATABASE"]
        username = mysql_details["USERNAME"]
        password = mysql_details["PASSWORD"]

        mysql_uri = f"mysql://{username}:{password}@{server}:{port}\
                     /{database}?charset=utf8"
        self.client = create_engine(mysql_uri, pool_recycle=3600)

    def execute_query(self, query: str) -> Union[int, str]:
        """Execute a Query via the MySQL connection.

        Args:
            query (str): Query strng.

        Returns:
            Union[int, str]: 0 if successful. Error Message if not.
        """
        try:
            self.client.execute(query)
            return 0
        except SQLAlchemyError as sql_error:
            error = str(sql_error.__dict__["orig"])
            return error


class MongoCollection:
    """MongoDB Collection Object"""

    def __init__(
        self,
        mongodb_details: dict[str, str],
        mongodb_general_settings: dict[str, str],
    ) -> None:
        server = mongodb_details["SERVER"]
        replica_set = mongodb_details["REPLICASET"]
        username = mongodb_details["USERNAME"]
        password = mongodb_details["PASSWORD"]
        database = mongodb_details["DATABASE"]
        collection = mongodb_details["COLLECTION"]

        auth_source = mongodb_general_settings["AUTHSOURCE"]
        auth_mechanism = mongodb_general_settings["AUTHMECHANISM"]
        read_preference = mongodb_general_settings["READ_PREFERENCE"]

        mongo_uri = f"mongodb://{username}:{password}@{server}"
        self.client = MongoClient(
            mongo_uri,
            replicaSet=replica_set,
            authSource=auth_source,
            readPreference=read_preference,
            authMechanism=auth_mechanism,
        )
        self.database = self.client.get(database, None)

        if self.database is None:
            raise ValueError("Database {database} does not exist.")

        self.collection = self.database.get(collection, None)

        if self.collection is None:
            raise ValueError("Collection {collection} does not exist.")

    def run_mongo_query(
        self,
        query: Union[list[Any], dict[str, Any]],
        project: Optional[dict[str, Any]] = None,
        sort_field: Optional[str] = "eventTime",
        limit_count: Optional[int] = None,
    ) -> DataFrame:
        """Run a MongoDB Query on the Collection.

        Args:
            query (dict): MongoDB Query.
            project (dict, optional): Specify the fields to be returned. Defaults to None.
            sort_field (str, optional): Field to sort on. Defaults to "eventTime".
            limit_results (bool, optional): Flag to limit the results. Defaults to False.
            limit_count (int, optional): Maximum number of results. Defaults to 10.

        Returns:
            DataFrame: Query Result.
        """
        results = []
        if project is not None:
            db_query = self.collection.find(query, project)
        else:
            db_query = self.collection.find(query)
        if sort_field is not None:
            db_query.sort(sort_field, DESCENDING)
        if limit_count:
            db_query.limit(limit_count)
        for doc in db_query:
            results.append(doc)

        results_df = DataFrame(list(results))
        return results_df

    def run_mongo_query_agr(self, query: Union[list[Any], dict[str, Any]]) -> DataFrame:
        """Return an aggregated result from a MongoDB Query.

        Args:
            query (dict): MongoDB Query.

        Returns:
            DataFrame: Aggregated Query result.
        """
        results = self.collection.aggregate(query, cursor={})
        results_df = DataFrame(list(results))
        return results_df
