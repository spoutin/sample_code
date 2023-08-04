import logging
import sys
from datetime import date, datetime, tzinfo
from typing import TYPE_CHECKING

import pandas as pd
from dateutil import tz
from pymongo import DESCENDING, MongoClient
from pymongo.collection import Collection
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import text

from . import settings

if TYPE_CHECKING:
    from pymongo.cursor import Cursor
    from pymongo.database import Database

logger = logging.getLogger(__name__)


def get_mongo_client(
    server: str, replicaset: str, username: str, password: str
) -> MongoClient:
    mongo_uri = f"mongodb://{username}:{password}@{server}"
    return MongoClient(
        mongo_uri,
        replicaSet=replicaset,
        authSource=settings.ARC["auth"]["source"],
        authMechanism=settings.ARC["auth"]["mechanism"],
        readPreference=settings.ARC["read_preference"],
    )


def get_mysql_client() -> Engine:
    mysql_uri = (
        f"mysql+pymysql://{settings.REPORTING['username']}:{settings.REPORTING['password']}"
        f"@{settings.REPORTING['server']}:{settings.REPORTING['port']}/"
        f"{settings.REPORTING['database']}?charset=utf8"
    )
    return create_engine(mysql_uri, pool_recycle=3600)


def run_mongo_query(
    collection: Collection,
    query: dict,
    projection: dict | None = None,
    sort_field: str = "eventTime",
    limit_count: int = 10,
) -> pd.DataFrame:
    if projection is not None:
        db_query: Cursor = collection.find(filter=query, projection=projection)
    else:
        db_query: Cursor = collection.find(query)
    if sort_field:
        db_query.sort(sort_field, DESCENDING)
    if limit_count:
        db_query.limit(limit_count)

    return pd.DataFrame(list(db_query))


def run_mongo_query_agr(collection: Collection, pipeline: list[dict]) -> pd.DataFrame:
    results = collection.aggregate(pipeline=pipeline, cursor={})
    return pd.DataFrame(list(results))


def run_mysql_query(client: Engine, query: str) -> None:
    try:
        with client.connect() as sql_connection:
            sql_connection.execute(text(query))
    except SQLAlchemyError:
        logger.exception("Error executing mysql query")
        sys.exit(1)


def init_auldata_lake_reporting_table(client: Engine) -> None:
    tablename: str = settings.REPORTING["tablename"]
    logger.info("Creating table...%s", tablename)

    create_table_query: str = (
        f"CREATE TABLE IF NOT EXISTS {tablename} ("
        " `SUBSCRIBERID` VARCHAR(100),"
        " `MDN` VARCHAR(100),"
        " `BAN` VARCHAR(100),"
        " `USAGESTART` DATETIME,"
        " `USAGEEND` DATETIME,"
        " `TOTALMB` DECIMAL,"
        " `AUDITDATE` DATETIME"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"
    )
    run_mysql_query(client, create_table_query)

    create_index_query: str = f"CREATE INDEX idx_AUDITDATE ON {tablename} (AUDITDATE);"
    run_mysql_query(client, create_index_query)


def get_auldata_subscribers(
    audit_range_start: datetime, audit_range_end: datetime, offer_name: str
) -> pd.DataFrame:
    audit_client: MongoClient = get_mongo_client(
        server=settings.AUDIT["server"],
        replicaset=settings.AUDIT["replicaset"],
        username=settings.AUDIT["username"],
        password=settings.AUDIT["password"],
    )
    audit_db: Database = audit_client[settings.AUDIT["database"]]
    audit_collection: Collection = audit_db[settings.AUDIT["collection"]]

    logger.debug(
        "audit_range_start: %s", audit_range_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    )
    logger.debug("audit_range_end: %s", audit_range_end.strftime("%Y-%m-%dT%H:%M:%SZ"))

    audit_query_pipeline: list[dict] = [
        {
            "$match": {
                "$and": [
                    {
                        "details": {
                            "$elemMatch": {
                                "state": "ADD",
                                "data.payload.payloads": {
                                    "$elemMatch": {
                                        "requestpayload.subscriptions": {
                                            "$elemMatch": {"offerName": offer_name}
                                        }
                                    }
                                },
                            }
                        }
                    },
                    {
                        "lastModifiedDate": {
                            "$gte": audit_range_start,
                            "$lte": audit_range_end,
                        }
                    },
                ]
            }
        },
        {"$unwind": {"path": "$details"}},
        {
            "$match": {
                "details.state": "ADD",
                "details.data.payload.payloads": {
                    "$elemMatch": {
                        "requestpayload.subscriptions": {
                            "$elemMatch": {"offerName": offer_name}
                        }
                    }
                },
            }
        },
        {"$unwind": {"path": "$details.data.payload.payloads"}},
        {
            "$unwind": {
                "path": "$details.data.payload.payloads.requestpayload.subscriptions"
            }
        },
        {
            "$project": {
                "_id": 0.0,
                "ban": 1.0,
                "subscriberId": "$details.data.payload.subscriberId",
                "effectiveDate": "$details.data.payload.payloads.requestpayload.subscriptions.effectiveDate",
                "expiryDate": "$details.data.payload.payloads.requestpayload.subscriptions.expiryDate",
            }
        },
    ]

    return run_mongo_query_agr(audit_collection, audit_query_pipeline)


def compare_and_generate_report(
    reporting_client: Engine, auldata_subs: pd.DataFrame, audit_date: date
) -> None:
    sub_lists: dict = {}
    nodes = ["A", "B", "C"]

    row: pd.Series
    for _, row in auldata_subs.iterrows():
        remainder: int = int(row["ban"]) % 3
        sub_lists[nodes[remainder]].append(row)

    node: str
    for node in nodes:
        usage_result: pd.DataFrame = run_compare_on_node(node, sub_lists[node])
        insert_report_data(reporting_client, audit_date, usage_result)


def run_compare_on_node(node: str, sub_list: list[pd.Series]) -> pd.DataFrame | None:
    if len(sub_list) <= 0:
        return None

    node_server: dict = settings.ARC["servers"][node]
    arc_server: str = node_server["server"]
    arc_replicaset: str = node_server["replicaset"]

    arc_client: MongoClient = get_mongo_client(
        server=arc_server,
        replicaset=arc_replicaset,
        username=settings.ARC["username"],
        password=settings.ARC["password"],
    )
    arc_db: Database = arc_client[settings.ARC["database"]]
    usage_collection: Collection = arc_db[settings.ARC["collection"]]
    usage_result: pd.DataFrame = pd.DataFrame(
        columns=["extSubId", "MDN", "BAN", "start", "end", "bytesIn", "bytesOut"]
    )
    usage_projection: dict = {
        "_id": 0,
        "extSubId": 1,
        "MDN": 1,
        "BAN": 1,
        "start": 1,
        "end": 1,
        "bytesIn": 1,
        "bytesOut": 1,
    }
    us_eastern_tz: tzinfo = tz.gettz("America/New_York")
    subscriber: pd.Series
    for subscriber in sub_list:
        effective_date: datetime = datetime.strptime(
            subscriber["effectiveDate"], "%Y-%m-%dT%H:%M:%SZ"
        ).astimezone(us_eastern_tz)
        expiry_date: datetime = datetime.strptime(
            subscriber["expiryDate"], "%Y-%m-%dT%H:%M:%SZ"
        ).astimezone(us_eastern_tz)

        usage_query: dict = {
            "$and": [
                {"end": {"$gte": effective_date, "$lte": expiry_date}},
                {"extSubId": subscriber["subscriberId"]},
                {"usageType": "OVER"},
                {"$or": [{"bytesIn": {"$gt": 0}, "bytesOut": {"$gt": 0}}]},
            ]
        }

        query_result: pd.DataFrame = run_mongo_query(
            collection=usage_collection, query=usage_query, projection=usage_projection
        )
        usage_result: pd.DataFrame = pd.concat(
            objs=[usage_result, query_result], axis=0
        )

    return usage_result


def insert_report_data(
    reporting_client: Engine, audit_date: date, usage_result: pd.DataFrame
) -> None:
    if len(usage_result) <= 0:
        return

    audit_date_str: str = audit_date.strftime("%Y-%m-%d %H:%M:%S")
    tablename: str = settings.REPORTING["tablename"]
    reporting_query: str = (
        f"INSERT INTO {tablename} "
        f"(SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES "
    )
    insert_rows: list[str] = []
    row: pd.Series
    for _, row in usage_result.iterrows():
        insert_row: list[str] = [
            f"'{row['extSubId']}'",
            f"'{row['MDN']}'",
            f"'{row['BAN']}'",
            f"'{row['start']}'",
            f"'{row['end']}'",
            f"'{int(row['bytesIn']) + int(row['bytesOut'])}'",
            f"'{audit_date_str}'",
        ]
        insert_rows.append(f"({','.join(insert_row)})")
    reporting_query += ",".join(insert_rows)
    run_mysql_query(client=reporting_client, query=reporting_query)
    logger.info("%s rows written to %s", usage_result.size, tablename)


def cleanup_auldata_lake_reporting_table(client: Engine) -> None:
    tablename: str = settings.REPORTING["tablename"]
    delete_query: str = (
        f"DELETE FROM {tablename} WHERE AUDITDATE < DATE_SUB(NOW(), INTERVAL 1 MONTH)"
    )
    run_mysql_query(client=client, query=delete_query)
