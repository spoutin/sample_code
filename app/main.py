"""All important functions goes here."""
from datetime import datetime
from ast import literal_eval
import pandas as pd
import pytz
from pandas import DataFrame

from app.utils.db_utils import MySQLClient, MongoCollection
from app.utils.time_utils import get_datetime_today, get_datetime_range_today
from app.config import (
    REPORTING_MYSQL_DB,
    REPORTING_AULDATALEAK_TABLENAME,
    AUDIT_MONGODB,
    ARC_MONGODB_NODES,
    ARC_MONGODB_DETAILS,
    GENERAL_MONGODB_SETTINGS,
)


def get_reporting_client() -> MySQLClient:
    """Build MySQL Client for Reporting.

    Returns:
        MySQLClient: MySQL Client for Reporting.
    """
    return MySQLClient(mysql_details=REPORTING_MYSQL_DB)


def get_audit_collection() -> MongoCollection:
    """Build MongoDB Collection for Auditing.

    Returns:
        MongoCollection: MongoDB Collection for Auditing
    """
    return MongoCollection(
        mongodb_details=AUDIT_MONGODB, mongodb_general_settings=GENERAL_MONGODB_SETTINGS
    )


def get_usage_collection(node: str) -> MongoCollection:
    """Build MongoDB Collection for Usage Reporting.

    Returns:
        MongoCollection: MongoDB Collection for Usage Reporting.
    """
    mongodb_details = {**ARC_MONGODB_NODES[node], **ARC_MONGODB_DETAILS}
    return MongoCollection(
        mongodb_details=mongodb_details,
        mongodb_general_settings=GENERAL_MONGODB_SETTINGS,
    )


def init_auldata_leak_reporting_table(client: MySQLClient) -> None:
    """Initialize Table for Data Leak.

    Args:
        client (MySQLClient): MySQL Client for Reporting.
    """
    print("Creating table... " + REPORTING_AULDATALEAK_TABLENAME)

    reporting_table_create_query = f"CREATE TABLE IF NOT EXISTS \
        {REPORTING_AULDATALEAK_TABLENAME} ( \
            `SUBSCRIBERID` VARCHAR(100), \
            `MDN` VARCHAR(100), \
            `BAN` VARCHAR(100), \
            `USAGESTART` DATETIME, \
            `USAGEEND` DATETIME, \
            `TOTALMB` DECIMAL, \
            `AUDITDATE` DATETIME \
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"

    reporting_table_create_index = f"CREATE INDEX idx_AUDITDATE \
                                    ON {REPORTING_AULDATALEAK_TABLENAME} (AUDITDATE);"

    client.execute_query(reporting_table_create_query)
    client.execute_query(reporting_table_create_index)


def get_auldata_subscribers(
    audit_collection: MongoCollection,
    audit_range_start: datetime,
    audit_range_end: datetime,
) -> DataFrame:
    """Get data of all Subscribers from the Audit Database.

    Args:
        audit_collection (MongoCollection): MongoDB Collection for Auditing.
        audit_range_start (datetime): Start Datetime of Audit.
        audit_range_end (datetime): End Datetime of Audit.

    Returns:
        DataFrame: Data of all Subscribers.
    """

    audit_query = [
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
                                            "$elemMatch": {"offerName": "MYOFFERNAME"}
                                        }
                                    }
                                },
                            },
                        },
                    },
                    {
                        "lastModifiedDate": {
                            "$gte": audit_range_start,
                            "$lte": audit_range_end,
                        },
                    },
                ]
            },
        },
        {"$unwind": {"path": "$details"}},
        {
            "$match": {
                "details.state": "ADD",
                "details.data.payload.payloads": {
                    "$elemMatch": {
                        "requestpayload.subscriptions": {
                            "$elemMatch": {"offerName": "MYOFFERNAME"}
                        },
                    },
                },
            },
        },
        {"$unwind": {"path": "$details.data.payload.payloads"}},
        {
            "$unwind": {
                "path": "$details.data.payload.payloads.requestpayload.subscriptions"
            },
        },
        {
            "$project": {
                "_id": 0.0,
                "ban": 1.0,
                "subscriberId": "$details.data.payload.subscriberId",
                "effectiveDate": "$details.data.payload.payloads.\
                    requestpayload.subscriptions.effectiveDate",
                "expiryDate": "$details.data.payload.payloads.\
                    requestpayload.subscriptions.expiryDate",
            },
        },
    ]

    return audit_collection.run_mongo_query_agr(audit_query)


def run_compare_on_node(
    node: str, sub_list: DataFrame, reporting_client: MySQLClient
) -> None:
    """Create a report of Usage of Subscribers for a single node.
       Write the report on the Reporting Database.

    Args:
        node (str): Choose which Node to compare.
        sub_list (DataFrame): Sub-list of Subscribers.
        reporting_client (MySQLClient): MySQL Client for Reporting.
    """
    if len(sub_list) == 0:
        return None

    audit_date = get_datetime_today()
    usage_collection = get_usage_collection(node)

    usage_result = DataFrame(
        columns=["extSubId", "MDN", "BAN", "start", "end", "bytesIn", "bytesOut"]
    )

    for _, subscriber in sub_list.iterrows():
        print(subscriber["subscriberId"])
        effective_date = datetime.strptime(
            subscriber["effectiveDate"], "%Y-%m-%dT%H:%M:%SZ"
        ).astimezone(pytz.timezone("US/Eastern"))
        expiry_date = datetime.strptime(
            subscriber["expiryDate"], "%Y-%m-%dT%H:%M:%SZ"
        ).astimezone(pytz.timezone("US/Eastern"))

        usage_query = {
            "$and": [
                {"end": {"$gte": effective_date, "$lte": expiry_date}},
                {"extSubId": literal_eval(subscriber["subscriberId"])},
                {"usageType": "OVER"},
                {"$or": [{"bytesIn": {"$gt": 0}, "bytesOut": {"$gt": 0}}]},
            ]
        }
        usage_project = {
            "_id": 0,
            "extSubId": 1,
            "MDN": 1,
            "BAN": 1,
            "start": 1,
            "end": 1,
            "bytesIn": 1,
            "bytesOut": 1,
        }
        query_result = usage_collection.run_mongo_query(usage_query, usage_project)
        usage_result = pd.concat([usage_result, query_result], axis=0)

        if len(usage_result) == 0:
            continue

        usage_result_reporting_query = f"INSERT INTO {REPORTING_AULDATALEAK_TABLENAME} \
            (SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES "
        for _, row in usage_result.iterrows():
            usage_result_reporting_query += f"('{row['extSubId']}', \
                {row['MDN']}, {row['BAN']}, '{row['start']}', \
                '{row['end']}', '{int(row['bytesIn']) + int(row['bytesOut'])}', \
                '{audit_date}'),"
        usage_result_reporting_query = usage_result_reporting_query[:-1]
        reporting_client.execute_query(usage_result_reporting_query)
        print(usage_result.size + " rows written to " + REPORTING_AULDATALEAK_TABLENAME)

    return None


def compare_auldata(auldata_subs: DataFrame, reporting_client: MySQLClient) -> None:
    """Generate Report of Usage of Subscribers for three nodes.

    Args:
        auldata_subs (DataFrame): Data of all Subscribers.
        reporting_client (MySQLClient): MySQL Client for Reporting.
    """
    nodes = list(ARC_MONGODB_NODES.keys())
    sub_lists: list[DataFrame] = [[], [], []]

    for _, row in auldata_subs.iterrows():
        remainder = int(row["ban"]) % 3
        sub_lists[remainder].append(row)

    for node, sub_list in zip(nodes, sub_lists):
        run_compare_on_node(node, sub_list, reporting_client)


def cleanup_auldata_leak_reporting_table(client: MySQLClient) -> None:
    """Delete the older Subscriber Data Reports older than 1 month.

    Args:
        client (MySQLClient): MySQL Client for Reporting.
    """
    reporting_table_delete_query = f"DELETE FROM {REPORTING_AULDATALEAK_TABLENAME} \
          WHERE AUDITDATE < DATE_SUB(NOW(), INTERVAL 1 MONTH)"
    client.execute_query(reporting_table_delete_query)

    print("Deleting table... " + REPORTING_AULDATALEAK_TABLENAME)


def run_program() -> None:
    """Run the Reporting Process"""
    reporting_client = get_reporting_client()
    init_auldata_leak_reporting_table(reporting_client)

    audit_collection = get_audit_collection()
    audit_range_start, audit_range_end = get_datetime_range_today()
    auldata_subs = get_auldata_subscribers(
        audit_collection, audit_range_start, audit_range_end
    )

    compare_auldata(auldata_subs, reporting_client)
    cleanup_auldata_leak_reporting_table(reporting_client)
