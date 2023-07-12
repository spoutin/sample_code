import pytz
import pandas as pd
from datetime import datetime, timedelta, date, time
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo import DESCENDING
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from settings import ARC_MONGO_AUTHMECHANISM, ARC_MONGO_AUTHSOURCE, ARC_MONGO_READ_PREFERENCE, AUDIT_COLLECTION, AUDIT_DATABASE, AUDIT_PASSWORD, AUDIT_REPLICASET, AUDIT_SERVER, AUDIT_USERNAME, COLLECTION, DATABASE, PASSWORD, REPLICASET_A, REPLICASET_B, REPLICASET_C, REPORTING_AULDATALEAK_TABLENAME, REPORTING_SQL_DATABASE, REPORTING_SQL_PASSWORD, REPORTING_SQL_PORT, REPORTING_SQL_SERVER, REPORTING_SQL_USERNAME, SERVER_A, SERVER_B, SERVER_C, USERNAME


def get_mongo_client(mongoServers: str, mongoReplicaset: str, username: str, password: str):
    mongo_uri = 'mongodb://%s:%s@%s' % (username, password, mongoServers)
    return MongoClient(mongo_uri, replicaSet=mongoReplicaset, authSource=ARC_MONGO_AUTHSOURCE,
                       readPreference=ARC_MONGO_READ_PREFERENCE,
                       authMechanism=ARC_MONGO_AUTHMECHANISM)


def connect_to_mysql():
    mysql_uri = 'mysql://%s:%s@%s:%s/%s?charset=utf8' % (REPORTING_SQL_USERNAME, REPORTING_SQL_PASSWORD,
                                                         REPORTING_SQL_SERVER, REPORTING_SQL_PORT,
                                                         REPORTING_SQL_DATABASE)
    return create_engine(mysql_uri, pool_recycle=3600)


def run_mongo_query(collection: Collection, query: dict, project: dict = None, sort: bool = True,
                    sort_field: str = 'eventTime',
                    limit_results: bool = False, limit_count: int = 10):
    results = []
    if project is not None:
        db_query = collection.find(query, project)
    else:
        db_query = collection.find(query)
    if sort:
        db_query.sort(sort_field, DESCENDING)
    if limit_results:
        db_query.limit(limit_count)
    for doc in db_query:
        results.append(doc)

    results_df = pd.DataFrame(list(results))
    return results_df


def run_mongo_query_agr(collection: Collection, query: dict):
    results = collection.aggregate(query, cursor={})
    results_df = pd.DataFrame(list(results))
    return results_df


def create_mysql_table(sql_client, q, tbl_name):
    try:
        sql_client.execute(q)
        return 0
    except SQLAlchemyError as e:
        error = str(e.__dict__['orig'])
        return error


def init_aludata_leak_reporting_table(client):
    print('Creating table... ' + REPORTING_AULDATALEAK_TABLENAME)

    reportingTableCreateQuery = f'CREATE TABLE IF NOT EXISTS {REPORTING_AULDATALEAK_TABLENAME} ( \
                                `SUBSCRIBERID` VARCHAR(100), \
                                `MDN` VARCHAR(100), \
                                `BAN` VARCHAR(100), \
                                `USAGESTART` DATETIME, \
                                `USAGEEND` DATETIME, \
                                `TOTALMB` DECIMAL, \
                                `AUDITDATE` DATETIME \
                            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;'

    reportingTableCreateIndex = f'CREATE INDEX idx_AUDITDATE \
                                    ON {REPORTING_AULDATALEAK_TABLENAME} (AUDITDATE);'

    create_mysql_table(client, reportingTableCreateQuery, REPORTING_AULDATALEAK_TABLENAME)
    create_mysql_table(client, reportingTableCreateIndex, REPORTING_AULDATALEAK_TABLENAME)


def get_auldata_subscribers(auditRangeStart: datetime, auditRangeEnd: datetime):
    auditClient = get_mongo_client(
        mongoServers=AUDIT_SERVER,
        mongoReplicaset=AUDIT_REPLICASET,
        username=AUDIT_USERNAME,
        password=AUDIT_PASSWORD)[AUDIT_DATABASE]
    auditCollection = auditClient[AUDIT_COLLECTION]

    # print(auditRangeStart.strftime('%Y-%m-%dT%H:%M:%SZ'))
    # print(auditRangeEnd.strftime('%Y-%m-%dT%H:%M:%SZ'))

    auditQuery = [
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
                                                "$elemMatch": {
                                                    "offerName": "MYOFFERNAME"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        {
                            "lastModifiedDate": {
                                "$gte": auditRangeStart,
                                "$lte": auditRangeEnd
                            }
                        }
                    ]
                }
            },
            {
                "$unwind": {
                    "path": "$details"
                }
            },
            {
                "$match": {
                    "details.state": "ADD",
                    "details.data.payload.payloads": {
                        "$elemMatch": {
                            "requestpayload.subscriptions": {
                                "$elemMatch": {
                                    "offerName": "MYOFFERNAME"
                                }
                            }
                        }
                    }
                }
            },
            {
                "$unwind": {
                    "path": "$details.data.payload.payloads"
                }
            },
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
                    "expiryDate": "$details.data.payload.payloads.requestpayload.subscriptions.expiryDate"
                }
            }
        ]

    return run_mongo_query_agr(auditCollection, auditQuery)


def run_compare_on_node(node: str, subList):
    auditDate = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    arcUsageServer = ""
    arcUsageReplicaset = ""

    if node == "A":
        arcUsageServer = SERVER_A
        arcUsageReplicaset = REPLICASET_A
    elif node == "B":
        arcUsageServer = SERVER_B
        arcUsageReplicaset = REPLICASET_B
    elif node == "C":
        arcUsageServer = SERVER_C
        arcUsageReplicaset = REPLICASET_C

    if len(subList) > 0:
        usageClient = get_mongo_client(
            mongoServers=arcUsageServer,
            mongoReplicaset=arcUsageReplicaset,
            username=USERNAME,            
            password=PASSWORD)[DATABASE]        
        usageCollection = usageClient[COLLECTION]
        usageResult = pd.DataFrame(columns = ['extSubId', 'MDN', 'BAN', 'start', 'end', 'bytesIn', 'bytesOut'])

        for subscriber in subList:
            effectiveDate = datetime.strptime(subscriber["effectiveDate"], '%Y-%m-%dT%H:%M:%SZ').astimezone(pytz.timezone('US/Eastern'))
            expiryDate = datetime.strptime(subscriber["expiryDate"], '%Y-%m-%dT%H:%M:%SZ').astimezone(pytz.timezone('US/Eastern'))

            usageQuery = {"$and": [
                {"end": {"$gte": effectiveDate, "$lte": expiryDate}},
                {"extSubId": eval(subscriber["subscriberId"])},
                {"usageType": "OVER"},
                {"$or": [{"bytesIn": {"$gt": 0}, "bytesOut": {"$gt": 0}}]}
            ]}
            usageProject = {"_id": 0, "extSubId": 1, "MDN": 1, "BAN": 1, "start": 1, "end": 1, "bytesIn": 1, "bytesOut": 1}
            queryResult = run_mongo_query(usageCollection, usageQuery, usageProject)
            usageResult = pd.concat([usageResult, queryResult], axis=0)

        if len(usageResult) > 0:
            usageResultReportingQuery = f"INSERT INTO {REPORTING_AULDATALEAK_TABLENAME} (SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES "
            for index, row in usageResult.iterrows():
                usageResultReportingQuery = usageResultReportingQuery + f"(\'{row['extSubId']}\', {row['MDN']}, {row['BAN']}, \'{row['start']}\', \'{row['end']}\', \'{int(row['bytesIn']) + int(row['bytesOut'])}\', \'{auditDate}\'),"
            usageResultReportingQuery = usageResultReportingQuery[:-1]
            reportingClient.execute(usageResultReportingQuery)
            print(usageResult.size + " rows written to " + REPORTING_AULDATALEAK_TABLENAME)

def compare(auldataSubs):
    subListA = []
    subListB = []
    subListC = []

    for index, row in auldataSubs.iterrows():
        remainder = int(row["ban"]) % 3
        if remainder == 0:
            subListA.append(row)
        elif remainder == 1:
            subListB.append(row)
        elif remainder == 2:
            subListC.append(row)

    run_compare_on_node("A", subListA)
    run_compare_on_node("B", subListB)
    run_compare_on_node("C", subListC)


def auldata_leak_reporting_table_cleanup(client):
    reportingTableDeleteQuery = f"DELETE FROM {REPORTING_AULDATALEAK_TABLENAME} WHERE AUDITDATE < DATE_SUB(NOW(), INTERVAL 1 MONTH)"
    client.execute(reportingTableDeleteQuery)


if __name__ == '__main__':
    reportingClient = connect_to_mysql()
    init_aludata_leak_reporting_table(reportingClient)
    auditDate = date.today() - timedelta(days=1)
    auditRangeStart = (datetime.combine(auditDate, time(0, 0, 0)))
    auditRangeEnd = (datetime.combine(auditDate, time(23, 59, 59)))

    auldataSubs = get_auldata_subscribers(auditRangeStart, auditRangeEnd)
    compare(auldataSubs)
    auldata_leak_reporting_table_cleanup(reportingClient)
