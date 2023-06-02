"""
in nutshell this script retreives the subscribers from a mongo DB and save it by dividing it into
3 chunks which will be inserted in different replicasets of Mysql DB.
similar to what we do in no_sql databases (Sharding).
"""

import os
import json

import pytz
import pandas as pd
from datetime import datetime, timedelta, date, time
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo import DESCENDING
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

REPORTING_SQL_USERNAME = os.environ.get('REPORTING_SQL_USERNAME')
REPORTING_SQL_PASSWORD = os.environ.get('REPORTING_SQL_PASSWORD')

AUDIT_USERNAME = os.environ.get('MONGO_AUDIT_USERNAME')
AUDIT_PASSWORD = os.environ.get('MONGO_AUDIT_PASSWORD')

USERNAME = os.environ.get('mongo_USERNAME')
PASSWORD = os.environ.get('mongo_PASSWORD')

"""
config.json contain all configuration that may differ between different clients or users where each one has his own config.
this file should be shared between Docker container and the external host through (mount or volume -v) command
during the run of docker image so it can be modifiable without accessing the code 

config.json content:
{
  "MONGO": {
    "SERVER_A" : "127.0.0.1:27017",
    "SERVER_B" : "127.0.0.1:27017",
    "SERVER_C" : "127.0.0.1:27017",
    "REPLICASET_A" : "rs0",
    "REPLICASET_B" : "rs1",
    "REPLICASET_C" : "rs2",
    "DATABASE" : "mydb",
    "COLLECTION" : "mycollection",
    "ARC_MONGO_PORT" : "27017",
    "ARC_MONGO_AUTHMECHANISM" : "SCRAM-SHA-1",
    "ARC_MONGO_AUTHSOURCE" : "admin",
    "ARC_MONGO_DATABASE" : "admin",
    "ARC_MONGO_READ_PREFERENCE" : "secondary",
    "REPORTING_AULDATALEAK_TABLENAME" : "auldata_leak"
  },
"MYSQL": {
    "REPORTING_SQL_SERVER" : "127.0.0.1",
    "REPORTING_SQL_PORT" : "3306",
    "REPORTING_SQL_DATABASE" : "myreportingdatabase"
},
  "AUDIT": {
    "AUDIT_SERVER" : "127.0.0.1:27018",
    "AUDIT_REPLICASET" : "rs4",
    "AUDIT_DATABASE" : "mydb",
    "AUDIT_COLLECTION" : "myauditcollection"
  }
}


"""
from threading import Lock

class Singleton(type):
    _instances = {}
    _lock: Lock = Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]
"""
By making owr config singleton (creational design pattern) we are sure that we have only one instance of our class
throughout all our execution.
When someone updates the file we are sure that all will share the same update.
"""
class ConfigPatternSingleton(metaclass=Singleton):

    def get_config_data(config_path):
        with open(os.path.join(config_path, 'config.json'), "r") as read_file:
            data = json.load(read_file)
        return data


DATA = ConfigPatternSingleton.get_config_data("")

"""
First principal of SOLID principles is to apply here:
Separation of Concerns (sigle responsibility)
every class should do one and only one task
what's linked to mongo should be separated in single class
"""
class MongoOperation:
    def get_mongo_client(self,mongoServers: str, mongoReplicaset: str, username: str, password: str):
        mongo_uri = 'mongodb://%s:%s@%s' % (username, password, mongoServers)
        return MongoClient(mongo_uri, replicaSet=mongoReplicaset, authSource=DATA["MONGO"]["ARC_MONGO_AUTHSOURCE"],
                           readPreference=DATA["MONGO"]["ARC_MONGO_READ_PREFERENCE"],
                           authMechanism=DATA["MONGO"]["ARC_MONGO_AUTHMECHANISM"])

    def run_mongo_query(self,collection: Collection, query: dict, project: dict = None, sort: bool = True,
                        sort_field: str = 'eventTime',
                        limit_results: bool = False, limit_count: int = 10):
        # results = []
        if project is not None:
            db_query = collection.find(query, project)
        else:
            db_query = collection.find(query)
        if sort:
            db_query.sort(sort_field, DESCENDING)
        if limit_results:
            db_query.limit(limit_count)
        # for doc in db_query:
        #     results.append(doc)
        """
        No need to loop through db_query and append to results
        we can only transform cursor to list via list(cursor)
        """
        results = list(db_query)

        results_df = pd.DataFrame(results)
        return results_df

    def run_mongo_query_agr(collection: Collection, query: dict):
        results = collection.aggregate(query, cursor={})
        results_df = pd.DataFrame(list(results))
        return results_df

    def get_auldata_subscribers(self,auditRangeStart: datetime, auditRangeEnd: datetime):
        auditClient = self.get_mongo_client(
            mongoServers=DATA["MONGO"]["AUDIT_SERVER"],
            mongoReplicaset=DATA["MONGO"]["AUDIT_REPLICASET"],
            username=AUDIT_USERNAME,
            password=AUDIT_PASSWORD)[DATA["MONGO"]["ARC_AUDIT_DATABASE"]]
        auditCollection = auditClient[DATA["MONGO"]["AUDIT_COLLECTION"]]

        # print(auditRangeStart.strftime('%Y-%m-%dT%H:%M:%SZ'))
        # print(auditRangeEnd.strftime('%Y-%m-%dT%H:%M:%SZ'))

        """
        this is the display of collection AUDIT_COLLECTION
        "AUDIT_COLLECTION":{
        "details": [
        {
            "requestpayload":{
                "subscriptions":[
                {
                "offerName": "MYOFFERNAME"
                }
                ]
            },
            "state":"ADD",
            "data":{
                "payload":{
                    "payloads":[
                            {
                            "requestpayload":{
                                "subscriptions":[
                                    {
                                    "offerName": "MYOFFERNAME"
                                    }
                                ]
                                }
                            }
                        ]
                    }
            }


        }
        ],
        "lastModifiedDate": date,
        }


        """

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
        """
        After this query is executed Data will be flatterred where each element of array represent a row of collection
        Alongside with fields of AUDIT_COLLECTION collection with the help of '$unwind' 
        """

        return self.run_mongo_query_agr(auditCollection, auditQuery)

"""
again
First principal of SOLID principles is to apply here:
Separation of Concerns (sigle responsibility)
what's linked to mysql should be separated in single class
"""
class MysqlOperations:



    def create_mysql_table(self, sql_client, q, tbl_name):
        try:
            sql_client.execute(q)
            return 0
        except SQLAlchemyError as e:
            error = str(e.__dict__['orig'])
            return error

    def connect_to_mysql(self):
        mysql_uri = 'mysql://%s:%s@%s:%s/%s?charset=utf8' % (
            DATA["MYSQL"]["REPORTING_SQL_USERNAME"], DATA["MYSQL"]["REPORTING_SQL_PASSWORD"],
            DATA["MYSQL"]["REPORTING_SQL_SERVER"], DATA["MYSQL"]["REPORTING_SQL_PORT"],
            DATA["MYSQL"]["REPORTING_SQL_DATABASE"])
        return create_engine(mysql_uri, pool_recycle=3600)
    def init_aludata_leak_reporting_table(self, client):
        print('Creating table... ' + DATA["MONGO"]["REPORTING_AULDATALEAK_TABLENAME"])

        reportingTableCreateQuery = f'CREATE TABLE IF NOT EXISTS {DATA["MONGO"]["REPORTING_AULDATALEAK_TABLENAME"]} ( \
                                    `SUBSCRIBERID` VARCHAR(100), \
                                    `MDN` VARCHAR(100), \
                                    `BAN` VARCHAR(100), \
                                    `USAGESTART` DATETIME, \
                                    `USAGEEND` DATETIME, \
                                    `TOTALMB` DECIMAL, \
                                    `AUDITDATE` DATETIME \
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;'

        reportingTableCreateIndex = f'CREATE INDEX idx_AUDITDATE \
                                        ON {DATA["MONGO"]["REPORTING_AULDATALEAK_TABLENAME"]} (AUDITDATE);'

        self.create_mysql_table(client, reportingTableCreateQuery, DATA["MONGO"]["REPORTING_AULDATALEAK_TABLENAME"])
        self.create_mysql_table(client, reportingTableCreateIndex, DATA["MONGO"]["REPORTING_AULDATALEAK_TABLENAME"])

    def aludata_leak_reporting_table_cleanup(self,client):
        reportingTableDeleteQuery = f"DELETE FROM {DATA['MONGO']['REPORTING_AULDATALEAK_TABLENAME']} WHERE AUDITDATE < DATE_SUB(NOW(), INTERVAL 1 MONTH)"
        client.execute(reportingTableDeleteQuery)

class Comparaison:
    def run_compare_on_node(self,node: str, subList):
        auditDate = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        arcUsageServer = ""
        arcUsageReplicaset = ""

        if node == "A":
            arcUsageServer = DATA["MONGO"]["SERVER_A"]
            arcUsageReplicaset = DATA["MONGO"]["REPLICASET_A"]
        elif node == "B":
            arcUsageServer = DATA["MONGO"]["SERVER_B"]
            arcUsageReplicaset = DATA["MONGO"]["REPLICASET_B"]
        elif node == "C":
            arcUsageServer = DATA["MONGO"]["SERVER_C"]
            arcUsageReplicaset = DATA["MONGO"]["REPLICASET_C"]

        if len(subList) > 0:
            mongo_op=MongoOperation()
            usageClient = mongo_op.get_mongo_client(
                mongoServers=arcUsageServer,
                mongoReplicaset=arcUsageReplicaset,
                username=USERNAME,
                password=PASSWORD)[DATA["MONGO"]["ARC_USAGE_DATABASE"]]
            usageCollection = usageClient[DATA["MONGO"]["COLLECTION"]]
            usageResult = pd.DataFrame(columns=['extSubId', 'MDN', 'BAN', 'start', 'end', 'bytesIn', 'bytesOut'])

            for subscriber in subList:
                effectiveDate = datetime.strptime(subscriber["effectiveDate"], '%Y-%m-%dT%H:%M:%SZ').astimezone(
                    pytz.timezone('US/Eastern'))
                expiryDate = datetime.strptime(subscriber["expiryDate"], '%Y-%m-%dT%H:%M:%SZ').astimezone(
                    pytz.timezone('US/Eastern'))

                usageQuery = {"$and": [
                    {"end": {"$gte": effectiveDate, "$lte": expiryDate}},
                    {"extSubId": eval(subscriber["subscriberId"])},
                    {"usageType": "OVER"},
                    {"$or": [{"bytesIn": {"$gt": 0}, "bytesOut": {"$gt": 0}}]}
                ]}
                usageProject = {"_id": 0, "extSubId": 1, "MDN": 1, "BAN": 1, "start": 1, "end": 1, "bytesIn": 1,
                                "bytesOut": 1}

                queryResult = run_mongo_query(usageCollection, usageQuery, usageProject)
                usageResult = pd.concat([usageResult, queryResult], axis=0)

            if len(usageResult) > 0:
                usageResultReportingQuery = f"INSERT INTO {DATA['MONGO']['REPORTING_AULDATALEAK_TABLENAME']} (SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES "
                for index, row in usageResult.iterrows():
                    usageResultReportingQuery = usageResultReportingQuery + f"(\'{row['extSubId']}\', {row['MDN']}, {row['BAN']}, \'{row['start']}\', \'{row['end']}\', \'{int(row['bytesIn']) + int(row['bytesOut'])}\', \'{auditDate}\'),"
                usageResultReportingQuery = usageResultReportingQuery[:-1]
                reportingClient.execute(usageResultReportingQuery)
                print(usageResult.size + " rows written to " + DATA["MONGO"]["REPORTING_AULDATALEAK_TABLENAME"])


    def compare(self,auldataSubs):
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

        self.run_compare_on_node("A", subListA)
        self.run_compare_on_node("B", subListB)
        self.run_compare_on_node("C", subListC)


if __name__ == '__main__':
    mysql_op=MysqlOperations()
    comparaison=Comparaison()
    mongo_op=MongoOperation()
    reportingClient = mysql_op.connect_to_mysql()
    mysql_op.init_aludata_leak_reporting_table(reportingClient)
    auditDate = date.today() - timedelta(days=1)
    auditRangeStart = (datetime.combine(auditDate, time(0, 0, 0)))
    auditRangeEnd = (datetime.combine(auditDate, time(23, 59, 59)))

    auldataSubs = mongo_op.get_auldata_subscribers(auditRangeStart, auditRangeEnd)
    comparaison.compare(auldataSubs)
    mysql_op.aludata_leak_reporting_table_cleanup(reportingClient)
