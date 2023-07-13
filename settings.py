import logging
import os

# Configure the root logger
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

REPORTING_SQL_SERVER = "127.0.0.1"
REPORTING_SQL_PORT = "3306"
REPORTING_SQL_DATABASE = "myreportingdatabase"
REPORTING_SQL_USERNAME = os.environ.get("REPORTING_SQL_USERNAME")
REPORTING_SQL_PASSWORD = os.environ.get("REPORTING_SQL_PASSWORD")

AUDIT_SERVER = "127.0.0.1:27018"
AUDIT_REPLICASET = "rs4"
AUDIT_USERNAME = os.environ.get("MONGO_AUDIT_USERNAME")
AUDIT_PASSWORD = os.environ.get("MONGO_AUDIT_PASSWORD")
AUDIT_DATABASE = "mydb"
AUDIT_COLLECTION = "myauditcollection"

SERVER_A = "127.0.0.1:27017"
SERVER_B = "127.0.0.1:27017"
SERVER_C = "127.0.0.1:27017"
REPLICASET_A = "rs0"
REPLICASET_B = "rs1"
REPLICASET_C = "rs2"
USERNAME = os.environ.get("mongo_USERNAME")
PASSWORD = os.environ.get("mongo_PASSWORD")
DATABASE = "mydb"
COLLECTION = "mycollection"
ARC_MONGO_PORT = "27017"
ARC_MONGO_AUTHMECHANISM = "SCRAM-SHA-1"
ARC_MONGO_AUTHSOURCE = "admin"
ARC_MONGO_DATABASE = "admin"
ARC_MONGO_READ_PREFERENCE = "secondary"

REPORTING_AULDATALEAK_TABLENAME = "auldata_leak"
