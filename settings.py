import logging
import os

logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

REPORTING_SQL_SERVER = os.environ.get("REPORTING_SQL_SERVER", "127.0.0.1")
REPORTING_SQL_PORT = os.environ.get("REPORTING_SQL_PORT", "3306")
REPORTING_SQL_DATABASE = os.environ.get("REPORTING_SQL_DATABASE", "myreportingdatabase")
REPORTING_SQL_USERNAME = os.environ.get("REPORTING_SQL_USERNAME")
REPORTING_SQL_PASSWORD = os.environ.get("REPORTING_SQL_PASSWORD")


AUDIT_SERVER = os.environ.get("AUDIT_SERVER", "127.0.0.1:27018")
AUDIT_REPLICASET = os.environ.get("AUDIT_REPLICASET", "rs4")
AUDIT_USERNAME = os.environ.get("MONGO_AUDIT_USERNAME")
AUDIT_PASSWORD = os.environ.get("MONGO_AUDIT_PASSWORD")
AUDIT_DATABASE = os.environ.get("AUDIT_DATABASE", "mydb")
AUDIT_COLLECTION = os.environ.get("AUDIT_COLLECTION", "myauditcollection")


SERVER_A = os.environ.get("SERVER_A", "127.0.0.1:27017")
SERVER_B = os.environ.get("SERVER_B", "127.0.0.1:27017")
SERVER_C = os.environ.get("SERVER_C", "127.0.0.1:27017")
REPLICASET_A = os.environ.get("REPLICASET_A", "rs0")
REPLICASET_B = os.environ.get("REPLICASET_B", "rs1")
REPLICASET_C = os.environ.get("REPLICASET_C", "rs2")

USERNAME = os.environ.get("mongo_USERNAME")
PASSWORD = os.environ.get("mongo_PASSWORD")
DATABASE = os.environ.get("DATABASE", "mydb")
COLLECTION = os.environ.get("COLLECTION", "mycollection")

ARC_MONGO_AUTHMECHANISM = os.environ.get("ARC_MONGO_AUTHMECHANISM", "SCRAM-SHA-1")
ARC_MONGO_AUTHSOURCE = os.environ.get("ARC_MONGO_AUTHSOURCE", "admin")
ARC_MONGO_READ_PREFERENCE = os.environ.get("ARC_MONGO_READ_PREFERENCE", "secondary")


REPORTING_AULDATALEAK_TABLENAME = os.environ.get(
    "REPORTING_AULDATALEAK_TABLENAME", "auldata_leak"
)
