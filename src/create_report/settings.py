import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def get_env_var(key: str) -> str:
    try:
        return os.environ[key]
    except KeyError:
        logger.exception("Environment variable %s not set", key)
        sys.exit(1)


REPORTING = {
    "server": get_env_var("REPORTING_SQL_SERVER"),
    "port": get_env_var("REPORTING_SQL_PORT"),
    "database": get_env_var("REPORTING_SQL_DATABASE"),
    "username": get_env_var("REPORTING_SQL_USERNAME"),
    "password": get_env_var("REPORTING_SQL_PASSWORD"),
    "tablename": get_env_var("REPORTING_SQL_TABLENAME"),
}

AUDIT = {
    "server": get_env_var("AUDIT_MONGO_SERVER"),
    "replicaset": get_env_var("AUDIT_MONGO_REPLICASET"),
    "username": get_env_var("AUDIT_MONGO_USERNAME"),
    "password": get_env_var("AUDIT_MONGO_PASSWORD"),
    "database": get_env_var("AUDIT_MONGO_DATABASE"),
    "collection": get_env_var("AUDIT_MONGO_COLLECTION"),
}

ARC = {
    "servers": {
        "A": {
            "server": get_env_var("ARC_MONGO_SERVER_A"),
            "replicaset": get_env_var("ARC_MONGO_REPLICASET_A"),
        },
        "B": {
            "server": get_env_var("ARC_MONGO_SERVER_B"),
            "replicaset": get_env_var("ARC_MONGO_REPLICASET_B"),
        },
        "C": {
            "server": get_env_var("ARC_MONGO_SERVER_C"),
            "replicaset": get_env_var("ARC_MONGO_REPLICASET_C"),
        },
    },
    "username": get_env_var("ARC_MONGO_USERNAME"),
    "password": get_env_var("ARC_MONGO_PASSWORD"),
    "database": get_env_var("ARC_MONGO_DATABASE"),
    "collection": get_env_var("ARC_MONGO_COLLECTION"),
    "auth": {
        "mechanism": get_env_var("ARC_MONGO_AUTH_MECHANISM"),
        "source": get_env_var("ARC_MONGO_AUTH_SOURCE"),
        "database": get_env_var("ARC_MONGO_AUTH_DATABASE"),
    },
    "read_preference": get_env_var("ARC_MONGO_READ_PREFERENCE"),
}

OFFER_NAME = get_env_var("OFFER_NAME")
