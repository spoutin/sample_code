"""Load or add configurations here. All secrets should be on .env"""
import os
from dotenv import load_dotenv


load_dotenv()


def get_env_var(env_var_key: str) -> str:
    """Get the Environment Variable given the key.

    Args:
        env_var_key (str): Environment Variable key.

    Raises:
        ValueError: Environment Variable is not found.

    Returns:
        str: Environment Variable.
    """
    env_var = os.environ.get(env_var_key, None)
    if env_var is None:
        raise ValueError(f"Missing Environment Variable: {env_var_key}")

    return env_var


# Reporting MySQL DB
REPORTING_MYSQL_DB = {
    "SERVER": get_env_var("REPORTING_SQL_SERVER"),
    "PORT": get_env_var("REPORTING_SQL_PORT"),
    "DATABASE": get_env_var("REPORTING_SQL_DATABASE"),
    "USERNAME": get_env_var("REPORTING_SQL_USERNAME"),
    "PASSWORD": get_env_var("REPORTING_SQL_PASSWORD"),
}
REPORTING_AULDATALEAK_TABLENAME = get_env_var("REPORTING_AULDATALEAK_TABLENAME")

# Audit MongoDB
AUDIT_MONGODB = {
    "SERVER": get_env_var("AUDIT_MONGO_SERVER"),
    "REPLICASET": get_env_var("AUDIT_MONGO_REPLICASET"),
    "USERNAME": get_env_var("AUDIT_MONGO_USERNAME"),
    "PASSWORD": get_env_var("AUDIT_MONGO_PASSWORD"),
    "DATABASE": get_env_var("AUDIT_MONGO_DATABASE"),
    "COLLECTION": get_env_var("AUDIT_MONGO_DATABASE"),
}

# Usage MongoDB's
ARC_MONGODB_NODES = {
    "A": {
        "SERVER": get_env_var("ARC_MONGO_SERVER_A"),
        "REPLICASET": get_env_var("ARC_MONGO_REPLICASET_A"),
    },
    "B": {
        "SERVER": get_env_var("ARC_MONGO_SERVER_B"),
        "REPLICASET": get_env_var("ARC_MONGO_REPLICASET_B"),
    },
    "C": {
        "SERVER": get_env_var("ARC_MONGO_SERVER_C"),
        "REPLICASET": get_env_var("ARC_MONGO_REPLICASET_C"),
    },
}
ARC_MONGODB_DETAILS = {
    "USERNAME": get_env_var("ARC_MONGO_USERNAME"),
    "PASSWORD": get_env_var("ARC_MONGO_PASSWORD"),
    "DATABASE": get_env_var("ARC_MONGO_DATABASE"),
    "COLLECTION": get_env_var("ARC_MONGO_COLLECTION"),
}

# General MongoDB Settings
GENERAL_MONGODB_SETTINGS = {
    "AUTHSOURCE": get_env_var("MONGO_AUTHSOURCE"),
    "AUTHMECHANISM": "SCRAM-SHA-1",
    "READ_PREFERENCE": "secondary",
}
