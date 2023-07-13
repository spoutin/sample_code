from pymongo import MongoClient

from sample_code.settings import (
    ARC_MONGO_AUTHMECHANISM,
    ARC_MONGO_AUTHSOURCE,
    ARC_MONGO_READ_PREFERENCE,
)


class BaseMongoDAO:
    def __init__(
        self,
        mongoServers: str,
        mongoReplicaset: str,
        username: str,
        password: str,
        database: str,
    ) -> None:
        mongo_uri = f"mongodb://{username}:{password}@{mongoServers}"
        self.client = MongoClient(
            mongo_uri,
            replicaSet=mongoReplicaset,
            authSource=ARC_MONGO_AUTHSOURCE,
            readPreference=ARC_MONGO_READ_PREFERENCE,
            authMechanism=ARC_MONGO_AUTHMECHANISM,
        )[database]
