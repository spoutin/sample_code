from datetime import datetime

from pymongo import MongoClient

from settings import (
    ARC_MONGO_AUTHMECHANISM,
    ARC_MONGO_AUTHSOURCE,
    ARC_MONGO_READ_PREFERENCE,
    AUDIT_COLLECTION,
)


class AuditDAO:
    def __init__(
        self,
        mongoServers: str,
        mongoReplicaset: str,
        username: str,
        password: str,
        database: str,
    ):
        mongo_uri = f"mongodb://{username}:{password}@{mongoServers}"
        self.client = MongoClient(
            mongo_uri,
            replicaSet=mongoReplicaset,
            authSource=ARC_MONGO_AUTHSOURCE,
            readPreference=ARC_MONGO_READ_PREFERENCE,
            authMechanism=ARC_MONGO_AUTHMECHANISM,
        )[database]

    def get_subscribers(self, auditRangeStart: datetime, auditRangeEnd: datetime):
        auditCollection = self.client[AUDIT_COLLECTION]
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
                                    },
                                }
                            }
                        },
                        {
                            "lastModifiedDate": {
                                "$gte": auditRangeStart,
                                "$lte": auditRangeEnd,
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
                                "$elemMatch": {"offerName": "MYOFFERNAME"}
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

        return auditCollection.aggregate(auditQuery, cursor={})