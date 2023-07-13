import logging
from datetime import datetime

from sample_code.dao._base_mongo import BaseMongoDAO
from sample_code.settings import (
    AUDIT_COLLECTION,
    AUDIT_DATABASE,
    AUDIT_PASSWORD,
    AUDIT_USERNAME,
)

logger = logging.getLogger(__name__)


class AuditDAO(BaseMongoDAO):
    def __init__(
        self,
        mongoServers: str,
        mongoReplicaset: str,
    ) -> None:
        super().__init__(
            username=AUDIT_USERNAME,
            password=AUDIT_PASSWORD,
            database=AUDIT_DATABASE,
            mongoServers=mongoServers,
            mongoReplicaset=mongoReplicaset,
        )

    def run_aggregation_query(collection, query, **kwargs):
        return collection.aggregate(query, **kwargs)

    def get_subscribers(self, auditRangeStart: datetime, auditRangeEnd: datetime):
        logger.info(
            f"Get subscriber usage between {auditRangeStart.isoformat()} and {auditRangeEnd.isoformat()}"
        )
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

        return self.run_aggregation_query(auditCollection, auditQuery, cursor={})
