import logging
from datetime import datetime

from pymongo import DESCENDING
from pymongo.collection import Collection

from sample_code.dao._base_mongo import BaseMongoDAO
from sample_code.settings import COLLECTION, DATABASE, PASSWORD, USERNAME

logger = logging.getLogger(__name__)


class UsageDAO(BaseMongoDAO):
    def __init__(
        self,
        mongoServers: str,
        mongoReplicaset: str,
    ) -> None:
        super().__init__(
            username=USERNAME,
            password=PASSWORD,
            database=DATABASE,
            mongoServers=mongoServers,
            mongoReplicaset=mongoReplicaset,
        )

    def run_query(
        collection: Collection,
        query: dict,
        project: dict = None,
        sort: bool = True,
        sort_field: str = "eventTime",
        limit_results: bool = False,
        limit_count: int = 10,
    ) -> list:
        if project is not None:
            db_query = collection.find(query, project)
        else:
            db_query = collection.find(query)

        if sort:
            db_query.sort(sort_field, DESCENDING)

        if limit_results:
            db_query.limit(limit_count)

        return [doc for doc in db_query]

    def get_subscriber_usage(
        self, subscriberId: str, effectiveDate: datetime, expiryDate: datetime
    ) -> list:
        logger.info(
            f"Get subscriber usage between {effectiveDate.isoformat()} and {expiryDate.isoformat()}"
        )
        collection = self.client[COLLECTION]
        usageQuery = {
            "$and": [
                {"end": {"$gte": effectiveDate, "$lte": expiryDate}},
                {"extSubId": eval(subscriberId)},
                {"usageType": "OVER"},
                {"$or": [{"bytesIn": {"$gt": 0}, "bytesOut": {"$gt": 0}}]},
            ]
        }
        usageProject = {
            "_id": 0,
            "extSubId": 1,
            "MDN": 1,
            "BAN": 1,
            "start": 1,
            "end": 1,
            "bytesIn": 1,
            "bytesOut": 1,
        }
        return self.run_query(collection, usageQuery, usageProject)
