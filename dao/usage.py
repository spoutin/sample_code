from pymongo import DESCENDING, MongoClient
from pymongo.collection import Collection

from sample_code.settings import (
    ARC_MONGO_AUTHMECHANISM,
    ARC_MONGO_AUTHSOURCE,
    ARC_MONGO_READ_PREFERENCE,
    COLLECTION,
)


class UsageDAO:
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

    def run_query(
        collection: Collection,
        query: dict,
        project: dict = None,
        sort: bool = True,
        sort_field: str = "eventTime",
        limit_results: bool = False,
        limit_count: int = 10,
    ):
        if project is not None:
            db_query = collection.find(query, project)
        else:
            db_query = collection.find(query)

        if sort:
            db_query.sort(sort_field, DESCENDING)

        if limit_results:
            db_query.limit(limit_count)

        results = []
        for doc in db_query:
            results.append(doc)
        # results_df = pd.DataFrame(list(results))
        # return results_df

        return results

    def get_subscriber_usage(self, subscriberId, effectiveDate, expiryDate) -> list:
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
