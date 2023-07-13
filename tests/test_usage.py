from datetime import date, timedelta

from sample_code.dao.usage import UsageDAO


def test_get_subscriber_usage(mock_usage_mongo_run_query):
    effDate = date.today() - timedelta(days=1)
    expDate = date.today()
    usageClient = UsageDAO("mongo-server.com", "mongo-replicaset")

    usageClient.get_subscriber_usage("1", effDate, expDate)
    assert mock_usage_mongo_run_query.called_once()
    assert mock_usage_mongo_run_query.mock_calls[0].args[1] == {
        "$and": [
            {"end": {"$gte": effDate, "$lte": expDate}},
            {"extSubId": 1},
            {"usageType": "OVER"},
            {"$or": [{"bytesIn": {"$gt": 0}, "bytesOut": {"$gt": 0}}]},
        ]
    }
    assert mock_usage_mongo_run_query.mock_calls[0].args[2] == {
        "_id": 0,
        "extSubId": 1,
        "MDN": 1,
        "BAN": 1,
        "start": 1,
        "end": 1,
        "bytesIn": 1,
        "bytesOut": 1,
    }
