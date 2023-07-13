from datetime import date, timedelta

from sample_code.dao.audit import AuditDAO


def test_get_subscribers(mock_usage_mongo_run_agg_query):
    startDate = date.today() - timedelta(days=1)
    endDate = date.today()
    auditClient = AuditDAO("mongo-server.com", "mongo-replicaset")

    auditClient.get_subscribers(startDate, endDate)
    assert mock_usage_mongo_run_agg_query.called_once()
    assert mock_usage_mongo_run_agg_query.mock_calls[0].args[1] == [
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
                                            "$elemMatch": {"offerName": "MYOFFERNAME"}
                                        }
                                    }
                                },
                            }
                        }
                    },
                    {
                        "lastModifiedDate": {
                            "$gte": startDate,
                            "$lte": endDate,
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
