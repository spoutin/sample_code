from unittest.mock import patch

import pandas as pd
from freezegun import freeze_time

from sample_code.main import Main


def test_compare(mock_main_run_compare_on_node):
    def serialize_list_series(series):
        return [dict(s) for s in series]

    data = pd.DataFrame(
        [
            {"ban": 1, "subscriberId": 1},
            {"ban": 2, "subscriberId": 3},
            {"ban": 3, "subscriberId": 6},
            {"ban": 4, "subscriberId": 7},
            {"ban": 5, "subscriberId": 21},
        ]
    )
    mainClient = Main()
    mainClient.compare(data)

    assert len(mock_main_run_compare_on_node.mock_calls) == 3

    assert mock_main_run_compare_on_node.mock_calls[0].args[0] == "A"
    assert serialize_list_series(
        mock_main_run_compare_on_node.mock_calls[0].args[1]
    ) == [{"ban": 3, "subscriberId": 6}]

    assert mock_main_run_compare_on_node.mock_calls[1].args[0] == "B"
    assert serialize_list_series(
        mock_main_run_compare_on_node.mock_calls[1].args[1]
    ) == [{"ban": 1, "subscriberId": 1}, {"ban": 4, "subscriberId": 7}]

    assert mock_main_run_compare_on_node.mock_calls[2].args[0] == "C"
    assert serialize_list_series(
        mock_main_run_compare_on_node.mock_calls[2].args[1]
    ) == [{"ban": 2, "subscriberId": 3}, {"ban": 5, "subscriberId": 21}]


@freeze_time("2023-07-13 12:00:01")
@patch("sample_code.dao.usage.UsageDAO.get_subscriber_usage")
def test_run_compare_on_node__case_one_sub(
    mock_get_subscriber_usage, mock_report_mysql_client
):
    mock_get_subscriber_usage.return_value = pd.DataFrame(
        [
            {
                "extSubId": "1",
                "MDN": "+2126666666",
                "BAN": 1,
                "start": "",
                "end": "",
                "bytesIn": 2048,
                "bytesOut": 512,
            },
        ]
    )
    data = [
        {
            "ban": 2,
            "subscriberId": "3",
            "effectiveDate": "2022-12-11T09:15:30Z",
            "expiryDate": "2023-01-01T11:43:00Z",
        }
    ]
    mainClient = Main()
    mock_report_mysql_client.reset_mock()
    mainClient.run_compare_on_node("A", data)

    assert len(mock_report_mysql_client.mock_calls) == 1
    assert (
        mock_report_mysql_client.mock_calls[0].args[0]
        == "INSERT INTO auldata_leak (SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES (1, +2126666666, 1, , , 2560, 2023-07-13 12:00:01)"
    )


@patch("sample_code.dao.usage.UsageDAO.get_subscriber_usage")
def test_run_compare_on_node__case_no_sub(
    mock_get_subscriber_usage, mock_report_mysql_client
):
    mock_get_subscriber_usage.return_value = pd.DataFrame([])
    data = [
        {
            "ban": 2,
            "subscriberId": "3",
            "effectiveDate": "2022-12-11T09:15:30Z",
            "expiryDate": "2023-01-01T11:43:00Z",
        }
    ]
    mainClient = Main()
    mock_report_mysql_client.reset_mock()
    mainClient.run_compare_on_node("A", data)

    assert len(mock_report_mysql_client.mock_calls) == 0
