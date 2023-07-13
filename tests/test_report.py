from sample_code.dao.reporting import ReportDAO


def test_process_data_for_insert():
    data = [[1, 3, "Test", "Hello world"], [1, 3, "Audit", "Rain man"]]
    res = ReportDAO.process_data_for_insert(data)

    assert res == "(1, 3, Test, Hello world), (1, 3, Audit, Rain man)"


def test_insert_reporting_data(mock_report_mysql_client):
    data = [[1, 3, "Test", "Hello world"], [1, 3, "Audit", "Rain man"]]
    reportClient = ReportDAO()
    mock_report_mysql_client.reset_mock()

    reportClient.insert_reporting_data(data)
    assert len(mock_report_mysql_client.mock_calls) == 1
    assert (
        mock_report_mysql_client.mock_calls[0].args[0]
        == "INSERT INTO auldata_leak (SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES (1, 3, Test, Hello world), (1, 3, Audit, Rain man)"
    )


def test_clean_reporting_data(mock_report_mysql_client):
    reportClient = ReportDAO()
    mock_report_mysql_client.reset_mock()

    reportClient.clean_reporting_data()
    assert len(mock_report_mysql_client.mock_calls) == 1
    assert (
        mock_report_mysql_client.mock_calls[0].args[0]
        == "DELETE FROM auldata_leak WHERE AUDITDATE < DATE_SUB(NOW(), INTERVAL 1 MONTH)"
    )
