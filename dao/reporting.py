from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from settings import (
    REPORTING_AULDATALEAK_TABLENAME,
    REPORTING_SQL_DATABASE,
    REPORTING_SQL_PASSWORD,
    REPORTING_SQL_PORT,
    REPORTING_SQL_SERVER,
    REPORTING_SQL_USERNAME,
)


class ReportDAO:
    def __init__(self):
        mysql_uri = f"mysql://{REPORTING_SQL_USERNAME}:{REPORTING_SQL_PASSWORD}@{REPORTING_SQL_SERVER}:{REPORTING_SQL_PORT}/{REPORTING_SQL_DATABASE}?charset=utf8"
        self.client = create_engine(mysql_uri, pool_recycle=3600)

    def run_query(self, query):
        try:
            self.client.execute(query)
            return 0
        except SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
            return error

    def create_reporting_table(self):
        reportingTableCreateQuery = f"CREATE TABLE IF NOT EXISTS {REPORTING_AULDATALEAK_TABLENAME} ( \
                                    `SUBSCRIBERID` VARCHAR(100), \
                                    `MDN` VARCHAR(100), \
                                    `BAN` VARCHAR(100), \
                                    `USAGESTART` DATETIME, \
                                    `USAGEEND` DATETIME, \
                                    `TOTALMB` DECIMAL, \
                                    `AUDITDATE` DATETIME \
                                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"

        reportingTableCreateIndex = f"CREATE INDEX idx_AUDITDATE \
                                        ON {REPORTING_AULDATALEAK_TABLENAME} (AUDITDATE);"

        self.run_query(reportingTableCreateQuery)
        self.run_query(reportingTableCreateIndex)

    @staticmethod
    def process_data_for_insert(rows):
        return ", ".join([f"({', '.join(r)})" for r in rows])

    def insert_reporting_data(self, rows):
        usageReportingQuery = f"INSERT INTO {REPORTING_AULDATALEAK_TABLENAME} (SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES "
        data = self.process_data_for_insert(rows)
        self.run_query(usageReportingQuery + data)
