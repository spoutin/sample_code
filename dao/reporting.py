import logging

from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from sample_code.settings import (
    REPORTING_AULDATALEAK_TABLENAME,
    REPORTING_SQL_DATABASE,
    REPORTING_SQL_PASSWORD,
    REPORTING_SQL_PORT,
    REPORTING_SQL_SERVER,
    REPORTING_SQL_USERNAME,
)

logger = logging.getLogger(__name__)


class ReportDAO:
    def __init__(self) -> None:
        mysql_uri = f"mysql://{REPORTING_SQL_USERNAME}:{REPORTING_SQL_PASSWORD}@{REPORTING_SQL_SERVER}:{REPORTING_SQL_PORT}/{REPORTING_SQL_DATABASE}?charset=utf8"
        self.client = create_engine(mysql_uri, pool_recycle=3600)

    def run_query(self, query):
        try:
            self.client.execute(query)
            return 0
        except SQLAlchemyError as e:
            error = str(e.__dict__["orig"])
            logger.error(error)
            return error

    def create_reporting_table(self) -> None:
        logger.info("Initiate reporting table")
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
    def process_data_for_insert(rows: list) -> str:
        return ", ".join([f"({', '.join(map(str, r))})" for r in rows])

    def insert_reporting_data(self, rows: list) -> None:
        logger.info("Insert new data in reporting table")
        usageReportingQuery = f"INSERT INTO {REPORTING_AULDATALEAK_TABLENAME} (SUBSCRIBERID, MDN, BAN, USAGESTART, USAGEEND, TOTALMB, AUDITDATE) VALUES "
        data = self.process_data_for_insert(rows)
        self.run_query(usageReportingQuery + data)

    def clean_reporting_data(self) -> None:
        logger.info("Clean reporting table")
        reportingTableDeleteQuery = f"DELETE FROM {REPORTING_AULDATALEAK_TABLENAME} WHERE AUDITDATE < DATE_SUB(NOW(), INTERVAL 1 MONTH)"
        self.run_query(reportingTableDeleteQuery)
