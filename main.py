import logging
from datetime import date, datetime, time, timedelta

import pandas as pd
import pytz

from sample_code.dao.audit import AuditDAO
from sample_code.dao.reporting import ReportDAO
from sample_code.dao.usage import UsageDAO
from sample_code.settings import (
    AUDIT_REPLICASET,
    AUDIT_SERVER,
    REPLICASET_A,
    REPLICASET_B,
    REPLICASET_C,
    SERVER_A,
    SERVER_B,
    SERVER_C,
)

logger = logging.getLogger(__name__)


class Main:
    def __init__(self) -> None:
        logger.info("Initiate clients with databases")
        self.reportingClient = ReportDAO()

        self.auditClient = AuditDAO(
            mongoServers=AUDIT_SERVER,
            mongoReplicaset=AUDIT_REPLICASET,
        )

        self.usageClient_A = UsageDAO(
            mongoServers=SERVER_A,
            mongoReplicaset=REPLICASET_A,
        )

        self.usageClient_B = UsageDAO(
            mongoServers=SERVER_B,
            mongoReplicaset=REPLICASET_B,
        )

        self.usageClient_C = UsageDAO(
            mongoServers=SERVER_C,
            mongoReplicaset=REPLICASET_C,
        )

    def get_auldata_subscribers(
        self, auditRangeStart: datetime, auditRangeEnd: datetime
    ):
        logger.info(
            f"Get subscribers for the range between {auditRangeStart.isoformat()} and {auditRangeEnd.isoformat()}"
        )
        res = self.auditClient.get_subscribers(auditRangeStart, auditRangeEnd)
        return pd.DataFrame(list(res))

    def compare(self, auldataSubs):
        logger.info(f"Start comparing subscribers's data")
        subListA = []
        subListB = []
        subListC = []

        for _, row in auldataSubs.iterrows():
            remainder = int(row["ban"]) % 3
            if remainder == 0:
                subListA.append(row)
            elif remainder == 1:
                subListB.append(row)
            elif remainder == 2:
                subListC.append(row)

        self.run_compare_on_node("A", subListA)
        self.run_compare_on_node("B", subListB)
        self.run_compare_on_node("C", subListC)

    def run_compare_on_node(self, node: str, subList: list):
        logger.info(f"Start comparing subscribers's data on the node {node}")
        to_date = lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ").astimezone(
            pytz.timezone("US/Eastern")
        )

        if len(subList) > 0:
            auditDate = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
            usageClient = getattr(self, f"usageClient_{node}", None)

            if not usageClient:
                raise Exception("Wrong node!")

            usageResult = pd.DataFrame(
                columns=[
                    "extSubId",
                    "MDN",
                    "BAN",
                    "start",
                    "end",
                    "bytesIn",
                    "bytesOut",
                ]
            )

            for subscriber in subList:
                effectiveDate = to_date(subscriber["effectiveDate"])
                expiryDate = to_date(subscriber["expiryDate"])

                res = usageClient.get_subscriber_usage(
                    subscriber["subscriberId"], effectiveDate, expiryDate
                )
                usageResult = pd.concat([usageResult, pd.DataFrame(res)], axis=0)

            if len(usageResult) > 0:
                data = [
                    [
                        row["extSubId"],
                        row["MDN"],
                        row["BAN"],
                        row["start"],
                        row["end"],
                        int(row["bytesIn"]) + int(row["bytesOut"]),
                        auditDate,
                    ]
                    for _, row in usageResult.iterrows()
                ]

                self.reportingClient.insert_reporting_data(data)


if __name__ == "__main__":
    logger.info("Start the main script")

    mainClient = Main()
    mainClient.reportingClient.create_reporting_table()

    auditDate = date.today() - timedelta(days=1)
    auditRangeStart = datetime.combine(auditDate, time(0, 0, 0))
    auditRangeEnd = datetime.combine(auditDate, time(23, 59, 59))

    auldataSubs = mainClient.get_auldata_subscribers(auditRangeStart, auditRangeEnd)
    mainClient.compare(auldataSubs)

    mainClient.reportingClient.clean_reporting_data()
    logger.info("Finish the main script")
