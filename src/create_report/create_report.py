from datetime import date, datetime, time, timedelta, timezone
from typing import TYPE_CHECKING

from . import settings
from .utils import (
    cleanup_auldata_lake_reporting_table,
    compare_and_generate_report,
    get_auldata_subscribers,
    get_mysql_client,
    init_auldata_lake_reporting_table,
)

if TYPE_CHECKING:
    import pandas as pd
    from sqlalchemy.engine.base import Engine


def create_report() -> None:
    reporting_client: Engine = get_mysql_client()
    init_auldata_lake_reporting_table(reporting_client)
    audit_date: date = datetime.now(timezone.utc).today() - timedelta(days=1)
    audit_range_start: datetime = datetime.combine(audit_date, time(0, 0, 0))
    audit_range_end: datetime = datetime.combine(audit_date, time(23, 59, 59))

    auldata_subs: pd.DataFrame = get_auldata_subscribers(
        audit_range_start, audit_range_end, settings.OFFER_NAME
    )
    compare_and_generate_report(reporting_client, auldata_subs, audit_date)
    cleanup_auldata_lake_reporting_table(reporting_client)
