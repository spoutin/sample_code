"""Python script to run the entire program."""
from app.utils.time_utils import get_datetime_range_today
from app.main import (
    get_reporting_client,
    get_audit_collection,
    init_auldata_leak_reporting_table,
    get_auldata_subscribers,
    compare_auldata,
    auldata_leak_reporting_table_cleanup,
)

if __name__ == "__main__":
    reporting_client = get_reporting_client()
    init_auldata_leak_reporting_table(reporting_client)

    audit_collection = get_audit_collection()
    audit_range_start, audit_range_end = get_datetime_range_today()
    auldata_subs = get_auldata_subscribers(
        audit_collection, audit_range_start, audit_range_end
    )

    compare_auldata(auldata_subs, reporting_client)
    auldata_leak_reporting_table_cleanup(reporting_client)
