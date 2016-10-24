from datetime import datetime, timedelta
from pypuppetdb.utils import UTC

DATE_FORMAT = "%Y-%m-%d"
DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"

QUERY_STATUS_COUNT_ALL = ('["extract", [["function","count"], "status"], '
                          '["and", [">=","start_time","{start}"], ["<","start_time","{end}"]], '
                          '["group_by", "status"]]')

QUERY_STATUS_COUNT_CERTNAME = ('["extract", [["function","count"], "status"], '
                               '["and", ["=","certname","{certname}"], [">=","start_time","{start}"], ["<","start_time","{end}"]], '
                               '["group_by", "status"]]')

def _iter_dates(days_number):
    """Return a list of datetime pairs AB, BC, CD, ... that represent the
       24hs time ranges of today (until this midnight) and the
       previous days.
    """
    one_day = timedelta(days=1)
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC()) + one_day
    days_list = list(today - one_day * i for i in range(days_number + 1))
    return zip(days_list[1:], days_list)

def _format_report_data(day, query_output):
    """Format the output of the query to a simpler dict."""
    result = {'day': day, 'changed': 0, 'unchanged': 0, 'failed': 0}
    for out in query_output:
        if out['status'] == 'changed':
            result['changed'] = out['count']
        elif out['status'] == 'unchanged':
            result['unchanged'] = out['count']
        elif out['status'] == 'failed':
            result['failed'] = out['count']
    return result

def get_daily_reports_chart(db, days_number, certname=None):
    """Return the sum of each report status (changed, unchanged, failed)
       per day, for today and the previous N days.

    This information is used to present a chart.

    :param days_number: How many days to sum, including today.
    :param certname: If certname is passed, only the reports of that
    certname will be added.  If certname is not passed, all reports in
    the database will be considered.
    """
    result = []
    query = QUERY_STATUS_COUNT_ALL if certname is None else QUERY_STATUS_COUNT_CERTNAME
    for start, end in reversed(_iter_dates(days_number)):
        day = start.strftime(DATE_FORMAT)
        query_info = {
            'start': start.strftime(DATETIME_FORMAT),
            'end': end.strftime(DATETIME_FORMAT),
            'certname': certname,
        }
        output = db._query('reports', query=query.format(**query_info))
        result.append(_format_report_data(day, output))
    return result
