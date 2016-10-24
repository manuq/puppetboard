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

def _iter_dates(days_length):
    one_day = timedelta(days=1)
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC()) + one_day
    days_list = list(today - one_day * i for i in range(days_length + 1))
    return zip(days_list[1:], days_list)

def _parse_output(day, output):
    parsed = {
        'day': day,
        'changed': 0,
        'unchanged': 0,
        'failed': 0,
    }
    for out in output:
        if out['status'] == 'changed':
            parsed['changed'] = out['count']
        elif out['status'] == 'unchanged':
            parsed['unchanged'] = out['count']
        elif out['status'] == 'failed':
            parsed['failed'] = out['count']
    return parsed

def get_daily_chart(db, certname=None):
    result = []
    query = None
    if certname is None:
        query = QUERY_STATUS_COUNT_ALL
    else:
        query = QUERY_STATUS_COUNT_CERTNAME
    for start, end in _iter_dates(days_length=8):
        day = start.strftime(DATE_FORMAT)
        start_json = start.strftime(DATETIME_FORMAT)
        end_json = end.strftime(DATETIME_FORMAT)
        query_info = {'start': start_json, 'end': end_json, 'certname': certname}
        output = db._query('reports', query=query.format(**query_info))
        result.append(_parse_output(day, output))
    result.reverse()
    return result
