# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function, division

from dateutil.relativedelta import relativedelta
import datetime
import time
import hashlib
import re
import json
import uuid as uuid_module
import collections
import os
from luigi.parameter import _DatetimeParameterBase

from etl.tasks.gstorage import GCSFlagTarget


class DateTimeParameter(_DatetimeParameterBase):
    """
    Parameter whose value is a :py:class:`~datetime.datetime` specified to the minute.
    A DateMinuteParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date and time specified to the minute. For example, ``2013-07-10T19:07:08``
    The interval parameter can be used to clamp this parameter to every N minutes, instead of every minute.
    """

    date_format = '%Y-%m-%dT%H:%M:%S'
    _timedelta = datetime.timedelta(seconds=1)


def strptime_by_format(target, format):
    return datetime.datetime.strptime(target, format)


def stptime_by_bigquery_format(target):
    return datetime.datetime.strptime(target.replace(' UTC', ''), "%Y-%m-%d %H:%M:%S")


def strptime(target):
    return strptime_by_format(target, "%Y-%m-%dT%H:%M:%S")


def strftime_by_format(target_at, format):
    if target_at:
        return datetime.datetime.strftime(target_at, format)


def strftime(target_at):
    return strftime_by_format(target_at, "%Y-%m-%dT%H:%M:%S")


def truncate_datetime(target_at, format):
    return datetime.datetime.strptime(
        datetime.datetime.strftime(target_at, format), "%Y-%m-%dT%H:%M:%S"
    )


def convert_jst_to_utc(target_at):
    return target_at - relativedelta(hours=9)


def convert_utc_to_jst(target_at):
    return target_at + relativedelta(hours=9)


def utc_now():
    return convert_jst_to_utc(datetime.datetime.now())


def jst_now():
    return datetime.datetime.now()


def jst_yesterday(target_at):
    return target_at - relativedelta(day=1)


def jst_beginning_of_day(target_at):
    return truncate_datetime(target_at, '%Y-%m-%dT00:00:00')


def jst_end_of_day(target_at):
    return truncate_datetime(target_at, '%Y-%m-%dT23:59:59')


def utc_beginning_of_day(target_at):
    return convert_jst_to_utc(jst_beginning_of_day(target_at))


def utc_end_of_day(target_at):
    return convert_jst_to_utc(jst_end_of_day(target_at))


def bq_date_range_format(target_at):
    return datetime.datetime.strftime(target_at, "%Y-%m-%d")


def bq_datetime_format(target_at):
    return datetime.datetime.strftime(target_at, "%Y-%m-%d %H:%M:%S")


def master_folder_date_format(target_at):
    if os.environ.get('STREAM_ID'):
        return os.environ.get('STREAM_ID')
    else:
        return datetime.datetime.strftime(target_at, "%Y%m%d%H")


def flatten(nested_list):
    result = []
    for element in nested_list:
        if isinstance(element, collections.Iterable) and not isinstance(element, basestring):
            result.extend(flatten(element))
        else:
            result.append(element)
    return result


def hash_digest(*target):
    return hashlib.sha224(",".join([str(detail) for detail in flatten(target)])).hexdigest()


def uuid():
    return str(uuid_module.uuid1()).replace('-', '')


def generate_query_hash(query):
    return hash_digest(json.dumps(query))


def generate_bq_table_suffix_keys(started_at, ended_at):
    """
        params: started_at UTC
        params: ended_at UTC
    """
    assert started_at < ended_at, 'started_at should ended_at small.'

    truncate_started_at = truncate_datetime(started_at, '%Y-%m-%dT00:00:00')
    truncate_ended_at = truncate_datetime(ended_at, '%Y-%m-%dT23:59:59')
    diff = truncate_ended_at - truncate_started_at
    result = []
    for index in range(0, diff.days + 1):
        if (truncate_started_at + datetime.timedelta(days=index)) > truncate_ended_at:
            break
        result.append(datetime.datetime.strftime(
            truncate_started_at + datetime.timedelta(days=index), "%Y%m%d"))
    return result


def generate_bq_tables(table_id, started_at, ended_at):
    return ['[' + table_id + suffix + ']' for suffix in generate_bq_table_suffix_keys(started_at, ended_at)]


def generate_identify_key(*args):
    data = []
    for arg in args:
        if not arg:
            continue
        if isinstance(arg, datetime.datetime):
            data.append(strftime(arg))
        else:
            data.append(str(arg))
    return hash_digest(data)


def convert_camelcase_to_snakecase(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def generate_temp_table_id(clazz, *args):
    params = []
    for arg in args:
        if type(arg) == datetime.datetime:
            params.append(strftime(arg))
        else:
            params.append(arg)

    return convert_camelcase_to_snakecase(clazz.__name__) + hash_digest(params)


def generate_bml_web_union_table(owner_id, started_at, ended_at):
    bml_log_table_id = "private_dmp_%s.private_dmp_user_bml_logs" % owner_id
    web_log_table_id = "private_dmp_%s.private_dmp_user_web_logs" % owner_id
    bml_log_tables = generate_bq_tables(bml_log_table_id, started_at, ended_at)
    web_log_tables = generate_bq_tables(web_log_table_id, started_at, ended_at)
    return "%s, %s" % (",".join(bml_log_tables), ",".join(web_log_tables))


def generate_aggregate_key(timezone="jst", summary_type="secondly", aggregate_date_key="created_at", delimiter='-', space='T'):
    add_hour = "0" if timezone == 'utc' else "9"
    key_format = "LPAD(STRING(%s(DATE_ADD(%s,+%s,'HOUR'))), %s, '0' ), '%s'"
    keys = {
        'YEAR': key_format % ('YEAR', aggregate_date_key, add_hour, '4', u''),
        'MONTH': key_format % ('MONTH', aggregate_date_key, add_hour, '2', u''),
        'DAY': key_format % ('DAY', aggregate_date_key, add_hour, '2', u''),
        'HOUR': key_format % ('HOUR', aggregate_date_key, add_hour, '2', u''),
        'MINUTE': key_format % ('MINUTE', aggregate_date_key, add_hour, '2', u''),
        'SECOND': key_format % ('SECOND', aggregate_date_key, add_hour, '2', u'')
    }

    if summary_type == 'hourly':
        keys['MINUTE'] = "'00'"
        keys['SECOND'] = "'00'"
    elif summary_type == 'daily':
        keys['HOUR'] = "'00'"
        keys['MINUTE'] = "'00'"
        keys['SECOND'] = "'00'"
    elif summary_type == 'minutely':
        keys['SECOND'] = "'00'"

    result = 'CONCAT(' + keys['YEAR']
    result += ',' + "'%s'" % delimiter
    result += ',' + keys['MONTH']
    result += ',' + "'%s'" % delimiter
    result += ',' + keys['DAY']
    result += ',' + "'%s'" % space
    result += ',' + keys['HOUR']
    result += ',' + "':'"
    result += ',' + keys['MINUTE']
    result += ',' + "':'"
    result += ',' + keys['SECOND']
    result += ')'
    return result


def generate_gcs_touch_target(*args):
    version = os.environ.get('GCS_TOUCH_VERSION') if os.environ.get(
        'GCS_TOUCH_VERSION') else '1.0.24'
    gcs_path = 'gs://oz-analytics-etl-storage/touch/%s' % version
    for arg in args:
        gcs_path += "/" + str(arg)
    return GCSFlagTarget(gcs_path)


def parse_json_str(data):
    json_object = None
    try:
        json_object = json.loads(data)
    except ValueError, e:
        return False
    return json_object


def parse_json_stream(data):
    json_object = None
    try:
        json_object = json.load(data)
    except ValueError, e:
        return None
    return json_object


def generate_dataflow_task_id(task_class):
    now = datetime.datetime.now()
    epoch = int(time.mktime(now.timetuple()))
    timestamp = epoch * 1000000 + now.microsecond
    return str(str(task_class.__name__ + str(timestamp)).replace("_", "").replace(".", "").lower())


def build_bigquery_schema(*tuples):
    return ",".join(["%s:%s" % key_value for key_value in tuples])
