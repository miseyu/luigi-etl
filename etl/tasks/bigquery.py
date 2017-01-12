# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import logging
import luigi
import json
import time
import datetime
from os import path
from luigi.contrib.bigquery import (
    BigqueryRunQueryTask,
    BigqueryTarget,
    BigqueryLoadTask,
    CreateDisposition,
    WriteDisposition,
    SourceFormat,
    BQTable,
    BigqueryClient,
    QueryMode
)

from etl.settings import get_config
from etl.tasks.gstorage import GCSFlagTarget
from etl import utils

logger = logging.getLogger('luigi-interface')


types_map = {
    'INTEGER': "integer",
    'FLOAT': "float",
    'BOOLEAN': "boolean",
    'STRING': "string",
    'TIMESTAMP': "date",
}


def transform_row(row, fields):
    column_index = 0
    row_data = {}

    for cell in row["f"]:
        field = fields[column_index]
        cell_value = cell['v']

        if cell_value is None:
            pass
        # Otherwise just cast the value
        elif field['type'] == 'INTEGER':
            cell_value = int(cell_value)
        elif field['type'] == 'FLOAT':
            cell_value = float(cell_value)
        elif field['type'] == 'BOOLEAN':
            cell_value = cell_value.lower() == "true"
        elif field['type'] == 'TIMESTAMP':
            cell_value = utils.strftime(
                datetime.datetime.fromtimestamp(float(cell_value)))

        row_data[field["name"]] = cell_value
        column_index += 1

    return row_data


def _get_query_results(jobs, project_id, job_id, start_index):
    query_reply = jobs.getQueryResults(
        projectId=project_id, jobId=job_id, startIndex=start_index).execute()
    logging.debug('query_reply %s', query_reply)
    if not query_reply['jobComplete']:
        time.sleep(10)
        return _get_query_results(jobs, project_id, job_id, start_index)

    return query_reply


class CustomBigqueryClient(BigqueryClient):

    def run_job_with_rows(self, job):
        new_job = self.client.jobs().insert(
            projectId=job["projectId"], body=job).execute()
        job_id = new_job['jobReference']['jobId']
        current_row = 0
        query_reply = _get_query_results(self.client.jobs(), project_id=job[
                                         "projectId"], job_id=job_id, start_index=current_row)

        logger.debug("bigquery replied: %s", query_reply)

        rows = []

        while ("rows" in query_reply) and current_row < query_reply['totalRows']:
            for row in query_reply["rows"]:
                rows.append(transform_row(
                    row, query_reply["schema"]["fields"]))

            current_row += len(query_reply['rows'])
            query_reply = self.client.jobs().getQueryResults(
                projectId=job["projectId"], jobId=job_id, startIndex=current_row).execute()

        columns = [{'name': f["name"],
                    'friendly_name': f["name"],
                    'type': types_map.get(f['type'], "string")} for f in query_reply["schema"]["fields"]]

        data = {
            "columns": columns,
            "rows": rows
        }
        return data


class BigqueryRunQueryDownloadTask(BigqueryRunQueryTask):

    @property
    def udf(self):
        return None

    @property
    def query_mode(self):
        """The query mode. See :py:class:`QueryMode`."""
        return QueryMode.INTERACTIVE

    @property
    def file_format(self):
        return 'json'

    def run(self):
        query = self.query
        output = self.output()
        assert isinstance(
            output, luigi.LocalTarget), 'Output should be a LocalTarget target, not %s' % (output)
        assert query, 'No query was provided'

        bq_client = CustomBigqueryClient()

        logger.info('Launching Query')
        logger.info('Query SQL: %s', query)

        query_setting = {
            'query': query
        }
        if self.udf:
            query_setting['userDefinedFunctionResources'] = [
                {
                    'inlineCode': self.udf
                }
            ]

        job = {
            'projectId': get_config().get_gcloud_project_id(),
            'configuration': {
                'query': query_setting
            }
        }

        result = bq_client.run_job_with_rows(job)
        with self.output().open('w') as out_file:
            if self.file_format == 'json':
                out_file.write(json.dumps(result['rows']))
            elif self.file_format in ('csv', 'tsv'):
                if not result:
                    out_file.write('')
                    return
                separator = ',' if self.file_format == 'csv' else '\t'
                headers = result['rows'][0].keys()
                headers.sort()
                header_index = dict()
                out_file.write(separator.join(headers) + '\n')
                for row in result['rows']:
                    for key in row.keys():
                        row[key] = row[key] if row[key] else '-'
                    out_file.write(separator.join(
                        [row[header].encode('ascii', 'ignore') for header in headers]) + '\n')


class BigqueryCreateTableTask(luigi.Task):
    dataset_id = luigi.Parameter(default=None)

    @property
    def project_id(self):
        return get_config().get_gcloud_project_id()

    @property
    def table_name(self):
        raise NotImplementedError()

    @property
    def table_id(self):
        return self.table_name

    @property
    def fields(self):
        raise NotImplementedError()

    def create_table(self):
        body = {
            'schema': {'fields': self.fields},
            'tableReference': {
                'projectId': self.output().table.project_id,
                'datasetId': self.output().table.dataset_id,
                'tableId': self.output().table.table_id
            }
        }
        self.output().client.client.tables().insert(projectId=self.output().table.project_id,
                                                    datasetId=self.output().table.dataset_id,
                                                    body=body).execute()

    def run(self):
        output = self.output()
        assert isinstance(
            output, BigqueryTarget), 'Output must be a bigquery target, not %s' % (output)

        fields = self.fields
        assert fields, 'No fields was provided'

        logger.info('Create table')
        logger.info('Destination: %s', output)

        self.create_table()

    def output(self):
        return BigqueryTarget(project_id=self.project_id, dataset_id=self.dataset_id, table_id=self.table_id)


class CustomBigqueryRunQueryTask(BigqueryRunQueryTask):

    @property
    def udf(self):
        return None

    @property
    def query_mode(self):
        """The query mode. See :py:class:`QueryMode`."""
        return QueryMode.INTERACTIVE

    def run(self):
        output = self.output()
        assert isinstance(
            output, BigqueryTarget), 'Output should be a bigquery target, not %s' % (output)

        query = self.query
        assert query, 'No query was provided'

        bq_client = output.client

        logger.info('Launching Query')
        logger.info('Query destination: %s (%s)',
                    output, self.write_disposition)
        logger.info('Query SQL: %s', query)

        query_setting = {
            'query': query,
            'priority': self.query_mode,
            'destinationTable': {
                'projectId': output.table.project_id,
                'datasetId': output.table.dataset_id,
                'tableId': output.table.table_id,
            },
            'allowLargeResults': True,
            'createDisposition': self.create_disposition,
            'writeDisposition': self.write_disposition,
            'flattenResults': self.flatten_results
        }
        if self.udf:
            query_setting['userDefinedFunctionResources'] = [
                {
                    'inlineCode': self.udf
                }
            ]

        job = {
            'projectId': output.table.project_id,
            'configuration': {
                'query': query_setting
            }
        }

        bq_client.run_job(output.table.project_id, job,
                          dataset=output.table.dataset)


class AppendBigqueryTarget(BigqueryTarget):

    def __init__(self, project_id, dataset_id, table_id, identify_key, client=None):
        super(AppendBigqueryTarget, self).__init__(
            project_id, dataset_id, table_id, client)
        self.touch_target = utils.generate_gcs_touch_target(
            "appendBigqueryTarget", utils.hash_digest(identify_key))

    def exists(self):
        if self.touch_target.exists():
            return True
        else:
            return False


class AppendBigqueryRunQueryTask(CustomBigqueryRunQueryTask):

    is_status = False

    @property
    def write_disposition(self):
        return WriteDisposition.WRITE_APPEND

    def complete(self):
        return self.is_status

    def run(self):
        super(AppendBigqueryRunQueryTask, self).run()
        self.output().touch_target.touch()
        self.is_status = True


class TruncateBigqueryTarget(AppendBigqueryTarget):
    pass


class TruncateBigqueryRunQueryTask(CustomBigqueryRunQueryTask):

    is_status = False

    @property
    def write_disposition(self):
        return WriteDisposition.WRITE_TRUNCATE

    def complete(self):
        return self.is_status

    def run(self):
        super(TruncateBigqueryRunQueryTask, self).run()
        self.output().touch_target.touch()
        self.is_status = True


class LogTransferByPrefixKeyQuery(AppendBigqueryRunQueryTask):
    input_dataset = luigi.Parameter(default=None)
    input_table_id = luigi.Parameter(default=None)
    output_dataset = luigi.Parameter(default=None)
    output_table_id = luigi.Parameter(default=None)
    prefix_key = luigi.Parameter(default=None)
    output_fields = luigi.ListParameter(default=None)

    @property
    def query(self):
        return """
            SELECT
                %s
            FROM
                [%s.%s]
            WHERE
                prefix_key = '%s'

        """ % (','.join(self.output_fields), self.input_dataset, self.input_table_id, self.prefix_key)

    def output(self):
        return AppendBigqueryTarget(project_id=get_config().get_gcloud_project_id(), dataset_id=self.output_dataset, table_id="%s%s" % (self.output_table_id, self.prefix_key), identify_key=utils.strftime(utils.jst_now()))


class CommonRunQueryDownloadTask(BigqueryRunQueryDownloadTask):
    query = luigi.Parameter(default=None)

    def output(self):
        return luigi.LocalTarget(path.dirname(path.abspath(__file__)) +
                                 ("/../../tmp/%s/%s/%s.json" % (
                                     self.__class__.__name__,
                                     utils.master_folder_date_format(
                                         utils.jst_now()),
                                     utils.hash_digest(self.query)
                                 ))
                                 )


class BigqueryStandardRunQueryTask(BigqueryRunQueryTask):

    @property
    def query_mode(self):
        """The query mode. See :py:class:`QueryMode`."""
        return QueryMode.INTERACTIVE

    def run(self):
        output = self.output()
        assert isinstance(
            output, BigqueryTarget), 'Output should be a bigquery target, not %s' % (output)

        query = self.query
        assert query, 'No query was provided'

        bq_client = output.client

        logger.info('Launching Standard SQL Query')
        logger.info('Query destination: %s (%s)',
                    output, self.write_disposition)
        logger.info('Query SQL: %s', query)

        query_setting = {
            'query': query,
            'priority': self.query_mode,
            'destinationTable': {
                'projectId': output.table.project_id,
                'datasetId': output.table.dataset_id,
                'tableId': output.table.table_id,
            },
            'allowLargeResults': True,
            'useLegacySql': False,
            'createDisposition': self.create_disposition,
            'writeDisposition': self.write_disposition,
            'flattenResults': self.flatten_results
        }
        job = {
            'projectId': output.table.project_id,
            'configuration': {
                'query': query_setting
            }
        }

        bq_client.run_job(output.table.project_id, job,
                          dataset=output.table.dataset)


class AppendBigqueryStandardRunQueryTask(BigqueryStandardRunQueryTask):

    is_status = False

    @property
    def write_disposition(self):
        return WriteDisposition.WRITE_APPEND

    def complete(self):
        return self.is_status

    def run(self):
        super(AppendBigqueryStandardRunQueryTask, self).run()
        self.output().touch_target.touch()
        self.is_status = True
