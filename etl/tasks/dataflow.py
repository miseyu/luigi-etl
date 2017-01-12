# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import logging
import select
import subprocess
import time
import hashlib
import re
from os import path
from luigi.contrib.bigquery import BigqueryTarget

from etl import utils
from etl.settings import get_config
from etl.tasks.gcore import GCloudTask
from etl.tasks.copy_storage_to_local import CopyDataflowJarToLocal
from etl.tasks.gstorage import GCSFlagTarget

logger = logging.getLogger('luigi-interface')


class _DataflowJob:

    def _get_job(self):
        job = self.dataflow.projects().jobs().get(projectId=self.project_number,
                                                  jobId=self.job_id).execute()
        if 'currentState' in job:
            logger.info('Google Cloud DataFlow job %s is %s',
                        str(job['name']), str(job['currentState']))
        else:
            logger.info('Google Cloud DataFlow with job_id %s has name %s',
                        self.job_id, str(job['name']))
        return job

    def wait_for_done(self):
        while True:
            if 'currentState' in self.job:
                logger.info("Start waiting for DataFlow process to complete. currentState=%s" % self.job[
                            'currentState'])
                if 'JOB_STATE_DONE' == self.job['currentState']:
                    return True
                elif 'JOB_STATE_FAILED' == self.job['currentState']:
                    raise Exception("Google Cloud Dataflow job " +
                                    str(self.job['name']) + " has failed.")
                elif 'JOB_STATE_CANCELLED' == self.job['currentState']:
                    raise Exception("Google Cloud Dataflow job " +
                                    str(self.job['name']) + " was cancelled.")
                elif 'JOB_STATE_RUNNING' == self.job['currentState']:
                    time.sleep(10)
                else:
                    logger.debug(str(self.job))
                    raise Exception("Google Cloud Dataflow job " + str(self.job['name']) + " was unknown state: " + str(
                        self.job['currentState']))
            else:
                time.sleep(15)

            self.job = self._get_job()

    def get(self):
        return self.job

    def __init__(self, dataflow, project_number, job_id):
        self.dataflow = dataflow
        self.project_number = project_number
        self.job_id = job_id
        self.job = self._get_job()


class _DataflowJava:

    def _line(self, fd):
        if fd == self.proc.stderr.fileno():
            return self.proc.stderr.readline()
        if fd == self.proc.stdout.fileno():
            return self.proc.stdout.readline()
        return None

    @staticmethod
    def _extract_job(line):

        _ASCII_re = re.compile(r'\A[\x00-\x7f]*\Z')

        def is_ascii_str(text):
            return isinstance(text, str) and _ASCII_re.match(text)
        if line is not None and is_ascii_str(line):
            if line.startswith("Submitted job: "):
                return line[15:-1]
        return None

    def wait_for_done(self):
        reads = [self.proc.stderr.fileno(), self.proc.stdout.fileno()]
        logger.info("Start waiting for DataFlow process to complete.")
        while self.proc.poll() is None:
            ret = select.select(reads, [], [], 5)
            if ret is not None:
                for fd in ret[0]:
                    line = self._line(fd)
                    self.job_id = self._extract_job(line)
                    if self.job_id is not None:
                        return self.job_id
                    else:
                        logger.debug(line[:-1])
            else:
                logger.info("Waiting for DataFlow process to complete.")

    def get(self):
        return self.job_id

    def __init__(self, cmd):
        self.job_id = None
        self.proc = subprocess.Popen(
            cmd, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


class DataFlowJavaTask(GCloudTask):
    service_name = 'dataflow'

    def output(self):
        return utils.generate_gcs_touch_target(self.job_name, self.identify_key())

    @property
    def job_name(self):
        raise NotImplementedError("subclass should define dataflow")

    @property
    def input_query(self):
        raise NotImplementedError("subclass should define input query")

    def output_target(self):
        raise NotImplementedError("subclass should define input query")

    def identify_key(self):
        return hashlib.sha512(",".join(self._build_cmd())).hexdigest()

    def run(self):
        cmd = self._build_cmd()
        self._execute_track(cmd)

    def _execute_track(self, cmd):
        http = self.client.http_authorized()
        dataflow_api = self.client.dataflow_api(http)
        logger.debug("DataFlow process: " + str(cmd))
        job_id = _DataflowJava(cmd).wait_for_done()
        _DataflowJob(dataflow_api, self.client.project_id,
                     job_id).wait_for_done()
        self._success()

    def _success(self):
        marker = self.output()
        if hasattr(marker, "touch") and callable(getattr(marker, "touch")):
            logger.info("Writing marker file " + str(marker))
            marker.touch()
        return

    def _build_cmd(self):
        version = get_config().get_dataflow_version()
        command = [
            "java",
            "-cp",
            path.dirname(path.abspath(__file__)) + (
                "/../../tmp/dataflow/dataflow-%s-all.jar" % version),
            'etl.dataflow.JobExecutor',
            self.job_name,
            "--project=" + get_config().get_gcloud_project_id(),
            "--stagingLocation=gs://storage/dataflow/JobExecutor/" + version,
            "--runner=BlockingDataflowPipelineRunner",
            "--inputType=QUERY",
            "--input=%s" % self.input_query,
            "--output=%s:%s.%s" % (self.output_target().table.project_id, self.output_target(
            ).table.dataset_id, self.output_target().table.table_id)
        ]

        for attr, value in self.variables().items():
            command.append("--" + attr + "=" + value)

        return command
