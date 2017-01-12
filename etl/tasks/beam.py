# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import logging
import luigi
import time
import os

import apache_beam as beam
from apache_beam.utils import dependency
from apache_beam.transforms import GroupByKey
from apache_beam.runners import create_runner, DataflowPipelineRunner
from apache_beam import Pipeline
from apache_beam.utils.options import PipelineOptions, GoogleCloudOptions, StandardOptions, WorkerOptions, SetupOptions

from etl.settings import get_config
from etl import utils

logger = logging.getLogger('luigi-interface')


class DataflowPythonTask(luigi.Task):

    def run(self):
        pipeline = self.create_pipeline()
        self.build_pipeline(pipeline)
        pipeline.run()
        self._success()

    @property
    def unique_key(self):
        raise NotImplementedError("unique_key should override")

    @property
    def num_workers(self):
        workers_count = int(os.environ.get('BEAM_WORKER_COUNT')) if os.environ.get(
            'BEAM_WORKER_COUNT') else 2
        return workers_count

    @property
    def max_num_workers(self):
        return self.num_workers + 2

    @property
    def disk_size_gb(self):
        return 200

    @property
    def machine_type(self):
        return 'n1-standard-2'

    @property
    def job_name(self):
        return (self.__class__.__name__ + str(time.time())).replace("_", "").replace(".", "").lower()

    def build_pipeline(self, pipeline):
        raise NotImplementedError("build_pipeline should override.")

    def create_pipeline(self):
        options = PipelineOptions()

        runner = str('BlockingDataflowPipelineRunner')
        #runner = str('DirectPipelineRunner')
        google_cloud_options = options.view_as(GoogleCloudOptions)
        google_cloud_options.project = get_config().get_gcloud_project_id()
        google_cloud_options.job_name = self.job_name
        google_cloud_options.staging_location = 'gs://storage/dataflow/JobExecutor/python/1.0.1/%s' % self.job_name
        google_cloud_options.temp_location = 'gs://storage/dataflow/JobExecutor/python/1.0.1/temp'

        worker_options = options.view_as(WorkerOptions)
        worker_options.num_workers = self.num_workers
        worker_options.max_num_workers = self.max_num_workers
        worker_options.disk_size_gb = self.disk_size_gb
        worker_options.machine_type = self.machine_type
        options.view_as(SetupOptions).save_main_session = True
        options.view_as(SetupOptions).requirements_file = os.path.join(
            '.', 'requirements_dataflow.txt')
        options.view_as(SetupOptions).setup_file = os.path.join(
            '.', 'setup.py')
        return Pipeline(runner=runner, options=options)

    def _success(self):
        self.output().touch()

    def output(self):
        return utils.generate_gcs_touch_target(self.__class__.__name__, self.unique_key)
