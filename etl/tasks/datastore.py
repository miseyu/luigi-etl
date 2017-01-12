# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import json
import requests
import luigi
from luigi import LocalTarget
from os import path
from google.cloud import datastore

from etl.tasks.gstorage import GCSFlagTarget
from etl import utils
from etl.settings import get_config
from etl.tasks.summary import SummaryCampaignSegmentTask
from etl.tasks.dmp_dashboard import DMPCampaignExportTask

_client = datastore.client.Client(project=get_config().get_gcloud_project_id(),
                                  namespace=get_config().get_datastore_namespace())


class DatastoreMultiPutTask(luigi.Task):

    def create_entities(self):
        raise NotImplementedError()

    def run(self):
        entities = self.create_entities()
        if entities:
            _client.put_multi(entities)
        marker = self.output()
        marker.touch()

    def output(self):
        return utils.generate_gcs_touch_target(self.__class__.__name__, utils.strftime(utils.jst_now()))
