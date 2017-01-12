# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import logging
import sys
import uuid
import webbrowser
from string import Template

import httplib2
import luigi
from googleapiclient.discovery import build
from luigi import configuration
from oauth2client.client import GoogleCredentials

from etl.settings import get_config

logger = logging.getLogger('luigi-interface')

try:
    import apiclient
    from apiclient import discovery
except ImportError:
    logger.warning("Loading gcloud module without google-api-client installed. Will crash at "
                   "runtime if gcloud functionality is used.")


class GCloudClient:

    def __init__(self, **kwargs):
        self._project_name = kwargs.get("name", "default")
        self._config_name = kwargs.get("config", "default")
        self.project_id = get_config().get_gcloud_project_id()

        self.credentials = GoogleCredentials.get_application_default()

    def http_authorized(self):
        return self.credentials.authorize(httplib2.Http())

    @staticmethod
    def http():
        return httplib2.Http()

    def oauth(self):
        return self.credentials

    def bigquery_api(self, http=None):
        return build('bigquery', 'v2', http=http or self.http_authorized())

    def storage_api(self, http=None):
        return build('storage', 'v1', http=http or self.http_authorized())

    def dataflow_api(self, http=None):
        return build('dataflow', 'v1b3', http=http or self.http_authorized())

    def dataproc_api(self, http=None):
        return build('dataproc', 'v1beta1', http=http or self.http_authorized())

    def logging_api(self, http=None):
        return build('logging', 'v2beta1', http=http or self.http_authorized())

    def datastore_api(self, http=None):
        return build('datastore', 'v1beta2', http=http or self.http_authorized())

    def project_name(self):
        return self._project_name

    def config_name(self):
        return self._config_name

    def get(self, api, name, config={}, default=None):
        key = (api + ".configuration." + self._config_name + "." + name).lower()
        value = config.get(name) or self.config.get(key)
        if value is None:
            if default is None:
                raise ValueError(
                    "Value for " + name + " not found, either in defaults or configuration as key " + key)
            else:
                return default
        return value


default_client = None


class GCloudTask(luigi.Task):
    client = None
    service_name = None
    bigquery_api = None
    uuid = str(uuid.uuid1())[:13]
    _resolved_name = None
    _attempt = 0

    def variables(self):
        return None

    def configuration(self):
        return {}

    def name(self):
        return type(self).__name__

    def resolved_name(self):
        if self._resolved_name is None:
            name = self.name()
            if self.variables() is not None:
                name = Template(self.name()).substitute(self.variables())
            self._resolved_name = name + "__" + self.uuid
        return self._resolved_name

    def job_name(self):
        return self.resolved_name() + "_" + str(self._attempt)

    def start_job(self):
        self._attempt += 1

    def resolved_name(self):
        if self._resolved_name is None:
            name = self.name()
            if self.variables() is not None:
                name = Template(self.name()).substitute(self.variables())
            self._resolved_name = name + "__" + self.uuid
        return self._resolved_name

    def __init__(self, *args, **kwargs):
        self.client = kwargs.get("client") or get_default_client()
        super(GCloudTask, self).__init__(*args, **kwargs)


def set_default_client(gclient):
    global default_client
    default_client = gclient


def load_default_client(project, config):
    global default_client
    gcloud_args_ix = [n for n, l in enumerate(
        sys.argv) if l.startswith('--gcloud')]
    for i in reversed(gcloud_args_ix):
        param = sys.argv[i]
        if param.startswith("--gcloud-project"):
            project = param[17:]
        elif param.startswith("--gcloud-config"):
            config = param[16:]
        else:
            raise RuntimeError(
                "luigi-gcloud only support: --gcloud-project and --gcloud-config")
        del sys.argv[i]
    print("luigi-gcloud will use project: " +
          project + " with configuration: " + config)
    default_client = GCloudClient(name=project, config=config)


def get_default_client():
    global default_client
    if default_client is None:
        default_client = GCloudClient()
    return default_client
