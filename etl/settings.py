# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import os
import luigi
import slackweb


class Config(object):

    def get_slack(self, slack_type='webhook_url'):
        luigi_config = luigi.configuration.get_config()
        webhook_url = luigi_config.get(
            'slack', slack_type, os.environ.get('WEBHOOK_URL'))
        return slackweb.Slack(webhook_url)

    def get_notify_slack(self):
        luigi_config = luigi.configuration.get_config()
        webhook_url = luigi_config.get(
            'slack', 'notify_webhook_url', os.environ.get('NOTIFY_WEBHOOK_URL'))
        return slackweb.Slack(webhook_url)

    def get_dataflow_version(self):
        luigi_config = luigi.configuration.get_config()
        return luigi_config.get('dataflow', 'version', os.environ.get('DATAFLOW_VERSION'))

    def get_td_database_name(self):
        luigi_config = luigi.configuration.get_config()
        return luigi_config.get('td', 'database_name', os.environ.get('TD_DATABASE_NAME'))

    def get_gcloud_project_id(self):
        luigi_config = luigi.configuration.get_config()
        return luigi_config.get('dataflow', 'projectId')

    def get_id_sync_dataset(self):
        return 'master_id_sync_production'

    def get_private_dmp_dataset_format(self):
        return "private_dmp_%s"

    def get_dmp_dashboard_config(self):
        luigi_config = luigi.configuration.get_config()
        return dict(base_url=luigi_config.get('dmp_dashboard', 'base_url'),
                    permanent_token=luigi_config.get('dmp_dashboard', 'permanent_token'))

    def get_platform_console_config(self):
        luigi_config = luigi.configuration.get_config()
        return dict(base_url=luigi_config.get('platform_console', 'base_url'),
                    api_key=luigi_config.get('platform_console', 'api_key'),
                    basic_user=luigi_config.get(
                        'platform_console', 'basic_user'),
                    basic_pass=luigi_config.get('platform_console', 'basic_pass'))

    def get_datastore_namespace(self):
        luigi_config = luigi.configuration.get_config()
        return luigi_config.get('datastore', 'namespace', os.environ.get('DATASTORE_NAMESPACE'))


class ConfigLoader(object):
    _instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = cls(*args, **kwargs)
            cls._instance.load_default()
        return cls._instance

    def __init__(self):
        self.config = None

    def get_config(self):
        return self.config

    def load_default(self):
        self.config = Config()


def get_config():
    return ConfigLoader.instance().get_config()
