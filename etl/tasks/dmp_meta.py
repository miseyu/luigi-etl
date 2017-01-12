# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import json
import requests
from os import path
import luigi
from luigi import LocalTarget

from etl.tasks.gstorage import GCSFlagTarget
from etl import utils
from etl.settings import get_config


class ConsoleClientException(Exception):
    pass


class Client(object):

    def __init__(self, base_url, access_key, secret_key):
        self.base_url = base_url
        self.access_key = access_key
        self.secret_key = secret_key

    def get(self, path, params):
        headers = {
            'X-HTTP-ACCESS-KEY': self.access_key,
            'X-HTTP-SECRET-KEY': self.secret_key
        }
        res = requests.get(self.base_url + path,
                           headers=headers, params=params)
        return self.__parse_response(res)

    def post(self, path, data):
        headers = {
            'X-HTTP-ACCESS-KEY': self.access_key,
            'X-HTTP-SECRET-KEY': self.secret_key
        }
        res = requests.post(self.base_url + path,
                            headers=headers, data=json.dumps(data))
        return self.__parse_response(res)

    def __parse_response(self, res):
        res.raise_for_status()
        result = res.json()
        return result['data']


class DMPMetaQueryTask(luigi.Task):

    """
    :param resource_path: rest url resource_path::
        /programs
    :param para: query_params::
            query_params = {
                'started_at': '2016-01-12 12:00:00',
                'ended_at': '2016-01-15 12:00:00'
            }
    """

    @property
    def resource_path(self):
        raise NotImplementedError()

    @property
    def query_params(self):
        raise NotImplementedError()

    def run(self):
        meta_config = get_config().get_meta_serivce_config()
        client = Client(base_url=meta_config['base_url'], access_key=meta_config[
                        'access_key'], secret_key=meta_config['secret_key'])
        res = client.get(self.resource_path, self.query_params)
        with self.output().open("w") as fout:
            fout.write(json.dumps(res['items']))


class DMPMetaPostTask(luigi.Task):

    @property
    def resource_path(self):
        raise NotImplementedError()

    @property
    def post_data(self):
        raise NotImplementedError()

    def run(self):
        meta_config = get_config().get_meta_serivce_config()
        client = Client(base_url=meta_config['base_url'], access_key=meta_config[
                        'access_key'], secret_key=meta_config['secret_key'])
        client.post(self.resource_path, self.post_data)
        marker = self.output()
        marker.touch()


class DMPMetaProgramHistoryQueryTask(DMPMetaQueryTask):
    program_id = luigi.Parameter(default=None)
    broadcaster_identify_key = luigi.Parameter(default=None)
    started_at = utils.DateTimeParameter(default=None)
    ended_at = utils.DateTimeParameter(default=None)

    @property
    def resource_path(self):
        return '/program_histories'

    @property
    def query_params(self):
        params = {
            'program_id': self.program_id,
            'broadcaster_identify_key': self.broadcaster_identify_key,
            'started_at': utils.strftime(self.started_at),
            'ended_at': utils.strftime(self.ended_at)
        }
        return params

    def output(self):
        identify_key = utils.generate_identify_key(
            self.program_id, self.broadcaster_identify_key, self.started_at, self.ended_at)
        return LocalTarget(path.dirname(path.abspath(__file__)) + ("/../../tmp/dmp_meta_master/%s/%s/%s.json" % (utils.master_folder_date_format(utils.jst_now()), identify_key, self.resource_path)))


class DMPMetaProgramHistoryTask(DMPMetaPostTask):
    identify_key = luigi.Parameter(default=None)
    broadcaster_identify_key = luigi.Parameter(default=None)
    started_at = luigi.Parameter(default=None)
    ended_at = luigi.Parameter(default=None)
    genre_id = luigi.Parameter(default=None)
    title = luigi.Parameter(default=None)
    tags = luigi.ListParameter(default=None)

    @property
    def resource_path(self):
        return '/program_histories'

    @property
    def post_data(self):
        return {
            'identify_key': self.identify_key,
            'broadcaster_identify_key': self.broadcaster_identify_key,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'genre_id': self.genre_id,
            'title': self.title,
            'tags': self.tags
        }

    def output(self):
        return utils.generate_gcs_touch_target('DMPMetaProgramHistoryTask', self.identify_key, utils.master_folder_date_format(utils.jst_now()))


class DMPMetaCmHistoryTask(DMPMetaPostTask):
    cm_identify_key = luigi.Parameter(default=None)
    program_identify_key = luigi.Parameter(default=None)
    started_at = luigi.Parameter(default=None)
    ended_at = luigi.Parameter(default=None)
    company = luigi.Parameter(default=None)
    product = luigi.Parameter(default=None)
    setting = luigi.Parameter(default=None)
    talent = luigi.Parameter(default=None)
    remarks = luigi.Parameter(default=None)
    memo = luigi.Parameter(default=None)

    @property
    def resource_path(self):
        return '/cm_histories'

    @property
    def post_data(self):
        return {
            'cm_identify_key': self.cm_identify_key,
            'program_identify_key': self.program_identify_key,
            'started_at': self.started_at,
            'ended_at': self.ended_at,
            'company': self.company,
            'product': self.product,
            'setting': self.setting,
            'talent': self.talent,
            'remarks': self.remarks,
            'memo': self.memo
        }

    def output(self):
        return utils.generate_gcs_touch_target('DMPMetaCmHistoryTask', self.cm_identify_key, self.program_identify_key, utils.master_folder_date_format(utils.jst_now()))


class DMPMetaCmHistoryQueryTask(DMPMetaQueryTask):
    program_history_id = luigi.Parameter(default=None)

    @property
    def resource_path(self):
        return '/cm_histories'

    @property
    def query_params(self):
        params = {
            'program_history_id': self.program_history_id
        }
        return params

    def output(self):
        return LocalTarget(path.dirname(path.abspath(__file__)) + ("/../../tmp/dmp_meta_master/%s/%s/%s.json" % (utils.master_folder_date_format(utils.jst_now()), self.program_history_id, self.resource_path)))


class DMPMetaProgramDnaItemQueryTask(DMPMetaQueryTask):
    broadcaster_identify_key = luigi.Parameter(default=None)
    started_at = utils.DateTimeParameter(default=None)
    ended_at = utils.DateTimeParameter(default=None)

    @property
    def resource_path(self):
        return '/program_dna_items'

    @property
    def query_params(self):
        params = {
            'broadcaster_identify_key': self.broadcaster_identify_key,
            'program_started_at': utils.strftime_by_format(self.started_at, '%Y-%m-%dT%H:%M:%S'),
            'program_ended_at': utils.strftime_by_format(self.ended_at, '%Y-%m-%dT%H:%M:%S')
        }
        return params

    def output(self):
        return LocalTarget(path.dirname(path.abspath(__file__)) + ("/../../tmp/dmp_meta_master/%s/%s/%s.json" % (utils.master_folder_date_format(utils.jst_now()), utils.generate_identify_key(self.broadcaster_identify_key, self.started_at, self.ended_at), self.resource_path)))
