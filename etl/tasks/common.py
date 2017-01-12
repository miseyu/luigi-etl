# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import logging
import luigi
import json
import time
import datetime
import functools
from types import MethodType

from etl.settings import get_config
from etl import utils

logger = logging.getLogger('luigi-interface')


class ETLMarkerTask(luigi.Task):

    def success(self):
        marker = self.output()
        marker.touch()

    @property
    def unique_key(self):
        raise NotImplementedError("unique_key should override")

    def output(self):
        return utils.generate_gcs_touch_target(self.__class__.__name__, self.unique_key)
