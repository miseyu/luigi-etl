# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

from luigi_td.bulk_import import BulkImport
import tdclient

from etl.settings import get_config


class BulkImportTask(BulkImport):
    default_timezone = 'Asia/Tokyo'
    database = get_config().get_td_database_name()

    def complete(self):
        client = self.config.get_client()
        try:
            session = client.bulk_import(self.session)
        except tdclient.api.NotFoundError:
            return False
        else:
            if self.no_commit:
                return session.status == 'ready'
            else:
                return session.status in ('committing', 'committed')
