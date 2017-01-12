# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals, print_function

import sys

import luigi
import time
import logging

from etl.settings import get_config

logger = logging.getLogger('luigi-interface')

stream_handler = logging.StreamHandler()
stream_handler.level = logging.INFO
stream_handler.setFormatter(logging.Formatter(
    "%(asctime)s %(levelname)8s %(message)s"))
logger.addHandler(stream_handler)

def main(argv=sys.argv[1:]):
    return luigi.run(argv)

if __name__ == '__main__':
    slack = get_config().get_notify_slack()
    argv = sys.argv[1:]
    task_name = argv[0]
    start = time.time()
    result = main(argv)
    elapsed_time = time.time() - start

    if result:
        attachment = ([{
                    "color": "#36a64f",
                    "text": "%s SUCCESS elapsed_time %s sec" % (task_name, str(elapsed_time)),
                    "mrkdwn_in": ["text"]}
        ])
        try:
            slack.notify(username="Luigi ETL", attachments=attachment)
        except:
            pass
        sys.exit(None)
    else:
        attachment = ([{
                    "color": "danger",
                    "text": "%s ERROR elapsed_time %s sec" % (task_name, str(elapsed_time)),
                    "mrkdwn_in": ["text"]}
        ])
        try:
            slack.notify(username="Luigi ETL", attachments=attachment)
        except:
            pass
        sys.exit("Error")
