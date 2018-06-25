"""
Octopus logger to track:
- all intra-operations
- all external operations
etc.
"""

import datetime
import logging
import sys
import os

now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M')
place = os.path.dirname(os.path.abspath(__file__))


def test_logger():
    log = logging.getLogger(__name__)
    log.setLevel(logging.DEBUG)

    cons_handler = logging.StreamHandler(stream=sys.stdout)
    cons_handler.setLevel(logging.DEBUG)
    cons_format = logging.Formatter('%(relativeCreated)2d %(threadName)-12s'
                                    '%(asctime)-24s'
                                    '%(levelname)-8s'
                                    '%(module)-20s'
                                    '%(funcName)-22s'
                                    'L:%(lineno)-6s'
                                    '%(message)8s')
    cons_handler.setFormatter(cons_format)
    log.addHandler(cons_handler)

    log_name = "/var/log/octopus/DJANGO_CELERY_RESULTS.log"

    # Extra detailed logging to file:
    f_handler = logging.FileHandler(log_name, mode='a', encoding='utf-8',)

    f_handler.setLevel(logging.DEBUG)
    # Extra detailed logging to console:
    f_format = logging.Formatter('%(asctime)-24s'
                                 '%(levelname)-8s'
                                 '%(filename)-23s'
                                 '%(funcName)-26s'
                                 'L:%(lineno)-6s'
                                 '%(message)8s')

    f_handler.setFormatter(f_format)
    log.addHandler(f_handler)

    return log


test_logger()

