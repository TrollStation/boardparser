#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import requests
from time import time  # , gmtime, strftime
from misc import stopwatch_countdown, line_print, init_config, init_logger, init_dbclient
from sosach import Board

__version__ = '0.1'

# TODO: add argument passing support

Config = init_config('config.conf')
init_logger(Config)

DBClient = init_dbclient(Config)
DBLink = DBClient['2ch_parser_b']  # .format(config['database_prefix'])]


def main():
    wait_timeout = Config.getint('global', 'wait_timeout', fallback=600)
    wait_timeout_fallback = Config.getint('global', 'wait_timeout_fallback', fallback=60)
    while Config.getboolean('global', 'loop', fallback=True):
        logging.info('=== START ===')
        start_time = time()
        requests_session = requests.Session()
        board = Board('b', requests_session)
        board.update_live_threads()
        board.parse_live_threads()
        board.save_live_threads(DBLink)
        board.separate_dead_threads(DBLink)
        total_time = int(time() - start_time)
        logging.info('Total work time is {0} seconds'.format(total_time))
        logging.info('=== STOP ===')
        if total_time < wait_timeout:
            stopwatch_countdown(wait_timeout - total_time,
                                'Waiting {0} seconds.'
                                .format(wait_timeout - total_time))
        else:
            logging.warning('Writing board time is longer than wait timeout')
            stopwatch_countdown(wait_timeout_fallback,
                                'Waiting {0} seconds.'.format(wait_timeout_fallback))
        Config.read('config.conf')


if __name__ == '__main__':
    print(chr(27) + "[2J")  # Clear terminal
    try:
        main()
    except KeyboardInterrupt:
        line_print('=== User exit ===')
        exit()
