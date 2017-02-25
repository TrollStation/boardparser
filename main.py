#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import requests
from time import time, strftime, gmtime
from misc import stopwatch_countdown, line_print, init_config, init_logger, init_dbclient
from sosach import SosachBoard

__version__ = '0.1'

# TODO: add argument passing support

Config = init_config('config.conf')
init_logger(Config)

db_client = init_dbclient(Config)
db_prefix = Config.get('global', 'database_prefix', fallback='boardparser')


def analyze_word_list_live(board):
    # WLink = DBLink['raw_words']
    words = []
    logging.info('Gathering word list')
    for thread in board.threads:
        for post in thread.posts:
            for word in post.message.split(' '):
                words.append(word)
    logging.info('Read {0} words'.format(len(words)))
    # with open('log/word_list_{0}.txt'.format(strftime('%d.%m.%y_%H:%M', gmtime())), 'w') as word_list_file:
    #    word_list_file.write('\n'.join(words))


def main():
    wait_timeout = Config.getint('global', 'wait_timeout', fallback=600)
    wait_timeout_fallback = Config.getint('global', 'wait_timeout_fallback', fallback=60)
    while Config.getboolean('global', 'loop', fallback=True):
        logging.info('=== START ===')
        start_time = time()
        requests_session = requests.Session()
        board = SosachBoard('b', requests_session, db_client, db_prefix)
        board.update_live_threads()
        board.parse_live_threads()
        board.save_live_threads()
        board.separate_dead_threads()
        board.download_files(6)
        total_time = int(time() - start_time)
        # analyze_word_list_live(board)
        logging.info('Total work time is {0} seconds'.format(total_time))
        logging.info('=== STOP ===')
        if total_time < wait_timeout:
            stopwatch_countdown(wait_timeout - total_time, 'Waiting {0} seconds.'.format(wait_timeout - total_time))
        else:
            logging.warning('Writing board time is longer than wait timeout')
            stopwatch_countdown(wait_timeout_fallback, 'Waiting {0} seconds.'.format(wait_timeout_fallback))
        Config.read('config.conf')


if __name__ == '__main__':
    print(chr(27) + "[2J")  # Clear terminal
    try:
        main()
    except KeyboardInterrupt:
        line_print('=== User exit ===')
        exit()
