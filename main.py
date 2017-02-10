#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import requests
from configparser import ConfigParser
from sosach import Board, Thread, Post
from time import sleep, time, gmtime, strftime
from pymongo import MongoClient
from misc import stopwatch_countdown, line_print

__version__ = '0.1'


def get_board(name):
    requests_session = requests.Session()
    board = Board(name, requests_session)
    board.update_threads()

    logging.info('Parsing {0} threads'.format(len(board.threads_json)))
    for parsed_thread in board.threads_json:
        th = Thread(board.name, requests_session, parsed_thread)
        out_string = '[{0}/{1}] Requesting #{2} thread on /{3}/... ' \
            .format(board.threads_json.index(parsed_thread) + 1,
                    len(board.threads_json),
                    th.number,
                    th.board_name)
        logging.debug(out_string)
        line_print(out_string)
        th.update_posts()
        try:
            for parsed_post in th.posts_json:
                p = Post(parsed_post)
                th.posts.append(p)
            board.threads.append(th)
        except TypeError:
            logging.warning('Parsing thread {0} failed'.format(th.number))
        sleep(board.api_request_timeout)
    return board


def save_board(board):
    logger.info('Saving {0} threads'.format(len(board.threads)))
    new_threads = 0
    new_posts = 0
    for thread in board.threads:
        thread_doc = {'board_name': thread.board_name,
                      'number': thread.number,
                      'subject': thread.subject,
                      'unique_posters': thread.unique_posters,
                      'views': thread.views,
                      'timestamp': thread.timestamp,
                      'processed': 0}
        thread_db_result = ThLink.find_one({'number': thread.number})
        if not thread_db_result:
            out_string = '[{0}/{1}] Saving new thread #{2} : '.format(board.threads.index(thread) + 1,
                                                                      len(board.threads),
                                                                      thread.number)
            logging.debug(out_string)
            line_print(out_string)
            ThLink.insert_one(thread_doc)
            new_threads += 1
        elif thread_db_result['views'] == thread.views and thread_db_result['unique_posters'] == thread.unique_posters:
            out_string = '[{0}/{1}] Thread #{2} not changed : '.format(board.threads.index(thread) + 1,
                                                                       len(board.threads),
                                                                       thread.number)
            logging.debug(out_string)
            line_print(out_string)
        else:
            out_string = '[{0}/{1}] Updating thread #{2} : '.format(board.threads.index(thread) + 1,
                                                                    len(board.threads),
                                                                    thread.number)
            logging.debug(out_string)
            line_print(out_string)
            ThLink.update_one({'number': thread.number}, {'$set': {'views': thread.views,
                                                                   'unique_posters': thread.unique_posters}})
        for post in thread.posts:
            post_doc = {'thread': post.thread_number,
                        'number': post.number,
                        'index': post.index,
                        'timestamp': post.timestamp,
                        'op': post.op,
                        'message': post.message}
            if not PLink.find_one({'number': post.number}):
                post_out_string = '[{0}/{1}] Saving post #{2}'.format(thread.posts.index(post) + 1,
                                                                      len(thread.posts),
                                                                      post.number)
                logging.debug(out_string + post_out_string)
                line_print(out_string + post_out_string)
                PLink.insert_one(post_doc)
                new_posts += 1
            else:
                post_out_string = '[{0}/{1}] Passing post #{2}'.format(thread.posts.index(post) + 1,
                                                                       len(thread.posts),
                                                                       post.number)
                logging.debug(out_string + post_out_string)
                line_print(out_string + post_out_string)
    line_print('')
    logging.info('Saved {0} threads and {1} posts'.format(new_threads, new_posts))
    logging.info('Stored {0} threads and {1} posts'.format(ThLink.count(), PLink.count()))


if __name__ == '__main__':
    config = ConfigParser()
    config.read('config.conf')
    if 'global' not in config.sections():
        config['global'] = {'wait_timeout': 1000,
                            'wait_timeout_fallback': 60,
                            'log_file': 'boardparser.log',
                            'log_file_level': 'debug',
                            'log_stdout_level': 'info',
                            'mongodb_host': '127.0.0.1',
                            'mongodb_port': 27017,
                            'database_prefix': 'boardparser',
                            'http_proxy': '',
                            'https_proxy': ''}
        with open('config.conf', 'w') as configfile:
            config.write(configfile)
    wait_timeout = int(config['global']['wait_timeout'])
    wait_timeout_fallback = int(config['global']['wait_timeout_fallback'])
    # TODO: move logging objects to misc
    # log_filename = 'parser_{0}.log'.format(strftime('%d.%m.%y_%H:%M', gmtime()))
    logging.basicConfig(format='%(asctime)s [%(levelname)s] : %(message)s', filename=config['global']['log_file'])
    logging.addLevelName(logging.WARNING, '\033[1;31m{0}\033[1;0m'.format(logging.getLevelName(logging.WARNING)))
    logging.addLevelName(logging.ERROR, '\033[1;41m{0}\033[1;0m'.format(logging.getLevelName(logging.ERROR)))
    logger = logging.getLogger()
    if config['global']['log_file_level'] == 'debug':
        logger.setLevel(logging.DEBUG)
    elif config['global']['log_file_level'] == 'info':
        logger.setLevel(logging.INFO)
    elif config['global']['log_file_level'] == 'warning':
        logger.setLevel(logging.INFO)
    elif config['global']['log_file_level'] == 'error':
        logger.setLevel(logging.INFO)
    else:
        logging.error('Config reading failed : log_file_level option is invalid')
    log_console_handler = logging.StreamHandler()
    if config['global']['log_stdout_level'] == 'debug':
        log_console_handler.setLevel(logging.DEBUG)
    elif config['global']['log_stdout_level'] == 'info':
        log_console_handler.setLevel(logging.INFO)
    elif config['global']['log_stdout_level'] == 'warning':
        log_console_handler.setLevel(logging.INFO)
    elif config['global']['log_stdout_level'] == 'error':
        log_console_handler.setLevel(logging.INFO)
    else:
        logging.error('Config reading failed : log_stdout_level option is invalid')
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] : %(message)s')
    log_console_handler.setFormatter(log_formatter)
    logger.addHandler(log_console_handler)
    DBClient = MongoClient(config['global']['mongodb_host'], int(config['global']['mongodb_port']))
    DBLink = DBClient['2ch_parser_b']  # .format(config['database_prefix'])]
    ThLink = DBLink['threads']
    PLink = DBLink['posts']
    try:
        while True:
            logging.info('=== START ===')
            start_time = time()
            start_fetch_time = time()
            b_board = get_board('b')
            logging.info(
                'Fetched {0} threads in {1} seconds'.format(len(b_board.threads), int(time() - start_fetch_time)))
            start_write_time = time()
            save_board(b_board)
            logging.info(
                'Written {0} threads in {1} seconds'.format(len(b_board.threads), int(time() - start_write_time)))
            total_time = int(time() - start_time)
            logging.info('Total work time is {0} seconds'.format(total_time))
            logging.info('=== STOP ===')
            if total_time < wait_timeout:
                stopwatch_countdown(wait_timeout - total_time,
                                    'Waiting update timeout {0} seconds.'
                                    .format(wait_timeout - total_time))
            else:
                logging.warning('Writing board time is longer than wait timeout')
                stopwatch_countdown(wait_timeout_fallback,
                                    'Waiting update timeout {0} seconds.'.format(wait_timeout_fallback))
    except KeyboardInterrupt:
        line_print('=== User exit ===')
        exit()
