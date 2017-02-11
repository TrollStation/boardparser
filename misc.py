#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from configparser import ConfigParser
from time import time, clock, sleep, strftime, gmtime
from shutil import get_terminal_size
from pymongo import MongoClient


def init_config(file):
    config = ConfigParser()
    config.read(file)
    if 'global' not in config.sections():
        config['global'] = {'loop': 1,
                            'wait_timeout': 1000,
                            'wait_timeout_fallback': 60,
                            'log_file_prefix': 'boardparser',
                            'log_file_level': 'debug',
                            'log_stdout_level': 'info',
                            'mongodb_host': '127.0.0.1',
                            'mongodb_port': 27017,
                            'database_prefix': 'boardparser',
                            'http_proxy': '',
                            'https_proxy': ''}
        with open('config.conf', 'w') as configfile:
            config.write(configfile)
    return config


def init_logger(config):
    log_filename = '{0}_{1}.log'.format(config.get('global', 'log_file_prefix', fallback='boardparser'),
                                        strftime('%d.%m.%y_%H:%M', gmtime()))
    logging.basicConfig(format='%(asctime)s [%(levelname)s] : %(message)s', filename=log_filename)
    logging.addLevelName(logging.WARNING, '\033[1;31m{0}\033[1;0m'.format(logging.getLevelName(logging.WARNING)))
    logging.addLevelName(logging.ERROR, '\033[1;41m{0}\033[1;0m'.format(logging.getLevelName(logging.ERROR)))
    logger = logging.getLogger()
    log_file_level = config.get('global', 'log_file_level', fallback='debug')
    if log_file_level == 'debug':
        logger.setLevel(logging.DEBUG)
    elif log_file_level == 'info':
        logger.setLevel(logging.INFO)
    elif log_file_level == 'warning':
        logger.setLevel(logging.INFO)
    elif log_file_level == 'error':
        logger.setLevel(logging.INFO)
    else:
        logging.error('Config reading failed : log_file_level option is invalid')
    log_console_handler = logging.StreamHandler()
    log_stdout_level = config.get('global', 'log_stdout_level', fallback='info')
    if log_stdout_level == 'debug':
        log_console_handler.setLevel(logging.DEBUG)
    elif log_stdout_level == 'info':
        log_console_handler.setLevel(logging.INFO)
    elif log_stdout_level == 'warning':
        log_console_handler.setLevel(logging.INFO)
    elif log_stdout_level == 'error':
        log_console_handler.setLevel(logging.INFO)
    else:
        logging.error('Config reading failed : log_stdout_level option is invalid')
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] : %(message)s')
    log_console_handler.setFormatter(log_formatter)
    logger.addHandler(log_console_handler)


def init_dbclient(config):
    return MongoClient(config.get('global', 'mongodb_host', fallback='127.0.0.1'),
                       config.getint('global', 'mongodb_port', fallback=27017))


# le костыль
def line_print(s):
    print(' ' * get_terminal_size()[0], end='')
    print('\r' * get_terminal_size()[0], end='')
    print(s, end='')
    print('\r' * len(s), end='')


def stopwatch_countdown(seconds, comment=''):
    # TODO: format with mm:ss
    start = time()
    clock()
    estimated = 1
    while estimated:
        estimated = seconds - int(time() - start)
        s = '[{0}] {1} '.format(estimated, comment)
        line_print(s)
        sleep(1)
