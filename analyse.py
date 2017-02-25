#!/usr/bin/env python
# -*- coding: utf-8 -*-


import logging
import subprocess
from time import time
from datetime import datetime, timedelta
from misc import stopwatch_countdown, line_print, init_config, init_logger, init_dbclient


__version__ = '0.1'

# TODO: add argument passing support

Config = init_config('config.conf')
Config.set('global', 'log_file_prefix', 'analyser')
init_logger(Config)

db_client = init_dbclient(Config)
db_prefix = Config.get('global', 'database_prefix', fallback='boardparser')


def get_video_duration(filename):
    command = ['ffprobe', '-v', 'quiet', '-of', 'csv=p=0', '-show_entries', 'format=duration', filename]
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return proc.communicate()


def format_duration(duration):
    sec = timedelta(seconds=int(duration))
    d = datetime(1, 1, 1) + sec
    return '%dd %sh %sm %ss' % (d.day, str(d.hour).zfill(2), str(d.minute).zfill(2), str(d.second).zfill(2))


def print_result(start_time, duration_fmt):
    total_time = int(time() - start_time)
    logging.info('Total duration is {0}'.format(duration_fmt))
    logging.info('Total work time is {0} seconds'.format(total_time))
    logging.info('=== STOP ===')
    return total_time


def main():
    wait_timeout = 1200
    while Config.getboolean('global', 'loop', fallback=True):
        logging.info('=== START ===')
        start_time = time()
        a_directory = 'files/b/'
        db_link = db_client[db_prefix+'_b']
        files = db_link['files'].find()
        index = 0
        total_duration = float(0)
        duration_fmt = ''
        for file in files:
            index += 1
            out, err = get_video_duration(a_directory + file['name'])
            if not err:
                try:
                    total_duration += float(out)
                    duration_fmt = format_duration(total_duration)
                    out_string = ' [{0}/{1}] Checking {2} {3}'.format(index, files.count(), file['name'], duration_fmt)
                    line_print(out_string)
                except ValueError:
                    logging.warning('Bad duration {0}'.format(file['name']))
            else:
                print(err)
                logging.error(err)
        total_time = print_result(start_time, duration_fmt)
        stopwatch_countdown(wait_timeout - total_time, 'Waiting {0} seconds.'.format(wait_timeout - total_time))


if __name__ == '__main__':
    # print(chr(27) + "[2J")  # Clear terminal
    try:
        main()
    except KeyboardInterrupt:
        line_print('=== User exit ===')
        exit()
