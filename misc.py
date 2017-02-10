#!/usr/bin/env python
# -*- coding: utf-8 -*-

from time import time, clock, sleep
from shutil import get_terminal_size


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
