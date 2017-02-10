#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging


Proxies = {}


class Board:
    api_request_timeout = 0

    def __init__(self, name, requests_session):
        self.name = name
        self.requests_session = requests_session
        self.proxy = {}
        self.threads_api_url = 'https://2ch.hk/{0}/threads.json'.format(name)
        self.threads_json = None
        self.threads = []

    def update_threads(self):
        logging.info('Requesting /{0}/ board threads...'.format(self.name))
        response = self.requests_session.get(self.threads_api_url, proxies=Proxies)
        if response.status_code != 200:
            logging.error('Requesting #{0} board threads failed. HTTP status code is {1}'.format(self.name,
                                                                                                 response.status_code))
            return
        try:
            self.threads_json = response.json()['threads']
        except ValueError:
            logging.error('Parsing JSON for threads on {0} failed'.format(self.name))


class Thread:
    def __init__(self, board_name, requests_session, thread):
        self.board_name = board_name
        self.requests_session = requests_session
        self.number = int(thread['num'])
        self.subject = thread['subject']
        self.timestamp = int(thread['timestamp'])
        self.unique_posters = 0
        self.views = int(thread['views'])
        self.posts_api_url = 'https://2ch.hk/{0}/res/{1}.json'.format(self.board_name, self.number)
        self.posts_json = None
        self.posts = []

    def update_posts(self):
        response = self.requests_session.get(self.posts_api_url, proxies=Proxies)
        if response.status_code == 200:
            try:
                parsed_response = response.json()
            except ValueError:
                logging.error('Parsing JSON for thread #{0} failed'.format(self.number))
                with open('parsing_thread_{0}_failed.json'.format(self.number), 'w') as failed_json_dump:
                    failed_json_dump.write(response.text)
                    failed_json_dump.close()
                return
            self.posts_json = parsed_response['threads'][0]['posts']
            self.unique_posters = parsed_response['unique_posters']
        else:
            logging.error('Requesting #{0} thread failed. HTTP code is {1}'.format(self.number,
                                                                                   response.status_code))


class Post:
    def __init__(self, post):
        self.number = post['num']
        self.index = post['number']
        self.thread_number = post['parent']
        self.timestamp = post['timestamp']
        self.message = post['comment']
        self.op = post['op']
        self.files = []
        for parsed_attachment in post['files']:
            f = Attachment(parsed_attachment)
            self.files.append(f)


class Attachment:
    def __init__(self, attachment):
        self.name = attachment['name']
        self.path = attachment['path']
