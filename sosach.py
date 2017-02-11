#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
from time import time, sleep
from misc import line_print


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

    def update_live_threads(self):
        start_fetch_time = time()
        logging.info('Requesting /{0}/ board threads'.format(self.name))
        response = self.requests_session.get(self.threads_api_url, proxies=Proxies)
        if response.status_code != 200:
            logging.error('Requesting #{0} board threads failed. HTTP status code is {1}'.format(self.name,
                                                                                                 response.status_code))
            return False
        try:
            self.threads_json = response.json()['threads']
            logging.info('Fetched {0} threads in {1} seconds'.format(len(self.threads_json),
                                                                     int(time() - start_fetch_time)))
        except ValueError:
            logging.error('Parsing JSON for threads on {0} failed'.format(self.name))

    def parse_live_threads(self):
        for parsed_thread in self.threads_json:
            th = Thread(self.name, self.requests_session, parsed_thread)
            out_string = '[{0}/{1}] Requesting #{2} thread on /{3}/'.format(self.threads_json.index(parsed_thread) + 1,
                                                                            len(self.threads_json),
                                                                            th.number,
                                                                            th.board_name)
            logging.debug(out_string)
            line_print(out_string)
            th.update_posts()
            try:
                for parsed_post in th.posts_json:
                    p = Post(parsed_post)
                    th.posts.append(p)
                self.threads.append(th)
            except TypeError:
                logging.warning('Parsing thread {0} failed'.format(th.number))
            sleep(self.api_request_timeout)

    def save_live_threads(self, db_link):
        start_write_time = time()
        logging.info('Saving {0} threads'.format(len(self.threads)))
        new_threads = 0
        new_posts = 0
        th_link = db_link['threads']
        p_link = db_link['posts']
        for thread in self.threads:
            thread_doc = {'board_name': thread.board_name,
                          'number': thread.number,
                          'subject': thread.subject,
                          'unique_posters': thread.unique_posters,
                          'views': thread.views,
                          'timestamp': thread.timestamp,
                          'processed': 0}
            thread_db_result = th_link.find_one({'number': thread.number})
            if not thread_db_result:
                out_string = '[{0}/{1}] Saving new thread #{2} : '.format(self.threads.index(thread) + 1,
                                                                          len(self.threads),
                                                                          thread.number)
                logging.debug(out_string)
                line_print(out_string)
                th_link.insert_one(thread_doc)
                new_threads += 1
            elif thread_db_result['views'] == thread.views and thread_db_result['unique_posters'] == \
                    thread.unique_posters:
                out_string = '[{0}/{1}] Thread #{2} not changed : '.format(self.threads.index(thread) + 1,
                                                                           len(self.threads),
                                                                           thread.number)
                logging.debug(out_string)
                line_print(out_string)
            else:
                out_string = '[{0}/{1}] Updating thread #{2} : '.format(self.threads.index(thread) + 1,
                                                                        len(self.threads),
                                                                        thread.number)
                logging.debug(out_string)
                line_print(out_string)
                th_link.update_one({'number': thread.number},
                                   {'$set': {'views': thread.views, 'unique_posters': thread.unique_posters}})
            for post in thread.posts:
                post_doc = {'thread': post.thread_number,
                            'number': post.number,
                            'index': post.index,
                            'timestamp': post.timestamp,
                            'op': post.op,
                            'message': post.message}
                if not p_link.find_one({'number': post.number}):
                    post_out_string = '[{0}/{1}] Saving post #{2}'.format(thread.posts.index(post) + 1,
                                                                          len(thread.posts),
                                                                          post.number)
                    logging.debug(out_string + post_out_string)
                    line_print(out_string + post_out_string)
                    p_link.insert_one(post_doc)
                    new_posts += 1
                else:
                    post_out_string = '[{0}/{1}] Passing post #{2}'.format(thread.posts.index(post) + 1,
                                                                           len(thread.posts),
                                                                           post.number)
                    logging.debug(out_string + post_out_string)
                    line_print(out_string + post_out_string)
        line_print('')
        logging.info('Written {0} threads in {1} seconds'.format(len(self.threads), int(time() - start_write_time)))
        logging.info('Saved {0} new threads and {1} new posts'.format(new_threads, new_posts))
        logging.info('Total stored {0} threads and {1} posts'.format(th_link.count(), p_link.count()))

    def separate_dead_threads(self, db_link):
        start_separate_time = time()
        pass


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
