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
        th_link.create_index('number')
        p_link.create_index('number')
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
        separated_threads_count = 0
        separated_posts_count = 0
        logging.info('Separating dead threads in /{0}/'.format(self.name))
        th_link = db_link['threads']
        p_link = db_link['posts']
        th_d_link = db_link['dead_threads']
        p_d_link = db_link['dead_posts']
        db_threads = th_link.find()
        live_threads_numbers = []
        for thread in self.threads:
            live_threads_numbers.append(thread.number)
        for db_thread in db_threads:
            if db_thread['number'] not in live_threads_numbers:
                dead_thread_doc = {'board_name': db_thread['board_name'],
                                   'number': db_thread['number'],
                                   'subject': db_thread['subject'],
                                   'unique_posters': db_thread['unique_posters'],
                                   'views': db_thread['views'],
                                   'timestamp': db_thread['timestamp'],
                                   'processed': db_thread['processed']}
                th_d_link.insert_one(dead_thread_doc)
                th_link.delete_one({'number': db_thread['number']})
                separated_threads_count += 1
                db_posts = p_link.find({'thread': db_thread['number']})
                for db_post in db_posts:
                    post_doc = {'thread': db_post['thread'],
                                'number': db_post['number'],
                                'index': db_post['index'],
                                'timestamp': db_post['timestamp'],
                                'op': db_post['op'],
                                'message': db_post['message']}
                    p_d_link.insert_one(post_doc)
                    p_link.delete_one({'number': db_post['number']})
                    separated_posts_count += 1
                    logging.debug('Post #{0} is dead and separated'.format(db_post['number']))
                logging.debug('Thread #{0} is dead and separated'.format(db_thread['number']))
        logging.info('Separated {0} dead threads with {1} posts in {2} seconds'
                     .format(separated_threads_count,
                             separated_posts_count,
                             int(time() - start_separate_time)))
        logging.info('Total stored {0} dead threads and {1} dead posts'.format(th_d_link.count(), p_d_link.count()))


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
            self.unique_posters = int(parsed_response['unique_posters'])
        else:
            logging.error('Requesting #{0} thread failed. HTTP code is {1}'.format(self.number,
                                                                                   response.status_code))


class Post:
    def __init__(self, post):
        self.number = int(post['num'])
        self.index = int(post['number'])
        if int(post['parent']):
            self.thread_number = int(post['parent'])
        else:
            self.thread_number = int(post['num'])
        self.timestamp = int(post['timestamp'])
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
