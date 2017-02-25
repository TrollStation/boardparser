#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import os
from time import time, sleep
from datetime import datetime
from misc import line_print, MLStripper

Proxies = {}

# File types
FILE_JPEG = 1
FILE_PNG = 2
FILE_WEBM = 6
FILE_STICKER = 100


class Board:
    api_request_timeout = 5
    api_wait_timeout = 0

    def __init__(self, name):
        self.name = name


class SosachBoard(Board):
    def __init__(self, name, requests_session, db_client, db_prefix):
        super(Board, self).__init__()
        self.name = name
        self.requests_session = requests_session
        self.db_link = db_client[db_prefix + '_' + name]
        self.proxy = {}
        self.threads_api_url = 'https://2ch.hk/{0}/threads.json'.format(name)
        self.threads_json = None
        self.threads = []

    def update_live_threads(self):
        logging.info('Requesting /{0}/ board threads'.format(self.name))
        response = self.requests_session.get(self.threads_api_url, proxies=Proxies, timeout=self.api_request_timeout)
        if response.status_code != 200:
            logging.error('Requesting /{0}/ threads failed. HTTP status code is {1}'.format(self.name,
                                                                                            response.status_code))
            return False
        try:
            self.threads_json = response.json()['threads']
            logging.info('Fetched {0} threads'.format(len(self.threads_json)))
        except ValueError:
            logging.error('Parsing JSON for threads on {0} failed'.format(self.name))
            return False

    def parse_live_threads(self):
        for parsed_thread in self.threads_json:
            th = Thread(self.name, self.requests_session, parsed_thread)
            out_string = ' [{0}/{1}] Requesting #{2} thread on /{3}/'.format(self.threads_json.index(parsed_thread) + 1,
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
                logging.warning('Parsing thread #{0} failed'.format(th.number))
            sleep(self.api_wait_timeout)

    def save_live_threads(self):
        logging.info('Saving {0} threads'.format(len(self.threads)))
        start_time = time()
        new_threads = 0
        new_posts = 0
        th_link = self.db_link['threads']
        p_link = self.db_link['posts']
        th_link.create_index('number')
        p_link.create_index('number')

        for thread in self.threads:
            db_result = th_link.find_one({'number': thread.number})
            if not db_result:
                out_string = ' [{0}/{1}] Saving new thread #{2} : '.format(self.threads.index(thread) + 1,
                                                                           len(self.threads),
                                                                           thread.number)
                logging.debug(out_string)
                line_print(out_string)
                th_link.insert_one(thread.get_db_doc())
                new_threads += 1
            elif db_result['views'] == thread.views and db_result['unique_posters'] == thread.unique_posters:
                out_string = ' [{0}/{1}] Thread #{2} not changed : '.format(self.threads.index(thread) + 1,
                                                                            len(self.threads),
                                                                            thread.number)
                logging.debug(out_string)
                line_print(out_string)
            else:
                out_string = ' [{0}/{1}] Updating thread #{2} : '.format(self.threads.index(thread) + 1,
                                                                         len(self.threads),
                                                                         thread.number)
                logging.debug(out_string)
                line_print(out_string)
                th_link.update_one({'number': thread.number},
                                   {'$set': {'views': thread.views, 'unique_posters': thread.unique_posters}})
            for post in thread.posts:
                if not p_link.find_one({'number': post.number}):
                    post_out_string = ' [{0}/{1}] Saving post #{2}'.format(thread.posts.index(post) + 1,
                                                                           len(thread.posts),
                                                                           post.number)
                    logging.debug(out_string + post_out_string)
                    line_print(out_string + post_out_string)
                    p_link.insert_one(post.get_db_doc())
                    new_posts += 1
                else:
                    post_out_string = ' [{0}/{1}] Passing post #{2}'.format(thread.posts.index(post) + 1,
                                                                            len(thread.posts),
                                                                            post.number)
                    logging.debug(out_string + post_out_string)
                    line_print(out_string + post_out_string)
        line_print('')
        logging.info('Written {0} threads in {1} seconds'.format(len(self.threads), int(time() - start_time)))
        logging.info('Saved {0} new threads and {1} new posts'.format(new_threads, new_posts))
        logging.info('Total saved {0} threads and {1} posts'.format(th_link.count(), p_link.count()))
        stats = {'new_posts': ''}

    def separate_dead_threads(self):
        start_time = time()
        threads_count = 0
        posts_count = 0
        logging.info('Separating dead threads in /{0}/'.format(self.name))
        th_link = self.db_link['threads']
        p_link = self.db_link['posts']
        th_d_link = self.db_link['dead_threads']
        p_d_link = self.db_link['dead_posts']
        db_threads = th_link.find()
        live_threads_numbers = []
        for thread in self.threads:
            live_threads_numbers.append(thread.number)
        for db_thread in db_threads:
            if db_thread['number'] not in live_threads_numbers:
                del (db_thread['_id'])
                th_d_link.insert_one(db_thread)
                th_link.delete_one({'number': db_thread['number']})
                threads_count += 1
                db_posts = p_link.find({'thread': db_thread['number']})
                for db_post in db_posts:
                    del (db_post['_id'])
                    p_d_link.insert_one(db_post)
                    p_link.delete_one({'number': db_post['number']})
                    posts_count += 1
                    logging.debug('Post #{0} is dead and separated'.format(db_post['number']))
                logging.debug('Thread #{0} is dead and separated'.format(db_thread['number']))
        logging.info('Separated {0} dead threads with {1} posts in {2} seconds'
                     .format(threads_count, posts_count, int(time() - start_time)))
        logging.info('Total stored {0} dead threads and {1} dead posts'.format(th_d_link.count(), p_d_link.count()))

    def download_files(self, file_type):
        start_time = time()
        files_count = 0
        download_list = []
        files_db_link = self.db_link['files']

        logging.info('Downloading files')
        for thread in self.threads:
            for post in thread.posts:
                for file in post.files:
                    if file['type'] == file_type:
                        download_list.append({
                            'url': 'https://2ch.hk{0}'.format(file['path']),
                            'name': file['name'],
                            'md5': file['md5'],
                        })

        directory_name = 'files/{0}'.format(self.name)
        if not os.path.exists(directory_name):
            os.makedirs(directory_name)

        for file in download_list:
            db_result = files_db_link.find_one({'md5': file['md5']})
            if not db_result:
                line_print(' [{0}/{1}] Downloading file'.format(download_list.index(file), len(download_list)))
                os.system('wget -q -O {0}/{1} {2}'.format(directory_name, file['name'], file['url']))
                db_file_doc = {'name': file['name'],
                               'md5': file['md5'],
                               'date': int(datetime.utcnow().timestamp())}
                files_db_link.insert_one(db_file_doc)
                files_count += 1
            else:
                line_print(' [{0}/{1}] Skipping file'.format(download_list.index(file), len(download_list)))

        logging.info('Downloaded {0}/{1} files in {2} seconds'.format(
            files_count, len(download_list), int(time() - start_time)))


        def save_stats_ntp(new_threads, new_posts):
            self.db_link['stats']


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

    def get_db_doc(self):
        return {'board_name': self.board_name,
                'number': self.number,
                'subject': self.subject,
                'unique_posters': self.unique_posters,
                'views': self.views,
                'timestamp': self.timestamp,
                'processed': 0}


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
        self.repair_message()
        self.op = post['op']
        self.files = post['files']

    def repair_message(self):
        s = MLStripper()
        s.feed(self.message)
        self.message = s.get_data()

    def get_db_doc(self):
        return {'thread': self.thread_number,
                'number': self.number,
                'index': self.index,
                'timestamp': self.timestamp,
                'op': self.op,
                'message': self.message,
                'files': self.files}
