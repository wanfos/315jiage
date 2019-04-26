#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 爬虫管理器

from crawler import Crawler
from db_connector import DBConnector
import threadpool
import threading
import os
import sys

class CrawlerManger:
    thread_lock = threading.Lock()

    def __init__(
            self,
            list_base_url='',
            content_base_url='',
            mysql_config={},
            table_name='',
            table_create_sql=None,
            data_rule={}):
        self.list_base_url = list_base_url
        self.content_base_url = content_base_url
        self.mysql_config = mysql_config
        self.table_name = table_name
        self.table_create_sql = table_create_sql
        self.data_rule = data_rule

    """
    根据页面区间抓数据
    start_page: 起始页数
    page_num: 页数
    chunk: 块数
    concurrency: 并发数
    """

    def fetch_by_page_range(self, start=1, num=1, chunk=1, concurrency=1):
        self.__create_table()

        if concurrency <= 1:
            self.__fetch_by_page_range(start, start + num)
            print("抓数据结束！！！")
            return

        chunk = 1 if chunk < 1 else chunk
        start = 1 if start < 1 else start
        num = 1 if num < 1 else num

        # 将总数平均分
        params = self.__split_num(start, num, chunk)
        print(params)
        func_var = []
        for param in params:
            func_var.append((None, param))

        # 使用线程池进行并发请求
        pool = threadpool.ThreadPool(concurrency)
        requests = threadpool.makeRequests(self.__fetch_by_page_range, func_var)
        [pool.putRequest(req) for req in requests]
        pool.wait()
        print("抓数据结束！！！")

    """
    抓指定页数据
    page_list: 页数列表， 如：想请求1、3、5页的数据，参数为[1, 3, 5]
    """

    def fetch_by_page_list(self, page_list):
        self.__create_table()
        self.__fetch_by_page_list(page_list)
        print("抓数据结束！！！")

    """
    抓指定内容id数据
    href_list: 内容id列表， 如：想请求1、3、5页的数据，参数为[1, 3, 5]
    """

    def fetch_by_href_list(self, href_list):
        self.__create_table()
        self.__fetch_by_href_list(href_list)
        print("抓数据结束！！！")

    """
    把总数平均分成chunk块
    start: 起始数
    num: 数量
    chunk: 块数
    """

    @staticmethod
    def __split_num(start, num, chunk):
        range_list = []
        for i in range(0, num, chunk):
            s = i + 1
            e = i + chunk
            if e > num:
                e = num
            range_list.append({'start': s + start - 1, 'end': e + start - 1})

        return range_list

    def __create_table(self):
        if self.table_create_sql is None:
            print("表创建语句不存在！")
            sys.exit(0)
        
        if self.table_create_sql is not None:
            conn = DBConnector(self.mysql_config)
            conn.execute_sql(self.table_create_sql)

    def __fetch_by_page_range(self, start, end):
        the_crawler = self.__create_crawler()
        the_crawler.fetch_by_page_range(start, end)

        self.__write_failed_file(the_crawler)

    def __fetch_by_page_list(self, page_list):
        the_crawler = self.__create_crawler()
        the_crawler.fetch_by_page_list(page_list)

        self.__write_failed_file(the_crawler)

    def __fetch_by_href_list(self, href_list):
        the_crawler = self.__create_crawler()
        the_crawler.fetch_by_href_list(href_list)

        self.__write_failed_file(the_crawler)

    def __create_crawler(self):
        crawler = Crawler(self.list_base_url, self.content_base_url, self.mysql_config, self.table_name, self.data_rule)
        crawler.ignored_href_list = self.__get_noexist_href_list()
        crawler.replace_to_empty_list = []
        return crawler

    def __get_noexist_href_list(self):
        file_path = os.path.join('error', "{}-noexist-href.txt".format(self.table_name))
        if os.path.exists(file_path) == False:
            return []

        try:
            file = open(file_path, "r")
            href_str = file.read()
            href_list = href_str.split('\n')
            for i in range(len(href_list)-1,-1,-1):
                if href_list[i] == '':
                    href_list.pop(i)
            return href_list
        finally:
            file.close()
        return []

    def __write_failed_file(self, crawler):
        self.thread_lock.acquire()

        if len(crawler.get_failed_page_list()) > 0:
            file = open(os.path.join('error', "{}-failed-page.txt".format(self.table_name)), "a+")
            file.write('\n'.join([str(x) for x in crawler.get_failed_page_list()]) + '\n')
            file.close()
 
        if len(crawler.get_failed_href_list()) > 0:
            file = open(os.path.join('error', "{}-failed-href.txt".format(self.table_name)), "a+")
            file.write('\n'.join(crawler.get_failed_href_list()) + '\n')
            file.close()

        self.thread_lock.release()

    """
    拉取失败的数据
    """

    def fetch_failed_data(self):
        href_list = []
        try:
            file = open(os.path.join('error', "{}-failed-href.txt".format(self.table_name)), "r")
            href_str = file.read()
            href_list = href_str.split('\n')
            for i in range(len(href_list)-1,-1,-1):
                if href_list[i] == '':
                    href_list.pop(i)
        finally:
            file.close()
        
        if len(href_list) > 0:
            print('正在获取失败数据，href:{}'.format(href_list))
            self.fetch_by_href_list(href_list)
