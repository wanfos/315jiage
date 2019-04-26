#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 爬虫

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from db_connector import DBConnector
import time
import mysql.connector
import sys
# reload(sys)
# sys.setdefaultencoding('utf-8') 

class Crawler:
    # 失败重试次数
    max_retry_times = 15
    # 失败重试延迟时间
    retry_delay_secs = 1

    def __init__(
            self,
            list_base_url='',
            content_base_url='',
            mysql_config={},
            table_name='',
            data_rule={}):
        self.list_base_url = list_base_url
        self.content_base_url = content_base_url
        self.mysql_config = mysql_config
        self.table_name = table_name
        self.data_rule = data_rule
        self.driver = None
        self.conn = DBConnector(mysql_config)
        self.label_table_field_dict = self.get_label_table_field_dict()
        # 记录请求失败的页数
        self.failed_page_list = []
        # 记录请求失败的内容id
        self.failed_href_list = []
        # 不需要拉取的href
        self.ignored_href_list = []
        # 需要替换为空字符串的字符串列表
        self.replace_to_empty_list = []

    def __del__(self):
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    # 打开浏览器
    def __open_browser(self):
        if self.driver is None:
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
            # self.driver = webdriver.Chrome(chrome_options=chrome_options)
            self.driver = webdriver.Chrome()
            self.driver.set_page_load_timeout(15)
            # self.driver = webdriver.Firefox()

    # 关闭浏览器
    def __close_browser(self):
        if self.driver is not None:
            self.driver.close()
            self.driver = None

    def get_failed_page_list(self):
        return self.failed_page_list

    def get_failed_href_list(self):
        return self.failed_href_list

    """
    抓数据(根据页面区间)
    start: 起始页数
    end: 结束页数
    """

    def fetch_by_page_range(self, start, end):
        while start <= end:
            self.__fetch_list(start)
            start += 1

        # 重新获取失败的数据
        self.__retry_failed()

    """
    抓指定页数据
    page_list: 页数列表， 如：想请求1、3、5页的数据，参数为[1, 3, 5]
    """

    def fetch_by_page_list(self, page_list):
        for page in page_list:
            self.__fetch_list(page)

        # 重新获取失败的数据
        self.__retry_failed()

    """
    抓指定内容数据
    href_list: 内容id列表， 如：想请求1、3、5页的数据，参数为[1, 3, 5]
    """

    def fetch_by_href_list(self, href_list):
        for href in href_list:
            self.__fetch_detail(href)

        # 重新获取失败的数据
        self.__retry_failed()

    """
    重新获取失败的数据
    """

    def __retry_failed(self):
        # 失败重试次数
        retry_times = 0
        # 获取失败时进行重试
        while len(self.failed_page_list) > 0 and retry_times < Crawler.max_retry_times:
            page_list = self.failed_page_list
            self.failed_page_list = []

            for page in page_list:
                self.__fetch_list(page)

            retry_times += 1
            time.sleep(Crawler.retry_delay_secs*retry_times)

        # 失败重试次数
        retry_times = 0
        # 获取失败时进行重试
        while len(self.failed_href_list) > 0 and retry_times < Crawler.max_retry_times:
            href_list = self.failed_href_list
            self.failed_href_list = []

            for href in href_list:
                self.__fetch_detail(href)

            retry_times += 1
            time.sleep(Crawler.retry_delay_secs*retry_times)

        if len(self.failed_page_list) > 0:
            print('-----------> 获取失败的列表: {}'.format(self.failed_page_list))
            
        if len(self.failed_href_list) > 0:
            print('-----------> 获取失败的内容: {}'.format(self.failed_href_list))

    def __get(self, url):
        # 失败重试次数
        retry_times = 0
        # 获取失败时进行重试
        while True:
            if self.driver is None:
                self.__open_browser()

            try:
                resp = self.driver.execute('get', {'url': url})
                if resp is not None and resp['status'] == 0:
                    return True

                if retry_times >= Crawler.max_retry_times:
                    break

                retry_times += 1
                time.sleep(Crawler.retry_delay_secs*retry_times)
            except TimeoutException:
                print("获取网页超时,{}".format(url))
                self.__close_browser()
                time.sleep(3)

        return False

    """
    抓列表页
    start_page: 起始页数
    end_page: 结束页数
    """

    def __fetch_list(self, page):
        if page == 1:
            url = self.list_base_url.format('')
        else:
            url = self.list_base_url.format('p{}'.format(page))

        result = self.__get(url)
        node_list = self.driver.find_elements_by_css_selector(".sCard>div>a")
        if result and len(node_list) > 1:
            print("正在获取第{}页数据...".format(page))
        else:
            print("========> 获取第{}页列表失败！！！".format(page))
            self.failed_page_list.append(page)
            return

        data_list = []
        for node in node_list:
            href = node.get_attribute("href")
            data_list.append({'href':href})

        failed_href_list_in_page = []
        for data in data_list:
            if self.__fetch_detail(data['href']) is False:
                failed_href_list_in_page.append(data['href'])

        print('-----------------> 第{}页数据获取完成.'.format(page))
        if len(failed_href_list_in_page) > 0:
            print('****************> 第{}页获取失败内容: {}.'.format(page, failed_href_list_in_page))

    """
    抓内容页
    href: 内容网址
    """

    def __fetch_detail(self, href=None):
        if href in self.ignored_href_list:
            print('忽略的内容的：{}'.format(href))
            return True

        data = {
            "id": href.replace(self.content_base_url, ''), 
            "source_url":href
            }

        result = self.__get(href)
        try:
            node_list = self.driver.find_elements_by_css_selector('#content p')
        except TimeoutException:
            print("获取内容超时，href:{}".format(href))
            return False

        if result is False or len(node_list) < 1:
            self.failed_href_list.append(href)
            print('获取内容失败!href:{}'.format(href))
            return False

        self.__extract_instructions(data)
        split_str = '：'
        for node in node_list:
            text = node.text.strip().encode('utf-8').replace('　', ' ')
            first_index = text.find(split_str)
            last_index = text.rfind(split_str)
            if first_index < 0:
                continue

            if first_index == last_index:
                self.__extract_content_to_data(text, data)

            for item in text.split(' '):
                self.__extract_content_to_data(item, data)

        try:
            self.__save_data_to_db(data)
        except mysql.connector.Error as e:
            self.failed_href_list.append(href)
            print('保存内容失败!href:{}, e:{}'.format(href, e))
            return False
        
        return True

    # 提取说明书
    def __extract_instructions(self, data):
        try:
            node_list = self.driver.find_elements_by_css_selector('#tab1>ul>li')
            text_list = []
            for node in node_list:
                text_list.append(self.driver.execute_script("return arguments[0].textContent;", node).encode('utf-8'))
            data[self.label_table_field_dict['说明书']] = '\n'.join(text_list)
        except TimeoutException:
            return False
        except NoSuchElementException:
            return False

        return True

    # 提取内容
    def __extract_content_to_data(self, text, data):
        split_str = '：'
        index = text.find(split_str)
        if index < 0:
            return None

        label = text[0:index].strip()

        if label in self.label_table_field_dict:
            value = text[index+len(split_str):].strip()
            if self.replace_to_empty_list is not None and len(self.replace_to_empty_list) > 0:
                for replace_str in self.replace_to_empty_list:
                    value = value.replace(replace_str, '')
            data[self.label_table_field_dict[label]] = value

    """
    将内容数据生成sql并存到文件中
    """

    def __save_data_to_db(self, data):
        sql = self.__build_insert_sql(data)
        try:
            self.conn.execute_sql(sql)
        except mysql.connector.Error as e:
            print('执行sql失败!sql: {}'.format(sql))
            raise e

    """
    生成insert语句
    """

    def __build_insert_sql(self, data={}):
        name_list = []
        value_list = []
        for name in data.keys():
            name_list.append(name)
            if name in data:
                value_list.append(self.__convert_value(name, data[name]))
            elif name == 'gmt_create' or name == 'gmt_modify' :
                value_list.append('NOW()')
            else:
                value_list.append('\"\"')
        return 'REPLACE INTO {}({}) VALUES ({});'.format(self.table_name, ','.join(name_list), ','.join(value_list))

    def __convert_value(self, name, value):
        value = str(value).replace('\\', '\\\\').replace('"', '\\"')
        if name not in self.data_rule:
            return '\"' + value + '\"'

        rule = self.data_rule[name]
        suffix = rule['suffix']
        if suffix is not None:
            value = value.replace(suffix, '')

        data_type = rule['type']
        if data_type == 'number':
            return str(value)
        else:
            return '\"' + value + '\"'

    """
    获取页面标签和字段名映射
    """
    def get_label_table_field_dict(self):
        sql = '''
            select COLUMN_NAME,COLUMN_COMMENT from information_schema.columns where table_name='{}'
            '''.format(self.table_name)
        results = self.conn.query_sql(sql)
        label_table_field_dict = {}
        for row in results:
            name = row[0]
            comment = row[1].encode('utf-8')
            sub_comment_list = comment.split('|')
            for sub_comment in sub_comment_list:
                label_table_field_dict[sub_comment] = name

        return label_table_field_dict
