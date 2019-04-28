#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 爬虫

import lxml
import requests
from requests.exceptions import ConnectionError, Timeout, HTTPError
from lxml import etree
from db_connector import DBConnector
import time
import mysql.connector
import platform
import sys
import operator

if operator.lt(platform.python_version(), '3'):
    reload(sys)
    sys.setdefaultencoding('utf-8')


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
        self.conn = DBConnector(mysql_config)
        self.label_table_field_dict = self.get_label_table_field_dict()
        # 记录请求失败的列表页
        self.failed_page_list = []
        # 记录请求失败的内容页
        self.failed_href_list = []

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
            try:
                r = requests.get(url)
                if r.status_code == 200:
                    r.encoding = 'utf-8'
                    return etree.HTML(r.text)

                if retry_times >= Crawler.max_retry_times:
                    break

                retry_times += 1
                time.sleep(Crawler.retry_delay_secs*retry_times)
            except ConnectionError:
                continue
            except Timeout:
                continue
            except:
                continue

        return None

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

        print("正在获取第{}页数据...".format(page))
        html = self.__get(url)
        if html is None:
            print("========> 获取第{}页列表失败！！！".format(page))
            self.failed_page_list.append(page)
            return

        href_list = html.xpath("//div[@class='title text-oneline']/a/@href")
        if len(href_list) < 1:
            print("========> 获取第{}页列表失败！！！".format(page))
            self.failed_page_list.append(page)
            return

        for href in href_list:
            href = href[3:]
            self.__fetch_detail(self.content_base_url + href)

        print('-----------------> 第{}页数据获取完成.'.format(page))

    """
    抓内容页
    href: 内容网址
    """

    def __fetch_detail(self, href=None):
        html = self.__get(href)
        if html is None:
            self.failed_href_list.append(href)
            print('获取内容失败!href:{}'.format(href))
            return

        data = {
            "id": href.replace(self.content_base_url, ''),
            "source_url": href
        }

        self.__extract_content(html, data)
        self.__extract_category(html, data)
        self.__extract_instructions(html, data)
        self.__extract_image(html, data)

        try:
            self.__save_data_to_db(data)
        except mysql.connector.Error as e:
            self.failed_href_list.append(href)
            print('保存内容失败!href:{}, e:{}'.format(href, e))
            return False

        return True

    """
    提取内容
    """

    def __extract_content(self, html, data):
        node_list = html.xpath("//div[@id='content']/p")
        split_str = '：'
        for node in node_list:
            text = self.__encode_with_utf8(node.xpath("string(.)").strip()).replace('　', ' ')
            first_index = text.find(split_str)
            last_index = text.rfind(split_str)
            if first_index < 0:
                continue

            # 只有一个分隔符
            if first_index == last_index:
                self.__extract_content_item(text, data)
            else:  # 多个分隔符
                for item in text.split(' '):
                    self.__extract_content_item(item, data)

    """
    提取内容
    """

    def __extract_content_item(self, text, data):
        split_str = '：'
        index = text.find(split_str)
        if index < 0:
            return

        label = text[0:index].strip()
        if label in self.label_table_field_dict:
            value = text[index+1:].strip()
            if label == '批准文号' and value.find(self.__encode_with_utf8('本品为处方药，须凭处方购买')) >= 0:
                value = value.replace(self.__encode_with_utf8('本品为处方药，须凭处方购买'), '').strip()
                data[self.label_table_field_dict[self.__encode_with_utf8('是否处方药')]] = 1
            data[self.label_table_field_dict[label]] = value

    """
    提取说明书
    """

    def __extract_instructions(self, html, data):
        try:
            node_list = html.xpath("//div[@id='tab1']/ul/li")
            text_list = []
            for node in node_list:
                text_list.append(self.__encode_with_utf8(str(node.xpath("string(.)"))))
            data[self.label_table_field_dict[self.__encode_with_utf8('说明书')]] = '\n'.join(text_list)
        except:
            pass

    """
    提取分类
    """

    def __extract_category(self, html, data):
        try:
            text = html.xpath(
                "//div[@class='show-main fl']//a")[2].xpath('string(.)')
            text = text[2:]
            data[self.label_table_field_dict[self.__encode_with_utf8('分类')]] = text
        except:
            pass

    """
    提取图片
    """

    def __extract_image(self, html, data):
        try:
            img_src_list = html.xpath("//div[@id='tab2']//img/@src")
            img_src_list = map(
                lambda x: self.content_base_url+x[3:], img_src_list)
            data[self.label_table_field_dict[self.__encode_with_utf8('图片')]] = ','.join(img_src_list)
        except:
            pass

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
                value_list.append(self.__convert_to_db_value(name, data[name]))
            elif name == 'gmt_create' or name == 'gmt_modify':
                value_list.append('NOW()')
            else:
                value_list.append('\"\"')
        return 'REPLACE INTO {}({}) VALUES ({});'.format(self.table_name, ','.join(name_list), ','.join(value_list))

    def __convert_to_db_value(self, name, value):
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
            name = self.__encode_with_utf8(row[0])
            comment = self.__encode_with_utf8(row[1])
            sub_comment_list = comment.split('|')
            for sub_comment in sub_comment_list:
                label_table_field_dict[sub_comment] = name
        return label_table_field_dict

    def __encode_with_utf8(self, text):
        if text is None:
            return None
            
        return text.encode('utf-8').decode('utf-8')