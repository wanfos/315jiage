#!/usr/bin/env python
# -*- coding:utf-8 -*-

import mysql.connector

class DBConnector:
  
    def __init__(self, config={}):
        self.config = config
        self.conn = None
        self.cursor = None

    def __del__(self):
        if self.cursor is not None:
            self.cursor.close()
            self.cursor = None
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    # 连接数据库
    def __connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as e:
            print('连接数据库失败!{}'.format(e))
            raise Exception('连接数据库失败!{}'.format(e))

    def execute_sql(self, sql):
        if sql is None:
            return

        if self.conn is None:
            self.__connect()

        try:
            self.cursor.execute(sql)
            self.conn.commit()
        except mysql.connector.Error as e:
            if e.args[0] in (2006, 2013, 2014, 2045, 2055):
                self.__connect()
            raise e

    def query_sql(self, sql):
        if sql is None:
            return

        if self.conn is None:
            self.__connect()

        try:
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except mysql.connector.Error as e:
            if e.args[0] in (2006, 2013, 2014, 2045, 2055):
                self.__connect()
            raise e