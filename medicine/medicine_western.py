#!/usr/bin/env python
# -*- coding:utf-8 -*-
# 药品价格315网“西药价格”数据抓取

import sys
sys.path.append("..")
from crawler_manager import CrawlerManger

# 列表URL
LIST_BASE_URL = 'https://www.315jiage.cn/XiYao/default{}.htm'
# 内容URL
CONTENT_BASE_URL = 'https://www.315jiage.cn/'
# 表创建语句
TABLE_CREATE_SQL = '''
    CREATE TABLE IF NOT EXISTS `medicine_western` (
    `id` varchar(200) NOT NULL COMMENT '主键',
    `name` varchar(200) DEFAULT '' COMMENT '产品名称|药品名称',
    `pinyin` varchar(20) DEFAULT '' COMMENT '拼音简码',
    `price_retail` decimal(20,4) DEFAULT 0 COMMENT '零售价格',
    `price_wholesale` decimal(20,4)DEFAULT 0  COMMENT '批发价格',
    `price_trend` varchar(20) DEFAULT '' COMMENT '价格趋势',
    `spec` varchar(1024) DEFAULT '' COMMENT '规格',
    `dosage_form` varchar(500) DEFAULT '' COMMENT '剂型',
    `package_unit` varchar(20) DEFAULT '' COMMENT '包装单位',
    `approval_number` varchar(40) DEFAULT '' COMMENT '批准文号',
    `producer` varchar(200) DEFAULT '' COMMENT '生产厂家',
    `barcode` varchar(40) DEFAULT '' COMMENT '条形码',
    `attending` varchar(512) DEFAULT '' COMMENT '主治疾病',
    `instructions` text COMMENT '说明书',
    `source_url` varchar(200) DEFAULT '' COMMENT '网页地址',
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='西药'
'''
# 数据规则
DATA_RULE = {
    'price_retail': {
        'type': 'number',
        'suffix': '元'},
    'price_wholesale': {
        'type': 'number',
        'suffix': '元'}
}

# 数据库配置
MYSQL_CONFIG = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '123456',
    'port': 3306,
    'database': 'viewchaindb',
    'charset': 'utf8'
}
# 表名
TABLE_NAME = 'medicine_western'


if __name__ == '__main__':
    manager = CrawlerManger(LIST_BASE_URL, CONTENT_BASE_URL, MYSQL_CONFIG, TABLE_NAME, TABLE_CREATE_SQL,DATA_RULE)
    manager.fetch_by_page_range(590, 1000, 100, 1)
    # manager.fetch_by_href_list(['https://www.315jiage.cn/x-BuYiLei/265526.htm'])

    # 拉取失败的数据
    # manager.fetch_failed_data()
