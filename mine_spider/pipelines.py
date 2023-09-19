# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import logging
import pymysql

class BilibiliAnimePipeline(object):
   
    def open_spider(self, spider):
        self.db_conn = pymysql.connect(host = spider.settings.get('MYSQL_HOST'), 
                                       port = spider.settings.get('MYSQL_PORT'), 
                                       db = spider.settings.get('MYSQL_DATABASE'), 
                                       user = spider.settings.get('MYSQL_USER'), 
                                       passwd = spider.settings.get('MYSQL_PASSWORD'), 
                                       charset = 'utf8')
        self.db_cursor = self.db_conn.cursor()
    
    def close_spider(self, spider):
        self.db_cursor.close()
        self.db_conn.close()
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        return item
