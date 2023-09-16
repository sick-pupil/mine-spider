# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import logging
import pymysql

class BilibiliPipeline(object):
   
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
        if item['channel_type'] == '番剧':
            data_list = item['data_list']
            for data_item in data_list:
                logging.info(data_item['rank_item_detail_title'])
        return item
