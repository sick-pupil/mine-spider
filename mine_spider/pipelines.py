# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import logging
import pymysql
from dbutils.pooled_db import PooledDB

class BilibiliAnimePipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-anime-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        

        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `block_hot_time_range`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `point`, 
                                                                `pubman_name`, 
                                                                `desc`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
        
        
        self.insert_video_tag_sql = """insert into `bilibili_videos_tags` (`bv`, 
                                                                            `tag`)
                                        values (%s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            pass
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                     item['rank_item_bv'], 
                                                     item['block_name'], 
                                                     item['block_hot_time_range'], 
                                                     item['rank_item_num'], 
                                                     item['rank_item_href'], 
                                                     item['rank_item_detail_title'], 
                                                     item['rank_item_detail_point'], 
                                                     item['rank_item_pubman'], 
                                                     item['rank_item_desc'], 
                                                     item['rank_item_time'], 
                                                     item['rank_item_play'], 
                                                     item['rank_item_danmu'], 
                                                     item['rank_item_star'], 
                                                     item['rank_item_coin']))
        
        
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
            
            
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频标签详情信息')
            tag_tuple_list = list()
            for tag in item['rank_item_video_detail']['video_detail_tags']:
                tag_tuple = tuple([item['rank_item_bv'], tag])
                tag_tuple_list.append(tag_tuple)
            self.db_cursor.executemany(self.insert_video_tag_sql, tag_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        return item


class BilibiliMusicPipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-music-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        
        
        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `pubman_name`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                         item['rank_item_bv'], 
                                                         item['block_name'], 
                                                         item['rank_item_num'], 
                                                         item['rank_item_href'], 
                                                         item['rank_item_detail_title'], 
                                                         item['rank_item_pubman'], 
                                                         item['rank_item_time'], 
                                                         item['rank_item_play'], 
                                                         item['rank_item_danmu'], 
                                                         item['rank_item_star'], 
                                                         item['rank_item_coin']))
                
                
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
           
           
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        
        return item


class BilibiliDougaPipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-douga-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        
        
        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `pubman_name`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                         item['rank_item_bv'], 
                                                         item['block_name'], 
                                                         item['rank_item_num'], 
                                                         item['rank_item_href'], 
                                                         item['rank_item_detail_title'], 
                                                         item['rank_item_pubman'], 
                                                         item['rank_item_time'], 
                                                         item['rank_item_play'], 
                                                         item['rank_item_danmu'], 
                                                         item['rank_item_star'], 
                                                         item['rank_item_coin']))
                
                
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
           
           
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        
        return item
    

class BilibiliGamePipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-game-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        
        
        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `pubman_name`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                         item['rank_item_bv'], 
                                                         item['block_name'], 
                                                         item['rank_item_num'], 
                                                         item['rank_item_href'], 
                                                         item['rank_item_detail_title'], 
                                                         item['rank_item_pubman'], 
                                                         item['rank_item_time'], 
                                                         item['rank_item_play'], 
                                                         item['rank_item_danmu'], 
                                                         item['rank_item_star'], 
                                                         item['rank_item_coin']))
                
                
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
           
           
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        
        return item
    
    
class BilibiliKichikuPipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-kichiku-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        
        
        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `pubman_name`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                         item['rank_item_bv'], 
                                                         item['block_name'], 
                                                         item['rank_item_num'], 
                                                         item['rank_item_href'], 
                                                         item['rank_item_detail_title'], 
                                                         item['rank_item_pubman'], 
                                                         item['rank_item_time'], 
                                                         item['rank_item_play'], 
                                                         item['rank_item_danmu'], 
                                                         item['rank_item_star'], 
                                                         item['rank_item_coin']))
                
                
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
           
           
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        
        return item
    
    
class BilibiliEntPipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-ent-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        
        
        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `pubman_name`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                         item['rank_item_bv'], 
                                                         item['block_name'], 
                                                         item['rank_item_num'], 
                                                         item['rank_item_href'], 
                                                         item['rank_item_detail_title'], 
                                                         item['rank_item_pubman'], 
                                                         item['rank_item_time'], 
                                                         item['rank_item_play'], 
                                                         item['rank_item_danmu'], 
                                                         item['rank_item_star'], 
                                                         item['rank_item_coin']))
                
                
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
           
           
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        
        return item
    
    
class BilibiliLifePipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-life-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        
        
        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `pubman_name`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                         item['rank_item_bv'], 
                                                         item['block_name'], 
                                                         item['rank_item_num'], 
                                                         item['rank_item_href'], 
                                                         item['rank_item_detail_title'], 
                                                         item['rank_item_pubman'], 
                                                         item['rank_item_time'], 
                                                         item['rank_item_play'], 
                                                         item['rank_item_danmu'], 
                                                         item['rank_item_star'], 
                                                         item['rank_item_coin']))
                
                
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
           
           
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        
        return item
    
    
class BilibiliTechPipeline(object):
    
    def __init__(self):
        self.logger = logging.getLogger('bilibili-tech-pipeline')
   
    def open_spider(self, spider):
        pass
        
        self.pool = PooledDB(pymysql,
                             mincached=5,
                             maxcached=5,
                             maxconnections=20,
                             blocking=True,
                             host=spider.settings.get('MYSQL_HOST'),
                             port=spider.settings.get('MYSQL_PORT'),
                             user=spider.settings.get('MYSQL_USER'),
                             db=spider.settings.get('MYSQL_DATABASE'),
                             password=spider.settings.get('MYSQL_PASSWORD'),
                             charset='utf8mb4')
        
        
        self.insert_rank_item_sql = """insert into `bilibili_rank_items` (`channel_name`,
                                                                `bv`, 
                                                                `block_name`, 
                                                                `order_num`, 
                                                                `href`, 
                                                                `title`, 
                                                                `pubman_name`, 
                                                                `time`, 
                                                                `play`, 
                                                                `danmu`, 
                                                                `star`, 
                                                                `coin`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_detail_sql = """insert into `bilibili_videos_detail` (`bv`, 
                                                                                `title`, 
                                                                                `play`, 
                                                                                `danmu`, 
                                                                                `pubtime`, 
                                                                                `like`, 
                                                                                `coin`, 
                                                                                `star`, 
                                                                                `share`, 
                                                                                `desc`, 
                                                                                `reply`, 
                                                                                `up_link`, 
                                                                                `up_name`, 
                                                                                `up_desc`, 
                                                                                `up_gz`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                                        
                                        
        self.insert_video_reply_sql = """insert into `bilibili_videos_replys` (`bv`, 
                                                                                `context`, 
                                                                                `time`, 
                                                                                `like`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_danmu_sql = """insert into `bilibili_videos_danmus` (`bv`, 
                                                                                `pubtime_in_video`, 
                                                                                `context`, 
                                                                                `pubtime`) 
                                        values (%s, %s, %s, %s)"""
                                        
        
        self.insert_video_up_sql = """insert into `bilibili_ups_info` (`bv`, 
                                                                        `up_uid`, 
                                                                        `up_name`, 
                                                                        `up_desc`, 
                                                                        `up_gz`, 
                                                                        `up_fs`, 
                                                                        `up_tg`, 
                                                                        `up_hj`) 
                                        values (%s, %s, %s, %s, %s, %s, %s, %s)"""
    
    def close_spider(self, spider):
        pass
    
    def process_item(self, item, spider):
        # 数据入库
        pass
        self.logger.info('采集热门项 bv : {}'.format(item['rank_item_bv']))
        self.logger.info('收集热门项信息')
        
        
        self.db_conn = self.pool.connection()
        self.db_cursor = self.db_conn.cursor()
        
        try:
            self.db_cursor.execute(self.insert_rank_item_sql, (item['channel_name'],
                                                         item['rank_item_bv'], 
                                                         item['block_name'], 
                                                         item['rank_item_num'], 
                                                         item['rank_item_href'], 
                                                         item['rank_item_detail_title'], 
                                                         item['rank_item_pubman'], 
                                                         item['rank_item_time'], 
                                                         item['rank_item_play'], 
                                                         item['rank_item_danmu'], 
                                                         item['rank_item_star'], 
                                                         item['rank_item_coin']))
                
                
            self.logger.info('收集视频详情信息')
            self.db_cursor.execute(self.insert_video_detail_sql, (item['rank_item_bv'], 
                                                     item['rank_item_video_detail']['video_detail_title'], 
                                                     item['rank_item_video_detail']['video_detail_play'], 
                                                     item['rank_item_video_detail']['video_detail_danmu'], 
                                                     item['rank_item_video_detail']['video_detail_pubtime'], 
                                                     item['rank_item_video_detail']['video_detail_like'], 
                                                     item['rank_item_video_detail']['video_detail_coin'], 
                                                     item['rank_item_video_detail']['video_detail_star'], 
                                                     item['rank_item_video_detail']['video_detail_share'], 
                                                     item['rank_item_video_detail']['video_detail_desc'], 
                                                     item['rank_item_video_detail']['video_detail_reply'], 
                                                     item['rank_item_video_detail']['video_detail_up_link'], 
                                                     item['rank_item_video_detail']['video_detail_up_name'], 
                                                     item['rank_item_video_detail']['video_detail_up_desc'],
                                                     item['rank_item_video_detail']['video_detail_up_gz']))
            
            
            self.logger.info('收集视频评论详情信息')
            reply_tuple_list = list()
            for reply in item['rank_item_video_detail']['video_detail_hot_replys']:
                reply_tuple = tuple([item['rank_item_bv'], reply['video_reply_context'], reply['video_reply_time'], reply['video_reply_like']])
                reply_tuple_list.append(reply_tuple)
            self.db_cursor.executemany(self.insert_video_reply_sql, reply_tuple_list)
           
           
            self.logger.info('收集视频弹幕详情信息')
            danmu_tuple_list = list()
            for danmu in item['rank_item_video_detail']['video_detail_danmus']:
                danmu_tuple = tuple([item['rank_item_bv'], danmu['video_danmu_pubtime_in_video'], danmu['video_danmu_context'], danmu['video_danmu_pubtime']])
                danmu_tuple_list.append(danmu_tuple)
            self.db_cursor.executemany(self.insert_video_danmu_sql, danmu_tuple_list)
            
            
            self.logger.info('收集视频发布人up详情信息')
            self.db_cursor.execute(self.insert_video_up_sql, (item['rank_item_bv'], 
                                                     item['rank_item_up_detail']['up_uid'], 
                                                     item['rank_item_up_detail']['up_name'], 
                                                     item['rank_item_up_detail']['up_desc'], 
                                                     item['rank_item_up_detail']['up_gz'], 
                                                     item['rank_item_up_detail']['up_fs'], 
                                                     item['rank_item_up_detail']['up_tg'], 
                                                     item['rank_item_up_detail']['up_hj']))
            
            self.db_conn.commit()
        except Exception as e:
            self.logger.error(repr(e))
            self.db_conn.rollback()
        finally:
            self.db_cursor.close()
            self.db_conn.close()
        
        
        return item