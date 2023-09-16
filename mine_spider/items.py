# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
# -*- coding: utf-8 -*-

from scrapy import Item, Field

class BilibiliItem(Item):
    
    channel_type = Field()
    data_list = Field()
    
    '''
    def __init__(self, channel_type, data_list):
        self.channel_type : str = channel_type
        self.data_list : list = data_list
    '''

class BilibiliAnimeItem(Item):
    
    block_name = Field()
    rank_hot_time_range = Field()
    rank_item_num = Field()
    rank_item_href = Field()
    rank_item_detail_title = Field()
    rank_item_detail_point = Field()
    rank_item_time = Field()
    rank_item_play = Field()
    rank_item_danmu = Field()
    rank_item_star = Field()
    rank_item_coin = Field()
    
    '''
    def __init__(self, 
                 block_name, 
                 rank_hot_time_range, 
                 rank_item_num, 
                 rank_item_href, 
                 rank_item_detail_title, 
                 rank_item_detail_point, 
                 rank_item_time, 
                 rank_item_play,
                 rank_item_danmu,
                 rank_item_star,
                 rank_item_coin):
        self.block_name : str = block_name
        self.rank_hot_time_range : str = rank_hot_time_range
        self.rank_item_num : int = rank_item_num
        self.rank_item_href : str = rank_item_href
        self.rank_detail_title : str = rank_item_detail_title
        self.rank_detail_point : str = rank_item_detail_point
        self.rank_item_time : str = rank_item_time
        self.rank_item_play : int = rank_item_play
        self.rank_item_danmu : int = rank_item_danmu
        self.rank_item_star : int = rank_item_star
        self.rank_item_coin : int = rank_item_coin
    '''
