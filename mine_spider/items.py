# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
# -*- coding: utf-8 -*-

from scrapy import Item, Field

class BilibiliItem(Item):
    
    channel_type : str = Field()
    data_list : list = Field()
    

class BilibiliAnimeItem(Item):
    
    block_name : str = Field()
    rank_hot_time_range : str = Field()
    rank_item_num : int = Field()
    rank_item_href : str = Field()
    rank_item_detail_title : str = Field()
    rank_item_detail_point : str = Field()
    rank_item_time : str = Field()
    rank_item_play : int = Field()
    rank_item_danmu : int = Field()
    rank_item_star : int = Field()
    rank_item_coin : int = Field()

