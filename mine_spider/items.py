# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
# -*- coding: utf-8 -*-

from scrapy import Item, Field

# 总返回
class Result(Item):
    
    code : int = Field()
    msg : str = Field()
    data : object = Field()

# B站番剧区返回
class BilibiliChannel(Item):
    
    # 频道类型
    channel_name : str = Field()
    # 频道热门列表
    rank_list : list = Field()

# 番剧区rank
class BilibiliRankItem(Item):
    
    # 小类区名字
    block_name : str = Field()
    # 小类区热门排行时间范围
    block_hot_time_range : str = Field()
    # 热门排行项序号
    rank_item_num : int = Field()
    # 热门排行项链接
    rank_item_href : str = Field()
    # 热门排行项标题
    rank_item_detail_title : str = Field()
    # 热门排行项综合评分
    rank_item_detail_point : str = Field()
    # 热门排行项发布人
    rank_item_pubman : str = Field()
    # 热门排行项发布简介
    rank_item_desc : str = Field()
    # 热门排行项发布时间
    rank_item_time : str = Field()
    # 热门排行项播放次数
    rank_item_play : str = Field()
    # 热门排行项弹幕数量
    rank_item_danmu : str = Field()
    # 热门排行项收藏数量
    rank_item_star : str = Field()
    # 热门排行项硬币数量
    rank_item_coin : str = Field()

# 视频详情信息
class BilibiliVideoDetail(Item):
    
    # 视频标题
    video_detail_title : str = Field()
    # 视频播放数
    video_detail_play : str = Field()
    # 视频弹幕数
    video_detail_danmu : str = Field()
    # 视频发布具体时间
    video_detail_pubtime : str = Field()
    # 视频点赞数
    video_detail_like : str = Field()
    # 视频投币数
    video_detail_coin : str = Field()
    # 视频收藏数
    video_detail_star : str = Field()
    # 视频转发数
    video_detail_share : str = Field()
    # 视频标签
    video_detail_tags : list = Field()
    # 视频简介
    video_detail_desc : str = Field()
    # 视频总评论数
    video_detail_reply : str = Field()
    # 视频最热前100条评论
    video_detail_hot_replys : list = Field()
    # 视频所有弹幕
    video_detail_danmus : list = Field()
    # 视频发布人信息
    video_detail_ups : list = Field()

# 视频弹幕详情
class BilibiliVideoDanmu(Item):
    
    # 弹幕发送的视频时间节点
    video_danmu_pubtime_in_video : str = Field()
    # 弹幕内容
    video_danmu_context : str = Field()
    # 弹幕发送时间
    video_danmu_pubtime : str = Field()

# 视频发布人信息
class BilibiliUpInfo(Item):
    
    # up名称
    up_name : str = Field()
    # up性别
    up_sex : str = Field()
    # up简介
    up_desc : str = Field()
    # up关注数
    up_gz : str = Field()
    # up粉丝数
    up_fs : str = Field()
    # up投稿数
    up_tg : str = Field()
    # up合集和列表数
    up_hj : str = Field()
    # up充电人数
    up_elec : str = Field()
    # up uid
    up_uid : str = Field()