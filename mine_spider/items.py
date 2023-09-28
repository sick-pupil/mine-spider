# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html
# -*- coding: utf-8 -*-

from scrapy import Item, Field

# 数据返回
class BilibiliResult(Item):
    
    code : int = Field()
    msg : str = Field()
    data : list = Field()

# 频道新区new rank
class BilibiliNewRankItem(Item):
    
    # 小类区名字
    area_name : str = Field()
    # 小类区热门项排行
    rank_item_order : int = Field()
    # 热门项视频标题
    rank_item_title : str = Field()
    # 热门项视频up名称
    rank_item_up_name : str = Field()
    # 热门项视频标题
    rank_video_card_title : str = Field()
    # 热门项视频up名称
    rank_video_card_up_name : str = Field()
    # 热门项视频发布时间
    rank_video_card_pubdate : str = Field()
    # 热门项视频播放量
    rank_video_card_play : str = Field()
    # 热门项视频弹幕量
    rank_video_card_danmu : str = Field()
    # 热门项视频收藏量
    rank_video_card_star : str = Field()
    # 热门项视频投币量
    rank_video_card_coin : str = Field()

# 频道区rank
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
    # 视频最热评论
    video_detail_hot_replys : list = Field()
    # 视频所有弹幕
    video_detail_danmus : list = Field()
    # 视频发布人个人空间链接
    video_detail_up_link : str = Field()
    # 视频发布人名称
    video_detail_up_name : str = Field()
    # 视频发布人简介
    video_detail_up_desc : str = Field()
    # 视频发布人被关注数量
    video_detail_up_gz : str = Field()

# 视频弹幕详情
class BilibiliVideoDanmu(Item):
    
    # 弹幕发送的视频时间节点
    video_danmu_pubtime_in_video : str = Field()
    # 弹幕内容
    video_danmu_context : str = Field()
    # 弹幕发送时间
    video_danmu_pubtime : str = Field()

# 视频评论详情
class BilibiliVideoReply(Item):
    
    # 评论内容
    video_reply_context : str = Field()
    # 评论时间
    video_reply_time : str = Field()
    # 评论被点赞数
    video_reply_like : str = Field()

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
