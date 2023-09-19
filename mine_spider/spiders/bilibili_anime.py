# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 19:39:43 2023

@author: Administrator
"""

from ..items import Result, BilibiliChannel, BilibiliRankItem, BilibiliVideoDetail, BilibiliVideoDanmu, BilibiliUpInfo
from scrapy import Spider, Selector, Request
import logging

class BilibiliAnimeSpider(Spider):
    
    name: str = 'bilibili-anime'
    allowed_domains: list = ['bilibili.com']
    start_url: str = 'https://www.bilibili.com/anime'
    custom_settings: dict = {
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 60 * 1000}, 
        'PLAYWRIGHT_MAX_CONTEXTS': 1,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 60 * 1000,
        'PLAYWRIGHT_MAX_PAGES_PER_CONTEXT': 20,
        'ITEM_PIPELINES': {
            'mine_spider.pipelines.BilibiliAnimePipeline': 500
        },
    }

    def __init__(self):
        self.channel_name = '番剧'
        self.result = Result()
        self.bilibili_anime = BilibiliChannel()
        self.bilibili_anime['channel_name'] = self.channel_name
        self.bilibili_anime['rank_list'] = list()
        self.result['data'] = self.bilibili_anime
    
    def start_requests(self):
        yield Request(url = self.start_url, 
            meta = {
                'playwright': True, 
                'playwright_context': 'bilibili-anime', 
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                 },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'load',    
                },
                'playwright_include_page': True,
            }, 
            callback = self.anime_parse,
            errback = self.err_anime_callback,
            dont_filter = True,
        )
    
    # 番剧
    async def anime_parse(self, response):        
        
        logging.info('请求频道{}， ua为{}'.format(self.channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(self.channel_name, response.request.headers['user-agent']))
            yield Request(url = self.start_url, 
                meta = {
                    'playwright': True, 
                    'playwright_context': 'bilibili-anime', 
                    'playwright_context_kwargs': {
                        'ignore_https_errors': True,
                     },
                    'playwright_page_goto_kwargs': {
                        'wait_until': 'load',    
                    },
                    'playwright_include_page': True,
                }, 
                callback = self.anime_parse,
                errback = self.err_anime_callback,
                dont_filter = True,
            )
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(self.channel_name))
                
            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
                
            # 开始解析
            pass
            
            # 取番剧区的每个小类区            
            block_list = await page.locator(selector = "//div[@class='block-area block-bangumi']", has = page.locator(selector = "//h4[@class='name']")).all()
            
            for block_item in block_list:
                # 小类区名称
                block_name = await block_item.locator("//h4[@class='name']").inner_text()
                # 小类区热门排行时间范围
                block_hot_time_range = await block_item.locator("//span[@class='selected']").inner_text()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                                
                for rank_item in rank_item_list_in_block:
                    # 热门排行项序号
                    rank_item_num = await rank_item.locator("//i[@class='ri-num']").inner_text()
                    rank_item_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
                    # 热门排行项视频链接
                    rank_item_href = await rank_item_base_info.get_attribute('href')
                    rank_item_detail = rank_item_base_info.locator("//div[@class='ri-detail']")
                    # 热门排行项视频标题
                    rank_item_detail_title = await rank_item_detail.locator("//p[@class='ri-title']").inner_text()
                    # 热门排行项综合评分
                    rank_item_detail_point = await rank_item_detail.locator("//p[@class='ri-point']").inner_text()
                    
                    animeRankItem = BilibiliRankItem()
                    animeRankItem['block_name'] = block_name
                    animeRankItem['block_hot_time_range'] = block_hot_time_range
                    animeRankItem['rank_item_num'] = rank_item_num
                    animeRankItem['rank_item_href'] = rank_item_href
                    animeRankItem['rank_item_detail_title'] = rank_item_detail_title
                    animeRankItem['rank_item_detail_point'] = rank_item_detail_point
                    
                    if block_name != '完结动画':
                        await rank_item.hover()
                        await page.wait_for_timeout(500)
                        video_info = page.locator("//div[@class='video-info-module']")
                        # 热门排行项发布时间
                        rank_item_time = await video_info.locator("//span[@class='time']").inner_text()
                        # 热门排行项播放次数
                        rank_item_play = await video_info.locator("//span[@class='play']").inner_text()
                        # 热门排行项弹幕数量
                        rank_item_danmu = await video_info.locator("//span[@class='danmu']").inner_text()
                        # 热门排行项收藏数量
                        rank_item_star = await video_info.locator("//span[@class='star']").inner_text()
                        # 热门排行项硬币数量
                        rank_item_coin = await video_info.locator("//span[@class='coin']").inner_text()
                        # 热门排行项发布人
                        rank_item_pubman = await video_info.locator("//span[@class='name']").inner_text()
                        # 热门排行项简介
                        rank_item_desc = await video_info.locator("//p[@class='txt']").inner_text()

                        animeRankItem['rank_item_pubman'] = rank_item_pubman
                        animeRankItem['rank_item_desc'] = rank_item_desc
                        animeRankItem['rank_item_time'] = rank_item_time
                        animeRankItem['rank_item_play'] = rank_item_play
                        animeRankItem['rank_item_danmu'] = rank_item_danmu
                        animeRankItem['rank_item_star'] = rank_item_star
                        animeRankItem['rank_item_coin'] = rank_item_coin
                    else:
                        animeRankItem['rank_item_pubman'] = ''
                        animeRankItem['rank_item_desc'] = ''
                        animeRankItem['rank_item_time'] = ''
                        animeRankItem['rank_item_play'] = ''
                        animeRankItem['rank_item_danmu'] = ''
                        animeRankItem['rank_item_star'] = ''
                        animeRankItem['rank_item_coin'] = ''

                    self.bilibili_anime['rank_list'].append(animeRankItem)
                
                # 点击热门时间范围
                await block_item.locator("//div[@class='pgc-rank-dropdown rank-dropdown']").hover()
                await block_item.locator("//li[@class='dropdown-item' and contains(text(), '一周')]").click()
                await page.wait_for_load_state("networkidle")
                
                # 小类区热门排行时间范围
                block_hot_time_range = await block_item.locator("//span[@class='selected']").inner_text()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                for rank_item in rank_item_list_in_block:
                    # 热门排行项序号
                    rank_item_num = await rank_item.locator("//i[@class='ri-num']").inner_text()
                    rank_item_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
                    # 热门排行项视频链接
                    rank_item_href = await rank_item_base_info.get_attribute('href')
                    rank_item_detail = rank_item_base_info.locator("//div[@class='ri-detail']")
                    # 热门排行项视频标题
                    rank_item_detail_title = await rank_item_detail.locator("//p[@class='ri-title']").inner_text()
                    # 热门排行项综合评分
                    rank_item_detail_point = await rank_item_detail.locator("//p[@class='ri-point']").inner_text()
                    
                    animeRankItem = BilibiliRankItem()
                    animeRankItem['block_name'] = block_name
                    animeRankItem['block_hot_time_range'] = block_hot_time_range
                    animeRankItem['rank_item_num'] = rank_item_num
                    animeRankItem['rank_item_href'] = rank_item_href
                    animeRankItem['rank_item_detail_title'] = rank_item_detail_title
                    animeRankItem['rank_item_detail_point'] = rank_item_detail_point
                    
                    if block_name != '完结动画':
                        await rank_item.hover()
                        await page.wait_for_timeout(500)
                        video_info = page.locator("//div[@class='video-info-module']")
                        # 热门排行项发布时间
                        rank_item_time = await video_info.locator("//span[@class='time']").inner_text()
                        # 热门排行项播放次数
                        rank_item_play = await video_info.locator("//span[@class='play']").inner_text()
                        # 热门排行项弹幕数量
                        rank_item_danmu = await video_info.locator("//span[@class='danmu']").inner_text()
                        # 热门排行项收藏数量
                        rank_item_star = await video_info.locator("//span[@class='star']").inner_text()
                        # 热门排行项硬币数量
                        rank_item_coin = await video_info.locator("//span[@class='coin']").inner_text()
                        # 热门排行项发布人
                        rank_item_pubman = await video_info.locator("//span[@class='name']").inner_text()
                        # 热门排行项简介
                        rank_item_desc = await video_info.locator("//p[@class='txt']").inner_text()
                        
                        animeRankItem['rank_item_pubman'] = rank_item_pubman
                        animeRankItem['rank_item_desc'] = rank_item_desc
                        animeRankItem['rank_item_time'] = rank_item_time
                        animeRankItem['rank_item_play'] = rank_item_play
                        animeRankItem['rank_item_danmu'] = rank_item_danmu
                        animeRankItem['rank_item_star'] = rank_item_star
                        animeRankItem['rank_item_coin'] = rank_item_coin            
                    else:
                        animeRankItem['rank_item_pubman'] = ''
                        animeRankItem['rank_item_desc'] = ''
                        animeRankItem['rank_item_time'] = ''
                        animeRankItem['rank_item_play'] = ''
                        animeRankItem['rank_item_danmu'] = ''
                        animeRankItem['rank_item_star'] = ''
                        animeRankItem['rank_item_coin'] = ''

                    self.bilibili_anime['rank_list'].append(animeRankItem)

        await page.close()
        
        # 产生日志
        for animeRankItem in self.bilibili_anime['rank_list']:
            logging.info('{}频道{}区域{}热门排行：{}：{} {} {}'.format(self.channel_name, 
                                                     animeRankItem['block_name'], 
                                                     animeRankItem['block_hot_time_range'],
                                                     animeRankItem['rank_item_num'], 
                                                     animeRankItem['rank_item_detail_title'], 
                                                     animeRankItem['rank_item_detail_point'], 
                                                     animeRankItem['rank_item_desc']))
            logging.info('发布时间{} 播放次数{} 弹幕量{} 收藏数{} 硬币数{} 发布人{}'.format(animeRankItem['rank_item_time'], 
                                                     animeRankItem['rank_item_play'], 
                                                     animeRankItem['rank_item_danmu'],
                                                     animeRankItem['rank_item_star'], 
                                                     animeRankItem['rank_item_coin'], 
                                                     animeRankItem['rank_item_pubman']))
            
        yield self.result

    async def err_anime_callback(self, failure):
        page = failure.request.meta["playwright_page"]
        logging.error('番剧页面加载出错')
        await page.close()
    
