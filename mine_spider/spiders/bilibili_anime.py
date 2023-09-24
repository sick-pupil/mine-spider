# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 19:39:43 2023

@author: Administrator
"""

from ..items import Result, BilibiliChannel, BilibiliRankItem, BilibiliVideoDetail, BilibiliVideoDanmu, BilibiliUpInfo
from ..settings import UA_LIST as ua_list
from scrapy import Spider, Selector, Request
import logging
import random

class BilibiliAnimeSpider(Spider):
    
    name: str = 'bilibili-anime'
    allowed_domains: list = ['bilibili.com']
    start_url: str = 'https://www.bilibili.com/anime'
    custom_settings: dict = {
        'PLAYWRIGHT_BROWSER_TYPE': 'firefox',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 30 * 60 * 1000}, 
        'PLAYWRIGHT_MAX_CONTEXTS': 1,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 30 * 60 * 1000,
        'PLAYWRIGHT_MAX_PAGES_PER_CONTEXT': 10,
        'ITEM_PIPELINES': {
            'mine_spider.pipelines.BilibiliAnimePipeline': 500
        },
        'DOWNLOADER_MIDDLEWARES': {
        #    'mine_spider.middlewares.MineSpiderDownloaderMiddleware': 900,
        }
    }

    def __init__(self):
        self.channel_name = '番剧'
        self.result = Result()
        self.bilibili_anime = BilibiliChannel()
        self.bilibili_anime['channel_name'] = self.channel_name
        self.bilibili_anime['rank_list'] = list()
        self.result['data'] = self.bilibili_anime
        
        self.url_prefix = 'https:'
    
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
                block_item_selector = Selector(text = await block_item.inner_html())
                # 小类区名称
                block_name = block_item_selector.xpath(query = "//h4[@class='name']/text()").extract_first()
                # 小类区热门排行时间范围
                block_hot_time_range = block_item_selector.xpath(query = "//span[@class='selected']/text()").extract_first()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                                
                for rank_item in rank_item_list_in_block:
                    rank_item_selector = Selector(text = await rank_item.inner_html())                    
                    # 热门排行项序号
                    rank_item_num = rank_item_selector.xpath(query = "//i[@class='ri-num']/text()").extract_first()
                    # 热门排行项视频链接
                    rank_item_href = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/@href").extract_first()
                    # 热门排行项视频标题
                    rank_item_detail_title = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-detail']/p[@class='ri-title']/text()").extract_first()
                    # 热门排行项综合评分
                    rank_item_detail_point = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-detail']/p[@class='ri-point']/text()").extract_first()
                    
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
                        video_info_html = await page.locator("//div[@class='video-info-module']").inner_html()
                        video_info_selector = Selector(text = video_info_html)
                        # 热门排行项发布时间
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='time']/text()").extract_first()
                        # 热门排行项播放次数
                        rank_item_play = video_info_selector.xpath(query = "//span[@class='play']/text()").extract_first()
                        # 热门排行项弹幕数量
                        rank_item_danmu = video_info_selector.xpath(query = "//span[@class='danmu']/text()").extract_first()
                        # 热门排行项收藏数量
                        rank_item_star = video_info_selector.xpath(query = "//span[@class='star']/text()").extract_first()
                        # 热门排行项硬币数量
                        rank_item_coin = video_info_selector.xpath(query = "//span[@class='coin']/text()").extract_first()
                        # 热门排行项发布人
                        rank_item_pubman = video_info_selector.xpath(query = "//span[@class='name']/text()").extract_first()
                        # 热门排行项简介
                        rank_item_desc = video_info_selector.xpath(query = "//p[@class='txt']/text()").extract_first()

                        animeRankItem['rank_item_pubman'] = rank_item_pubman
                        if rank_item_desc is None:
                            animeRankItem['rank_item_desc'] = ''
                        else:
                            animeRankItem['rank_item_desc'] = rank_item_desc
                        animeRankItem['rank_item_time'] = rank_item_time
                        animeRankItem['rank_item_play'] = rank_item_play.strip()
                        animeRankItem['rank_item_danmu'] = rank_item_danmu.strip()
                        animeRankItem['rank_item_star'] = rank_item_star.strip()
                        animeRankItem['rank_item_coin'] = rank_item_coin.strip()
                    else:
                        animeRankItem['rank_item_pubman'] = ''
                        animeRankItem['rank_item_desc'] = ''
                        animeRankItem['rank_item_time'] = ''
                        animeRankItem['rank_item_play'] = ''
                        animeRankItem['rank_item_danmu'] = ''
                        animeRankItem['rank_item_star'] = ''
                        animeRankItem['rank_item_coin'] = ''
                    
                    self.bilibili_anime['rank_list'].append(animeRankItem)
                    
                    if block_name != '连载动画' and block_name != '完结动画': #and 'BV1ku4y1z7gu' in rank_item_href:
                        pass
                        yield Request(url = self.url_prefix + rank_item_href,
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
                            callback = self.anime_video_parse,
                            errback = self.err_anime_callback,
                            dont_filter = True,
                        )
                
                # 点击热门时间范围
                await block_item.locator("//div[@class='pgc-rank-dropdown rank-dropdown']").hover()
                await block_item.locator("//li[@class='dropdown-item' and contains(text(), '一周')]").click()
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(500)
                block_item_selector = Selector(text = await block_item.inner_html())
                
                # 小类区热门排行时间范围
                block_hot_time_range = block_item_selector.xpath(query = "//span[@class='selected']/text()").extract_first()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                for rank_item in rank_item_list_in_block:
                    rank_item_selector = Selector(text = await rank_item.inner_html())
                    # 热门排行项序号
                    rank_item_num = rank_item_selector.xpath(query = "//i[@class='ri-num']/text()").extract_first()
                    # 热门排行项视频链接
                    rank_item_href = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/@href").extract_first()
                    # 热门排行项视频标题
                    rank_item_detail_title = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-detail']/p[@class='ri-title']/text()").extract_first()
                    # 热门排行项综合评分
                    rank_item_detail_point = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-detail']/p[@class='ri-point']/text()").extract_first()
                    
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
                        video_info_html = await page.locator("//div[@class='video-info-module']").inner_html()
                        video_info_selector = Selector(text = video_info_html)
                        # 热门排行项发布时间
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='time']/text()").extract_first()
                        # 热门排行项播放次数
                        rank_item_play = video_info_selector.xpath(query = "//span[@class='play']/text()").extract_first()
                        # 热门排行项弹幕数量
                        rank_item_danmu = video_info_selector.xpath(query = "//span[@class='danmu']/text()").extract_first()
                        # 热门排行项收藏数量
                        rank_item_star = video_info_selector.xpath(query = "//span[@class='star']/text()").extract_first()
                        # 热门排行项硬币数量
                        rank_item_coin = video_info_selector.xpath(query = "//span[@class='coin']/text()").extract_first()
                        # 热门排行项发布人
                        rank_item_pubman = video_info_selector.xpath(query = "//span[@class='name']/text()").extract_first()
                        # 热门排行项简介
                        rank_item_desc = video_info_selector.xpath(query = "//p[@class='txt']/text()").extract_first()
                        
                        animeRankItem['rank_item_pubman'] = rank_item_pubman
                        if rank_item_desc is None:
                            animeRankItem['rank_item_desc'] = ''
                        else:
                            animeRankItem['rank_item_desc'] = rank_item_desc
                        animeRankItem['rank_item_time'] = rank_item_time
                        animeRankItem['rank_item_play'] = rank_item_play.strip()
                        animeRankItem['rank_item_danmu'] = rank_item_danmu.strip()
                        animeRankItem['rank_item_star'] = rank_item_star.strip()
                        animeRankItem['rank_item_coin'] = rank_item_coin.strip()
                    else:
                        animeRankItem['rank_item_pubman'] = ''
                        animeRankItem['rank_item_desc'] = ''
                        animeRankItem['rank_item_time'] = ''
                        animeRankItem['rank_item_play'] = ''
                        animeRankItem['rank_item_danmu'] = ''
                        animeRankItem['rank_item_star'] = ''
                        animeRankItem['rank_item_coin'] = ''

                    self.bilibili_anime['rank_list'].append(animeRankItem)
                    
                    if block_name != '连载动画' and block_name != '完结动画': # and 'BV1ku4y1z7gu' in rank_item_href:
                        pass
                        yield Request(url = self.url_prefix + rank_item_href,
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
                            callback = self.anime_video_parse,
                            errback = self.err_anime_callback,
                            dont_filter = True,
                        )
                
        # 产生日志
        for animeRankItem in self.bilibili_anime['rank_list']:
            pass
            #logging.info('{}频道{}区域{}热门排行：{}：{}\n{}\n链接: {}\n简介: {}'.format(self.channel_name, 
            #                                         animeRankItem['block_name'], 
            #                                         animeRankItem['block_hot_time_range'],
            #                                         animeRankItem['rank_item_num'], 
            #                                         animeRankItem['rank_item_detail_title'], 
            #                                         animeRankItem['rank_item_detail_point'], 
            #                                         animeRankItem['rank_item_href'],
            #                                         animeRankItem['rank_item_desc']))
            #logging.info('发布时间{} 播放次数{} 弹幕量{} 收藏数{} 硬币数{} 发布人: {}\n'.format(animeRankItem['rank_item_time'], 
            #                                         animeRankItem['rank_item_play'], 
            #                                         animeRankItem['rank_item_danmu'],
            #                                         animeRankItem['rank_item_star'], 
            #                                         animeRankItem['rank_item_coin'], 
            #                                         animeRankItem['rank_item_pubman']))
                    
        await page.close()
        yield self.result
    
    async def anime_video_parse(self, response):
        
        logging.info('获取视频详情, 请求频道{}，视频链接{}, ua为{}'.format(self.channel_name, response.request.url, response.request.headers['user-agent']))
        page = response.meta['playwright_page']
        
        while await page.locator("//div[@class='bui-collapse-header']").count() == 0:
            await page.wait_for_timeout(500)
        
        
        await page.locator("//div[@class='bui-collapse-header']").click()
        while await page.locator("//li[@class='bui-long-list-item']").count() == 0:
            await page.wait_for_timeout(500)
        
        resp = await page.content()
        selector = Selector(text = resp)
        
        for dammu_item in selector.xpath("//li[@class='bui-long-list-item']"):
            logging.info('时间: {}, 弹幕内容: {}, 发送时间: {}'.format(dammu_item.xpath(".//span[@class='dm-info-time']/text()").extract_first(), 
                                                      dammu_item.xpath(".//span[@class='dm-info-dm']/text()").extract_first(), 
                                                      dammu_item.xpath(".//span[@class='dm-info-date']/text()").extract_first()))
        
        logging.info('视频标题 : {}'.format(selector.xpath("//div[@id='viewbox_report']/h1[@class='video-title']/text()").extract_first()))
        video_info_detail = selector.xpath("//div[@class='video-info-detail-list']")
        logging.info('视频播放量 : {}'.format(video_info_detail.xpath("//span[@class='view item']/text()").extract_first()))
        logging.info('视频弹幕量 : {}'.format(video_info_detail.xpath("//span[@class='dm item']/text()").extract_first()))
        logging.info('视频发布时间 : {}'.format(video_info_detail.xpath("//span[@class='pubdate-ip item']/text()").extract_first()))
        
        up_info = selector.xpath("//div[contains(@class, 'up-info-container')]")
        up_link = up_info.xpath("//div[@class='up-info--left']/descendant::a[@class='up-avatar']/@href").extract_first()
        logging.info('up主个人空间链接 : {}'.format(up_info.xpath("//div[@class='up-info--left']/descendant::a[@class='up-avatar']/@href").extract_first()))
        logging.info('up主名称 : {}'.format(up_info.xpath("//div[@class='up-info--right']/descendant::a[@class='up-name is_vip']/text()").extract_first()))
        logging.info('up主简介 : {}'.format(up_info.xpath("//div[@class='up-info--right']/descendant::div[@class='up-description up-detail-bottom']/text()").extract_first()))
        logging.info('up主已关注数量 : {}'.format(up_info.xpath("//div[@class='up-info--right']/descendant::span[@class='follow-btn-inner']/text()").extract_first()))
        
        video_toolbar = selector.xpath("//div[@class='video-toolbar-left']")
        logging.info('视频点赞量 : {}'.format(video_toolbar.xpath("//span[@class='video-like-info video-toolbar-item-text']/text()").extract_first()))
        logging.info('视频投币量 : {}'.format(video_toolbar.xpath("//span[@class='video-coin-info video-toolbar-item-text']/text()").extract_first()))
        logging.info('视频收藏量 : {}'.format(video_toolbar.xpath("//span[@class='video-fav-info video-toolbar-item-text']/text()").extract_first()))
        logging.info('视频转发量 : {}'.format(video_toolbar.xpath("//span[@class='video-share-info-text']/text()").extract_first()))
        
        under_video_container = selector.xpath("//div[@class='left-container-under-player']")
        logging.info('视频简介 : {}'.format(under_video_container.xpath("//span[@class='desc-info-text']/text()").extract_first()))
        
        for tag in under_video_container.xpath("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link topic-link']/span[@class='tag-txt']/text()"):
            logging.info('视频标签 : {}'.format(tag.get()))
        for tag in under_video_container.xpath("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link newchannel-link van-popover__reference']/text()"):
            logging.info('视频标签 : {}'.format(tag.get()))
        for tag in under_video_container.xpath("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link']/text()"):
            logging.info('视频标签 : {}'.format(tag.get()))
        
        while await page.locator(selector = "//div[@class='login-prompt']").count() == 0:
            await page.wait_for_timeout(500)
        await page.locator(selector = "//div[@class='login-prompt']").scroll_into_view_if_needed()
        
        while await page.locator(selector = "//div[@class='reply-list']/descendant::div[@class='root-reply']").count() == 0:
            await page.wait_for_timeout(500)
                
        resp = await page.content()
        selector = Selector(text = resp)
                
        for list_item in selector.xpath(query = "//div[@class='reply-list']/descendant::div[@class='root-reply']"):
            logging.info('评论内容 : {}'.format(list_item.xpath(".//span[@class='reply-content']/text()").extract_first()))
            logging.info('评论时间 : {}'.format(list_item.xpath(".//span[@class='reply-time']/text()").extract_first()))
            logging.info('评论被点赞数 : {}'.format(list_item.xpath(".//span[@class='reply-like']/span/text()").extract_first()))
        
        yield Request(url = self.url_prefix + up_link,
            meta = {
                'playwright': True, 
                'playwright_context': 'bilibili-anime', 
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                 },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'networkidle',
                },
                'playwright_include_page': True,
            }, 
            callback = self.up_info_parse,
            errback = self.err_anime_callback,
            dont_filter = True,
        )
        
        await page.close()
        
    async def up_info_parse(self, response):
        
        logging.info('获取视频发布人详情, 请求频道{}，up个人空间链接{}, ua为{}'.format(self.channel_name, response.request.url, response.request.headers['user-agent']))
        page = response.meta['playwright_page']
            
        up_basic_info = await page.locator("//div[@class='h-basic']").inner_html()
        up_basic_info_selector = Selector(text = up_basic_info)
        logging.info('up名称 : {}'.format(up_basic_info_selector.xpath("//span[@id='h-name']/text()").extract_first()))
        logging.info('up简介 : {}'.format(up_basic_info_selector.xpath("//div[@class='h-basic-spacing']/h4[@class='h-sign']/text()").extract_first()))
        
        up_tab_links = await page.locator("//div[@id='navigator']/descendant::div[@class='n-tab-links']").inner_html()
        up_tab_links_selector = Selector(text = up_tab_links)
        logging.info('up投稿数 : {}'.format(up_tab_links_selector.xpath("//a[@class='n-btn n-video n-audio n-article n-album']/span[@class='n-num']/text()").extract_first()))
        logging.info('up合集数 : {}'.format(up_tab_links_selector.xpath("//a[@class='n-btn n-channel']/span[@class='n-num']/text()").extract_first()))
        
        up_gz_fs = await page.locator("//div[@id='navigator']/descendant::div[@class='n-statistics']").inner_html()
        up_gz_fs_selector = Selector(text = up_gz_fs)
        logging.info('up被关注数 : {}'.format(up_gz_fs_selector.xpath("//div[@class='n-data n-gz']/p[@id='n-gz']/text()").extract_first()))
        logging.info('up粉丝数 : {}'.format(up_gz_fs_selector.xpath("//div[@class='n-data n-fs']/p[@id='n-fs']/text()").extract_first()))
        
        logging.info('up UID : {}'.format(await page.locator("//div[@class='info-wrap']/span[@class='info-value']").first.inner_text()))
        
        await page.close()

    async def err_anime_callback(self, failure):
        logging.info(repr(failure))
        page = failure.request.meta["playwright_page"]
        logging.error('番剧页面加载出错')
        #await page.screenshot(path="/screenshot.png", full_page=True)
        #with open(file = "/screenshot.html", mode = 'w', encoding = 'utf-8') as f:
        #    f.write(await page.content())
        await page.close()
    
