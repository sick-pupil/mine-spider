# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 15:43:19 2023

@author: ASUS
"""

from ..items import BilibiliNewRankItem, BilibiliRankItem, BilibiliVideoDetail, BilibiliVideoDanmu, BilibiliVideoReply, BilibiliUpInfo
from scrapy import Spider, Selector, Request
from datetime import datetime
from playwright.sync_api import TimeoutError
from playwright._impl._api_types import Error
from scrapy_playwright.page import PageMethod


class BilibiliMusicSpider(Spider):
    
    name: str = 'bilibili-music'
    allowed_domains: list = ['bilibili.com']
    start_url: str = 'https://www.bilibili.com/v/music'
    custom_settings: dict = {
        'PLAYWRIGHT_BROWSER_TYPE': 'firefox',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 1000 * 60 * 60 * 3}, 
        'PLAYWRIGHT_MAX_CONTEXTS': 1,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 1000 * 60 * 60 * 3,
        'PLAYWRIGHT_MAX_PAGES_PER_CONTEXT': 50,
        'ITEM_PIPELINES': {
            'mine_spider.pipelines.BilibiliMusicPipeline': 500
        },
        'DOWNLOADER_MIDDLEWARES': {
            'mine_spider.middlewares.MineSpiderDownloaderMiddleware': 900,
        }
    }

    def __init__(self):
        self.channel_name : str = '音乐'
        self.url_prefix : str = 'https:'
    
    def start_requests(self):
        yield Request(url = self.start_url,
            meta = {
                'playwright': True, 
                'playwright_context': 'bilibili-music', 
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'load',
                    'timeout': 1000 * 60 * 30,
                },
                "playwright_page_methods": [
                    PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 30),
                    PageMethod("set_default_timeout", timeout=1000 * 60 * 30),
                ],
                'playwright_include_page': True,
            }, 
            callback = self.music_parse,
            errback = self.err_music_callback,
            dont_filter = True,
        )
    
    # 番剧
    async def music_parse(self, response):        
                
        self.logger.info('请求频道{}， ua为{}'.format(self.channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            self.logger.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(self.channel_name, response.request.headers['user-agent']))
            yield Request(url = self.start_url,
                meta = {
                    'playwright': True, 
                    'playwright_context': 'bilibili-music', 
                    'playwright_context_kwargs': {
                        'ignore_https_errors': True,
                    },
                    'playwright_page_goto_kwargs': {
                        'wait_until': 'load',
                        'timeout': 1000 * 60 * 30,
                    },
                    "playwright_page_methods": [
                        PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 30),
                        PageMethod("set_default_timeout", timeout=1000 * 60 * 30),
                    ],
                    'playwright_include_page': True,
                }, 
                callback = self.music_parse,
                errback = self.err_music_callback,
                dont_filter = True,
            )
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                self.logger.info('{}频道回到旧版页面'.format(self.channel_name))
                
            for area_grid in await page.locator("//div[@class='bili-grid']").all():
                await area_grid.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
                
            # 开始解析
            pass
            bili_area_grid_list = await page.locator("//div[@class='bili-grid']").all()
            
            for area_grid in bili_area_grid_list:
                
                area_grid_selector = Selector(text = await area_grid.inner_html())
                        
                area_name = await area_grid.locator("//div[@class='video-card-list']/div[@class='area-header']/div[@class='left']").inner_text()
                if area_name == '前方高能':
                    continue
                
                self.logger.info(area_name)
                rank_item_list = await area_grid.locator("//aside/descendant::li[@class='bili-rank-list-video__item']/div[@class='bili-rank-list-video__item--wrap']").all()
                for rank_item in rank_item_list:
                    rank_item_selector = Selector(text = await rank_item.inner_html())
                    rank_item_order = rank_item_selector.xpath("//span[@class='bili-rank-list-video__item--index']/text()").extract_first()
                    
                    rank_item_card = await rank_item.locator("//a[contains(@class, 'rank-video-card')]").inner_html()
                    rank_item_card_selector = Selector(text = rank_item_card)
                    rank_item_title = rank_item_card_selector.xpath("//div[contains(@class, 'rank-video-card__info')]/h3[@class='rank-video-card__info--tit']/text()").extract_first()
                    self.logger.info('小类区 {}'.format(area_name))
                    self.logger.info('排行 {} 标题 {}'.format(rank_item_order, rank_item_title))
        
                    await rank_item.hover()
                    await page.wait_for_timeout(800)
                    
                    rank_video_card = await page.locator("//div[@class='rank-video-card__popover']").inner_html()
                    rank_video_card_selector = Selector(text = rank_video_card)
                    rank_video_card_title = rank_video_card_selector.xpath("//h3[@class='rank-video-card__popover--tit']/text()").extract_first()
                    rank_video_card_up_name = rank_video_card_selector.xpath("//p[@class='rank-video-card__popover--author']/span[position()=1]/text()").extract_first()
                    rank_video_card_pubdate = rank_video_card_selector.xpath("//p[@class='rank-video-card__popover--author']/span[position()=2]/text()").extract_first()
                    
                    rank_video_card_play = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=1]/span/text()").extract_first()
                    rank_video_card_danmu = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=2]/span/text()").extract_first()
                    rank_video_card_star = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=3]/span/text()").extract_first()
                    rank_video_card_coin = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=4]/span/text()").extract_first()
                    
                    self.logger.info('播放量 : {}'.format(rank_video_card_play))
                    self.logger.info('弹幕量 : {}'.format(rank_video_card_danmu))
                    self.logger.info('收藏量 : {}'.format(rank_video_card_star))
                    self.logger.info('投币量 : {}'.format(rank_video_card_coin))

        await page.close()
        await page.context.close()
    
    async def err_music_callback(self, failure):
        self.logger.info('err_music_callback')
        self.logger.info(repr(failure))
        page = failure.request.meta["playwright_page"]
        self.logger.error('页面加载出错 url {}'.format(failure.request.url))
        await page.close()
        await page.context.close()
    
    async def err_video_callback(self, failure):
        self.logger.info('err_video_callback')
        self.logger.info(repr(failure))
        page = failure.request.meta["playwright_page"]
        self.logger.error('页面加载出错 url {}'.format(failure.request.url))
        await page.close()
        await page.context.close()
    
    async def err_up_callback(self, failure):
        self.logger.info('err_up_callback')
        self.logger.info(repr(failure))
        page = failure.request.meta["playwright_page"]
        self.logger.error('页面加载出错 url {}'.format(failure.request.url))
        await page.close()
        await page.context.close()
