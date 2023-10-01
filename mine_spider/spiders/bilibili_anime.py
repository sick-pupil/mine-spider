# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 19:39:43 2023

@author: Administrator
"""

from ..items import BilibiliRankItem, BilibiliVideoDetail, BilibiliVideoDanmu, BilibiliVideoReply, BilibiliUpInfo
from scrapy import Spider, Selector, Request
# from datetime import datetime
from playwright.sync_api import TimeoutError
from playwright._impl._api_types import Error
from scrapy_playwright.page import PageMethod


class BilibiliAnimeSpider(Spider):
    
    name: str = 'bilibili-anime'
    allowed_domains: list = ['bilibili.com']
    start_url: str = 'https://www.bilibili.com/anime'
    custom_settings: dict = {
        'PLAYWRIGHT_BROWSER_TYPE': 'firefox',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 1000 * 60 * 30}, 
        'PLAYWRIGHT_MAX_CONTEXTS': 1,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 1000 * 60 * 30,
        'PLAYWRIGHT_MAX_PAGES_PER_CONTEXT': 50,
        'ITEM_PIPELINES': {
            'mine_spider.pipelines.BilibiliAnimePipeline': 500
        },
        'DOWNLOADER_MIDDLEWARES': {
            'mine_spider.middlewares.MineSpiderDownloaderMiddleware': 900,
        }
    }

    def __init__(self):
        self.channel_name : str = '番剧'        
        self.url_prefix : str = 'https:'
        self.result_by_dict : dict = dict()
    
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
                    'timeout': 1000 * 60 * 30,
                },
                "playwright_page_methods": [
                    PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 30),
                    PageMethod("set_default_timeout", timeout=1000 * 60 * 30),
                ],
                'playwright_include_page': True,
            }, 
            callback = self.anime_parse,
            errback = self.err_anime_callback,
            dont_filter = True,
        )
    
    # 番剧
    async def anime_parse(self, response):        
                
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
                self.logger.info('{}频道回到旧版页面'.format(self.channel_name))
                
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
                    animeRankItem['rank_item_bv'] = rank_item_href.split('/')[4]
                    animeRankItem['rank_item_detail_title'] = rank_item_detail_title
                    animeRankItem['rank_item_detail_point'] = rank_item_detail_point
                    
                    if block_name != '完结动画':
                        await rank_item.hover()
                        await page.wait_for_timeout(800)
                        await page.locator("//div[@class='video-info-module']").wait_for()
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
                    
                    self.result_by_dict[animeRankItem['rank_item_bv'], animeRankItem]
                        
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
                    animeRankItem['rank_item_bv'] = rank_item_href.split('/')[4]
                    animeRankItem['rank_item_detail_title'] = rank_item_detail_title
                    animeRankItem['rank_item_detail_point'] = rank_item_detail_point
                    
                    if block_name != '完结动画':
                        await rank_item.hover()
                        await page.wait_for_timeout(800)
                        await page.locator("//div[@class='video-info-module']").wait_for()
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
                
                    self.result_by_dict[animeRankItem['rank_item_bv'], animeRankItem]
        
        # 产生日志
        for bv, animeRankItem in self.result_by_dict.items():
            pass
            block_name = animeRankItem['block_name']
            rank_item_href = animeRankItem['rank_item_href']
            rank_item_bv = animeRankItem['rank_item_bv']
            
            if block_name != '连载动画' and block_name != '完结动画':
                yield Request(url = self.url_prefix + rank_item_href,
                    meta = {
                        'playwright': True, 
                        'playwright_context': 'bilibili-anime-video-{}'.format(rank_item_bv), 
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
                    callback = self.anime_video_parse,
                    errback = self.err_video_callback,
                    dont_filter = True,
                )
            
            #self.logger.info('{}频道{}区域{}热门排行：{}：{}\n{}\n链接: {}\n简介: {}'.format(self.channel_name, 
            #                                         animeRankItem['block_name'], 
            #                                         animeRankItem['block_hot_time_range'],
            #                                         animeRankItem['rank_item_num'], 
            #                                         animeRankItem['rank_item_detail_title'], 
            #                                         animeRankItem['rank_item_detail_point'], 
            #                                         animeRankItem['rank_item_href'],
            #                                         animeRankItem['rank_item_desc']))
            #self.logger.info('发布时间{} 播放次数{} 弹幕量{} 收藏数{} 硬币数{} 发布人: {}\n'.format(animeRankItem['rank_item_time'], 
            #                                         animeRankItem['rank_item_play'], 
            #                                         animeRankItem['rank_item_danmu'],
            #                                         animeRankItem['rank_item_star'], 
            #                                         animeRankItem['rank_item_coin'], 
            #                                         animeRankItem['rank_item_pubman']))

        await page.close()
        await page.context.close()
        
        
    
    async def anime_video_parse(self, response):
        
        rank_item_bv = response.request.url.split('/')[4]
        
        rank_item_video_detail = BilibiliVideoDetail()
        
        self.logger.info('获取视频详情, 请求频道{}，视频链接{}, ua为{}'.format(self.channel_name, response.request.url, response.request.headers['user-agent']))
        
        page = response.meta['playwright_page']
        
        await page.locator("//div[@class='bpx-player-video-area']").wait_for(timeout=1000 * 60)
        await page.evaluate('''() => {
            let elements = document.querySelectorAll('.bpx-player-video-area');
            elements.forEach(element => element.parentNode.removeChild(element));
        }''')
        
        await page.locator("//div[@class='bui-collapse-header']").wait_for(timeout=1000 * 60)
        await page.locator("//div[@class='bui-collapse-header']").hover()
        await page.locator("//div[@class='bui-collapse-header']").click()
        await page.locator("//div[@class='bui-collapse-body']").wait_for(timeout=1000 * 60)
        
        try:
            await page.locator("//div[@class='dm-info-row ']").first.wait_for(timeout=1000 * 60)
        except (TimeoutError, Error):
            self.logger.error('wait for dm-info-row  timeout')
        
        resp = await page.content()
        selector = Selector(text = resp)
        
        rank_item_video_danmu_list : list = []
        for dammu_item_selector in selector.xpath("//div[@class='dm-info-row ']"):
            dammu_item = BilibiliVideoDanmu()
            # 弹幕时间
            dammu_item['video_danmu_pubtime_in_video'] = dammu_item_selector.xpath(".//span[@class='dm-info-time']/text()").extract_first()
            # 弹幕内容
            dammu_item['video_danmu_context'] = dammu_item_selector.xpath(".//span[@class='dm-info-dm']/text()").extract_first().strip()
            # 弹幕发送时间
            dammu_item['video_danmu_pubtime'] = dammu_item_selector.xpath(".//span[@class='dm-info-date']/text()").extract_first().strip()
            rank_item_video_danmu_list.append(dammu_item)
        rank_item_video_detail['video_detail_danmus'] = rank_item_video_danmu_list
        
        try:
            await page.locator("//div[@id='viewbox_report']/h1[@class='video-title']").wait_for(timeout=1000 * 60)
            await page.locator("//div[@class='video-info-detail-list']").wait_for(timeout=1000 * 60)
        except (TimeoutError, Error):
            self.logger.error('wait for bui-long-list-item and video-info-detail-list timeout')
        
        video_info_detail = selector.xpath("//div[@class='video-info-detail-list']")
        # 视频标题
        rank_item_video_detail['video_detail_title'] = selector.xpath("//div[@id='viewbox_report']/h1[@class='video-title']/text()").extract_first()
        # 视频播放量
        rank_item_video_detail['video_detail_play'] = video_info_detail.xpath("//span[@class='view item']/text()").extract_first().strip()
        # 视频弹幕量
        rank_item_video_detail['video_detail_danmu'] = video_info_detail.xpath("//span[@class='dm item']/text()").extract_first().strip()
        # 视频发布时间
        rank_item_video_detail['video_detail_pubtime'] = video_info_detail.xpath("//span[@class='pubdate-text']/text()").extract_first().strip()
        
        try:
            await page.locator("//div[contains(@class, 'up-info-container')]").wait_for(timeout=1000 * 60)
        except (TimeoutError, Error):
            self.logger.error('wait for up-info-container timeout')
        
        up_info = selector.xpath("//div[contains(@class, 'up-info-container')]")        
        # up主个人空间链接
        rank_item_video_detail['video_detail_up_link'] = up_info.xpath("//div[@class='up-info--left']/descendant::a[@class='up-avatar']/@href").extract_first()
        # up主名称
        rank_item_video_detail['video_detail_up_name'] = up_info.xpath("//div[@class='up-info--right']/descendant::a[contains(@class, 'up-name')]/text()").extract_first().strip()
        # up主简介
        rank_item_video_detail['video_detail_up_desc'] = up_info.xpath("//div[@class='up-info--right']/descendant::div[@class='up-description up-detail-bottom']/text()").extract_first()
        # up主被关注数量
        rank_item_video_detail['video_detail_up_gz'] = up_info.xpath("//div[@class='up-info--right']/descendant::span[@class='follow-btn-inner']/text()").extract_first().strip()
        
        try:
            await page.locator("//div[@class='video-toolbar-left']").wait_for(timeout=1000 * 60)
        except (TimeoutError, Error):
            self.logger.error('wait for video-toolbar-left timeout')
        
        video_toolbar = selector.xpath("//div[@class='video-toolbar-left']")
        # 视频点赞
        rank_item_video_detail['video_detail_like'] = video_toolbar.xpath("//span[@class='video-like-info video-toolbar-item-text']/text()").extract_first()
        # 视频投币
        rank_item_video_detail['video_detail_coin'] = video_toolbar.xpath("//span[@class='video-coin-info video-toolbar-item-text']/text()").extract_first()
        # 视频收藏
        rank_item_video_detail['video_detail_star'] = video_toolbar.xpath("//span[@class='video-fav-info video-toolbar-item-text']/text()").extract_first()
        # 视频转发
        rank_item_video_detail['video_detail_share'] = video_toolbar.xpath("//span[contains(@class, 'video-share-info-text') or contains(@class, 'video-share-info')]/text()").extract_first()
        
        
        try:
            await page.locator("//div[@class='left-container-under-player']").wait_for(timeout=1000 * 60)
        except (TimeoutError, Error):
            self.logger.info('wait for left-container-under-player timeout')
        
        under_video_container = selector.xpath("//div[@class='left-container-under-player']")
        rank_item_video_detail['video_detail_desc'] = under_video_container.xpath("//span[@class='desc-info-text']/text()").extract_first()
        rank_item_video_tag_list : list = []
        
        
        for tag_selector in under_video_container.xpath("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link topic-link']/span[@class='tag-txt']/text()"):
            # 视频标签
            rank_item_video_tag_list.append(tag_selector.get().strip())
        for tag_selector in under_video_container.xpath("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link newchannel-link van-popover__reference']/text()"):
            # 视频标签
            rank_item_video_tag_list.append(tag_selector.get().strip())
        for tag_selector in under_video_container.xpath("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link']/text()"):
            # 视频标签
            rank_item_video_tag_list.append(tag_selector.get().strip())
        
        rank_item_video_detail['video_detail_tags'] = rank_item_video_tag_list
        
        await page.evaluate("""
            window.scrollTo({
                top: document.body.scrollHeight,
                behavior: 'smooth',
            });
            """
        )
        await page.wait_for_timeout(3000)
                        
        await page.locator(selector = "//div[@class='rec-footer']").wait_for(timeout=1000 * 60)
        await page.locator(selector = "//div[@class='rec-footer']").scroll_into_view_if_needed()
        
        await page.locator(selector = "//div[@class='left-container-under-player']").wait_for(timeout=1000 * 60)
        await page.locator(selector = "//div[@class='left-container-under-player']").scroll_into_view_if_needed()
        
        try:
            await page.locator(selector = "//div[@class='reply-header']").wait_for(timeout=1000 * 60)
            await page.locator(selector = "//div[@class='reply-warp']").wait_for(timeout=1000 * 60)
            await page.locator(selector = "//div[@class='reply-list']/descendant::div[@class='root-reply']").first.wait_for(timeout=1000 * 60)
        except (TimeoutError, Error):
            self.logger.info('waiting for root-reply timeout')
            
        try:
            await page.locator(selector = "//div[@class='comment-header clearfix']").wait_for(timeout=1000 * 60)
            await page.locator(selector = "//div[@class='comment-list ']").wait_for(timeout=1000 * 60)
            await page.locator(selector = "//div[@class='comment-list ']/descendant::div[contains(@class, 'list-item reply-wrap ')]").first.wait_for(timeout=1000 * 60)
        except (TimeoutError, Error):
            self.logger.info('waiting for list-item reply-wrap timeout')
                
        resp = await page.content()
        selector = Selector(text = resp)
        
        rank_item_video_reply_list : list = []
        if selector.xpath(query = "//div[@class='reply-list']/descendant::div[@class='root-reply']") is not None:
            for list_item_selector in selector.xpath(query = "//div[@class='reply-list']/descendant::div[@class='root-reply']"):
                reply_item = BilibiliVideoReply()
                # 评论内容
                reply_item['video_reply_context'] = list_item_selector.xpath(".//span[@class='reply-content']/text()").extract_first()
                # 评论时间
                reply_item['video_reply_time'] = list_item_selector.xpath(".//span[@class='reply-time']/text()").extract_first()
                # 评论被点赞数
                reply_item['video_reply_like'] = list_item_selector.xpath(".//span[@class='reply-like']/span/text()").extract_first()
                rank_item_video_reply_list.append(reply_item)
                
        elif selector.xpath(query = "//div[@class='comment-list ']/descendant::div[@class='con ']") is not None:
            for list_item_selector in selector.xpath(query = "//div[@class='comment-list ']/descendant::div[@class='con ']"):
                reply_item = BilibiliVideoReply()
                # 评论内容
                reply_item['video_reply_context'] = list_item_selector.xpath(".//p[@class='text']/text()").extract_first()
                # 评论时间
                reply_item['video_reply_time'] = list_item_selector.xpath(".//div[@class='info']/span[@class='time-location']/span[@class='reply-time']/text()").extract_first()
                # 评论被点赞数
                reply_item['video_reply_like'] = list_item_selector.xpath(".//div[@class='info']/span[@class='like ']/span/text()").extract_first()
                rank_item_video_reply_list.append(reply_item)
        rank_item_video_detail['video_detail_hot_replys'] = rank_item_video_reply_list
        rank_item_video_detail['video_detail_reply'] = selector.xpath(query = "//span[@class='total-reply']/text()").extract_first()
        
        animeRankItem = self.result_by_dict[rank_item_bv]
        animeRankItem['rank_item_video_detail'] = rank_item_video_detail
                
        # 输出日志
        #self.logger.info('视频标题 : {}'.format(rank_item_video_detail['video_detail_title']))
        #self.logger.info('视频播放数 : {}'.format(rank_item_video_detail['video_detail_play']))
        #self.logger.info('视频弹幕数 : {}'.format(rank_item_video_detail['video_detail_danmu']))
        #self.logger.info('视频发布具体时间 : {}'.format(rank_item_video_detail['video_detail_pubtime']))
        #self.logger.info('视频点赞数 : {}'.format(rank_item_video_detail['video_detail_like']))
        #self.logger.info('视频投币数 : {}'.format(rank_item_video_detail['video_detail_coin']))
        #self.logger.info('视频收藏数 : {}'.format(rank_item_video_detail['video_detail_star']))
        #self.logger.info('视频转发数 : {}'.format(rank_item_video_detail['video_detail_share']))
        #for tag in rank_item_video_detail['video_detail_tags']:
        #    self.logger.info('视频标签 : {}'.format(tag))
        
        #self.logger.info('视频简介 : {}'.format(rank_item_video_detail['video_detail_desc']))
        #self.logger.info('视频总评论数 : {}'.format(rank_item_video_detail['video_detail_reply']))
        #for reply in rank_item_video_detail['video_detail_hot_replys']:
        #    self.logger.info('评论内容 : {}'.format(reply['video_reply_context']))
        #    self.logger.info('评论时间 : {}'.format(reply['video_reply_time']))
        #    self.logger.info('评论被点赞数 : {}'.format(reply['video_reply_like']))
        
        #for dammu in rank_item_video_detail['video_detail_danmus']:
        #    self.logger.info('弹幕发送的视频时间节点 : {}'.format(dammu['video_danmu_pubtime_in_video']))
        #    self.logger.info('弹幕内容 : {}'.format(dammu['video_danmu_context']))
        #    self.logger.info('弹幕发送时间 : {}'.format(dammu['video_danmu_pubtime']))
        
        #self.logger.info('视频发布人个人空间链接 : {}'.format(rank_item_video_detail['video_detail_up_link']))
        #self.logger.info('视频发布人名称 : {}'.format(rank_item_video_detail['video_detail_up_name']))
        #self.logger.info('视频发布人简介 : {}'.format(rank_item_video_detail['video_detail_up_desc']))
        #self.logger.info('视频发布人被关注数量 : {}'.format(rank_item_video_detail['video_detail_up_gz']))
        
        up_link_id = rank_item_video_detail['video_detail_up_link'].split('/')[3]
        yield Request(url = self.url_prefix + rank_item_video_detail['video_detail_up_link'],
            meta = {
                'playwright': True, 
                'playwright_context': 'video-up-{}-{}'.format(rank_item_bv, up_link_id), 
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                 },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'networkidle',
                    'timeout': 1000 * 60 * 30,
                },
                "playwright_page_methods": [
                    PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 30),
                    PageMethod("set_default_timeout", timeout=1000 * 60 * 30),
                ],
                'playwright_include_page': True,
            }, 
            callback = self.up_info_parse,
            errback = self.err_anime_callback,
            dont_filter = True,
        )
        
        #await page.screenshot(path='/screenshot_{}_{}.png'.format(response.request.url.split('/')[4], datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
        #with open(file='/screenshot_{}_{}.html'.format(response.request.url.split('/')[4], datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
        #    f.write(await page.content())
        
        await page.close()
        await page.context.close()
        
        
    async def up_info_parse(self, response):
        
        self.logger.info('获取视频发布人详情, 请求频道{}，up个人空间链接{}, ua为{}'.format(self.channel_name, response.request.url, response.request.headers['user-agent']))
        page = response.meta['playwright_page']
        
        video_bv = await page.context.name.split('-')[2]
        self.logger.info(video_bv)
        
        bilibiliUpInfo = BilibiliUpInfo()
        bilibiliUpInfo['up_bv'] = video_bv
        
        try:
            await page.locator("//div[@class='geetest_panel geetest_wind']").wait_for(timeout=1000 * 5)
        except (TimeoutError, Error):
            self.logger.info('wait for geetest_panel geetest_wind timeout')
        await page.evaluate('''() => {
            let elements = document.querySelectorAll('.geetest_panel geetest_wind');
            elements.forEach(element => element.parentNode.removeChild(element));
        }''')
        await page.wait_for_load_state('networkidle')
            
        up_basic_info = await page.locator("//div[@class='h-basic']").inner_html()
        up_basic_info_selector = Selector(text = up_basic_info)
        # up名称
        bilibiliUpInfo['up_name'] = up_basic_info_selector.xpath("//span[@id='h-name']/text()").extract_first()
        # up简介
        bilibiliUpInfo['up_desc'] = up_basic_info_selector.xpath("//div[@class='h-basic-spacing']/h4[@class='h-sign']/text()").extract_first().strip()
        
        up_tab_links = await page.locator("//div[@id='navigator']/descendant::div[@class='n-tab-links']").inner_html()
        up_tab_links_selector = Selector(text = up_tab_links)
        # up投稿数
        bilibiliUpInfo['up_tg'] = up_tab_links_selector.xpath("//a[@class='n-btn n-video n-audio n-article n-album']/span[@class='n-num']/text()").extract_first().strip()
        # up合集数
        bilibiliUpInfo['up_hj'] = up_tab_links_selector.xpath("//a[@class='n-btn n-channel']/span[@class='n-num']/text()").extract_first().strip()
        
        up_gz_fs = await page.locator("//div[@id='navigator']/descendant::div[@class='n-statistics']").inner_html()
        up_gz_fs_selector = Selector(text = up_gz_fs)
        # up被关注数
        bilibiliUpInfo['up_gz'] = up_gz_fs_selector.xpath("//div[@class='n-data n-gz']/p[@id='n-gz']/text()").extract_first().strip()
        # up粉丝数
        bilibiliUpInfo['up_fs'] = up_gz_fs_selector.xpath("//div[@class='n-data n-fs']/p[@id='n-fs']/text()").extract_first().strip()
        
        up_uid = await page.locator("//div[@class='info-wrap']/span[@class='info-value']").first.inner_text()
        # up UID
        bilibiliUpInfo['up_uid'] = up_uid
        
        animeRankItem = self.result_by_dict[video_bv]
        animeRankItem['rank_item_up_detail'] = bilibiliUpInfo
        
        await page.close()
        await page.context.close()
        
        yield animeRankItem
        
        self.result_by_dict.pop(video_bv)

    async def err_anime_callback(self, failure):
        self.logger.info('err_anime_callback')
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
    
