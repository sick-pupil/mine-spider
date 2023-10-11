# -*- coding: utf-8 -*-
"""
Created on Mon Oct  9 19:22:41 2023

@author: Administrator
"""

from ..items import BilibiliRankItem, BilibiliVideoDetail, BilibiliVideoDanmu, BilibiliVideoReply, BilibiliUpInfo
from scrapy import Spider, Selector, Request
from datetime import datetime
from playwright.sync_api import TimeoutError
from playwright._impl._api_types import Error
from scrapy_playwright.page import PageMethod


class BilibiliDougaSpider(Spider):
    
    name: str = 'bilibili-douga'
    allowed_domains: list = ['bilibili.com']
    start_url: str = 'https://www.bilibili.com/v/douga'
    custom_settings: dict = {
        'PLAYWRIGHT_BROWSER_TYPE': 'firefox',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 1000 * 60 * 60 * 3}, 
        'PLAYWRIGHT_MAX_CONTEXTS': 1,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 1000 * 60 * 60 * 3,
        'PLAYWRIGHT_MAX_PAGES_PER_CONTEXT': 1,
        'ITEM_PIPELINES': {
            'mine_spider.pipelines.BilibiliDougaPipeline': 500
        },
        'DOWNLOADER_MIDDLEWARES': {
            'mine_spider.middlewares.MineSpiderDownloaderMiddleware': 900,
        }
    }

    def __init__(self):
        self.channel_name : str = 'douga'
        self.url_prefix : str = 'https:'
        self.result_by_dict : dict = dict()
    
    def start_requests(self):
        yield Request(url = self.start_url,
            meta = {
                'playwright': True, 
                'playwright_context': 'bilibili-douga', 
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'load',
                    'timeout': 1000 * 60 * 15,
                },
                "playwright_page_methods": [
                    PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 15),
                    PageMethod("set_default_timeout", timeout=1000 * 60 * 15),
                ],
                'playwright_include_page': True,
            }, 
            callback = self.douga_parse,
            errback = self.err_douga_callback,
            dont_filter = True,
        )
    
    # 番剧
    async def douga_parse(self, response):        
                
        self.logger.info('请求频道{}， ua为{}'.format(self.channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()
        selector = Selector(text = resp)
        
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            self.logger.info('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(self.channel_name, response.request.headers['user-agent']))
            yield Request(url = self.start_url,
                meta = {
                    'playwright': True, 
                    'playwright_context': 'bilibili-douga', 
                    'playwright_context_kwargs': {
                        'ignore_https_errors': True,
                    },
                    'playwright_page_goto_kwargs': {
                        'wait_until': 'load',
                        'timeout': 1000 * 60 * 15,
                    },
                    "playwright_page_methods": [
                        PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 15),
                        PageMethod("set_default_timeout", timeout=1000 * 60 * 15),
                    ],
                    'playwright_include_page': True,
                }, 
                callback = self.douga_parse,
                errback = self.err_douga_callback,
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
            try:
                await page.wait_for_load_state(state='networkidle', timeout=1000 * 30)
            except (TimeoutError, Error):
                self.logger.info('wait for bili-grid load to networkidle state timeout')
            
            #await page.screenshot(path='/screenshot_{}_{}.png'.format('douga', datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
            #with open(file='/screenshot_{}_{}.html'.format('douga', datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
            #    f.write(await page.content())
            
            # 开始解析
            pass
            bili_area_grid_list = await page.locator("//div[@class='bili-grid']").all()
            
            for area_grid in bili_area_grid_list:
                
                area_grid_selector = Selector(text = await area_grid.inner_html())
                        
                area_name = await area_grid.locator("//div[@class='area-header']/div[@class='left']/descendant::span").inner_text()
                if area_name == '前方高能':
                    continue
                
                rank_item_list = await area_grid.locator("//aside/descendant::li[@class='bili-rank-list-video__item']/div[@class='bili-rank-list-video__item--wrap']").all()
                for rank_item_index in range(len(rank_item_list)):
                    if rank_item_index < 9:
                        
                        rank_item = rank_item_list[rank_item_index]
                        rank_item_selector = Selector(text = await rank_item.inner_html())
                        rank_item_order = rank_item_selector.xpath("//span[@class='bili-rank-list-video__item--index']/text()").extract_first()
                        rank_item_link = rank_item_selector.xpath("//a[contains(@class, 'rank-video-card')]/@href").extract_first()
                        rank_item_bv = rank_item_link.split('/')[4]
                        
                        rank_item_card = await rank_item.locator("//a[contains(@class, 'rank-video-card')]").inner_html()
                        rank_item_card_selector = Selector(text = rank_item_card)
                        rank_item_title = rank_item_card_selector.xpath("//div[contains(@class, 'rank-video-card__info')]/h3[@class='rank-video-card__info--tit']/text()").extract_first()
                        
                        #self.logger.info('小类区 {}'.format(area_name))
                        #self.logger.info('排行 {} 标题 {}'.format(rank_item_order, rank_item_title))
                        #self.logger.info('链接 {}'.format(rank_item_link))
            
                        await rank_item.hover()
                        await page.wait_for_timeout(800)
                        
                        rank_video_card = await page.locator("//div[@class='rank-video-card__popover']").inner_html()
                        rank_video_card_selector = Selector(text = rank_video_card)
                        rank_video_card_title = rank_video_card_selector.xpath("//h3[@class='rank-video-card__popover--tit']/text()").extract_first()
                        rank_video_card_up_name = rank_video_card_selector.xpath("//p[@class='rank-video-card__popover--author']/span[position()=1]/text()").extract_first()
                        rank_video_card_pubdate = rank_video_card_selector.xpath("//p[@class='rank-video-card__popover--author']/span[position()=2]/text()").extract_first().strip('· ')
                        
                        rank_video_card_play = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=1]/span/text()").extract_first()
                        rank_video_card_danmu = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=2]/span/text()").extract_first()
                        rank_video_card_star = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=3]/span/text()").extract_first()
                        rank_video_card_coin = rank_video_card_selector.xpath("//li[@class='rank-video-card__popover--stats__item' and position()=4]/span/text()").extract_first()
                        
                        #self.logger.info('up名称 : {}'.format(rank_video_card_up_name))
                        #self.logger.info('发布时间 : {}'.format(rank_video_card_pubdate))
                        #self.logger.info('播放量 : {}'.format(rank_video_card_play))
                        #self.logger.info('弹幕量 : {}'.format(rank_video_card_danmu))
                        #self.logger.info('收藏量 : {}'.format(rank_video_card_star))
                        #self.logger.info('投币量 : {}'.format(rank_video_card_coin))
                        
                        dougaRankItem = BilibiliRankItem()
                        dougaRankItem['channel_name'] = self.channel_name
                        dougaRankItem['rank_item_bv'] = rank_item_bv
                        dougaRankItem['block_name'] = area_name
                        dougaRankItem['rank_item_num'] = rank_item_order
                        dougaRankItem['rank_item_href'] = rank_item_link
                        dougaRankItem['rank_item_detail_title'] = rank_item_title
                        dougaRankItem['rank_item_pubman'] = rank_video_card_up_name
                        dougaRankItem['rank_item_time'] = rank_video_card_pubdate
                        dougaRankItem['rank_item_play'] = rank_video_card_play
                        dougaRankItem['rank_item_danmu'] = rank_video_card_danmu
                        dougaRankItem['rank_item_star'] = rank_video_card_star
                        dougaRankItem['rank_item_coin'] = rank_video_card_coin
                        
                        self.result_by_dict[dougaRankItem['rank_item_bv']] = dougaRankItem
        
        # 产生日志
        for bv, dougaRankItem in self.result_by_dict.items():
            pass
            block_name = dougaRankItem['block_name']
            rank_item_href = dougaRankItem['rank_item_href']
            rank_item_bv = dougaRankItem['rank_item_bv']
            
            if 'BV1iC4y1L7yS' in rank_item_bv:
                yield Request(url = self.url_prefix + rank_item_href,
                    meta = {
                        'playwright': True, 
                        'playwright_context': 'bilibili-douga-video-{}'.format(rank_item_bv), 
                        'playwright_context_kwargs': {
                            'ignore_https_errors': True,
                        },
                        'playwright_page_goto_kwargs': {
                            'wait_until': 'load',
                            'timeout': 1000 * 60 * 10,
                        },
                        "playwright_page_methods": [
                            PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 10),
                            PageMethod("set_default_timeout", timeout=1000 * 60 * 10),
                        ],
                        'playwright_include_page': True,
                    }, 
                    callback = self.douga_video_parse,
                    errback = self.err_video_callback,
                    dont_filter = True,
                    cb_kwargs = dict(rank_item_bv=rank_item_bv)
                )
        
        await page.close()
        await page.context.close()
        

    async def douga_video_parse(self, response, rank_item_bv : str):
        
        rank_item_video_detail = BilibiliVideoDetail()
        
        self.logger.info('获取视频详情, 请求频道{}，视频链接{}, ua为{}'.format(self.channel_name, response.request.url, response.request.headers['user-agent']))
        
        page = response.meta['playwright_page']
        
        await page.locator("//div[@class='bpx-player-video-area']").wait_for(timeout=1000 * 30)
        await page.evaluate('''() => {
            let elements = document.querySelectorAll('.bpx-player-video-area');
            elements.forEach(element => element.parentNode.removeChild(element));
        }''')
        
        try:
            await page.wait_for_load_state(state='networkidle', timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for networkidle timeout')
        
        try:
            await page.locator(selector = "//span[@class='next-button']", has = page.locator(selector = "//span[@class='switch-button on']")).wait_for(timeout=1000 * 30)
            await page.locator(selector = "//span[@class='next-button']", has = page.locator(selector = "//span[@class='switch-button on']")).hover(timeout=1000 * 30)
            await page.locator(selector = "//span[@class='next-button']", has = page.locator(selector = "//span[@class='switch-button on']")).click(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for switch-button timeout')
        
        await page.wait_for_timeout(3000)
        
        
        page_url = page.url
        if 'video/BV' not in page_url:
            self.result_by_dict.pop(rank_item_bv)
            await page.close()
            await page.context.close()
            return
        
        #await page.screenshot(path='/screenshot_{}_{}.png'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
        #with open(file='/screenshot_{}_{}.html'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
        #    f.write(await page.content())
        
        await page.locator("//div[@class='bui-collapse-header']").wait_for(timeout=1000 * 30)
        await page.locator("//div[@class='bui-collapse-header']").hover()
        await page.locator("//div[@class='bui-collapse-header']").click()
        await page.locator("//div[@class='bui-collapse-body']").wait_for(timeout=1000 * 30)
                
        try:
            await page.locator("//div[@class='dm-info-row ']").first.wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for dm-info-row  timeout')
        
        #await page.screenshot(path='/screenshot_{}_{}.png'.format(response.request.url.split('/')[4], datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
        #with open(file='/screenshot_{}_{}.html'.format(response.request.url.split('/')[4], datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
        #    f.write(await page.content())
        
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
            await page.locator("//div[@id='viewbox_report']/h1[@class='video-title']").wait_for(timeout=1000 * 30)
            await page.locator("//div[@class='video-info-detail-list']").wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for bui-long-list-item and video-info-detail-list timeout')
        
        try:
            await page.locator("//div[@class='video-info-detail-list']/descendant::span[@class='view item']").wait_for(timeout=1000 * 30)
            await page.locator("//div[@class='video-info-detail-list']/descendant::span[@class='dm item']").wait_for(timeout=1000 * 30)
            await page.locator("//div[@class='video-info-detail-list']/descendant::span[@class='pubdate-text']").wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for view item dm pubdate-text item timeout')
        
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
            await page.locator("//div[contains(@class, 'up-info-container')]").wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for up-info-container timeout')
        
        try:
            await page.locator("//div[contains(@class, 'members-info-container')]/descendant::div[@class='staff-info']/a").wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for members-info-container timeout')
        
        resp = await page.content()
        selector = Selector(text = resp)
        if await page.locator("//div[contains(@class, 'up-info-container')]").count() != 0:
            up_info = selector.xpath("//div[contains(@class, 'up-info-container')]")
            if up_info is not None:
                # up主个人空间链接
                rank_item_video_detail['video_detail_up_link'] = up_info.xpath("//div[@class='up-info--left']/descendant::a[@class='up-avatar']/@href").extract_first()
                # up主名称
                rank_item_video_detail['video_detail_up_name'] = up_info.xpath("//div[@class='up-info--right']/descendant::a[contains(@class, 'up-name')]/text()").extract_first().strip()
                # up主简介
                rank_item_video_detail['video_detail_up_desc'] = up_info.xpath("//div[@class='up-info--right']/descendant::div[@class='up-description up-detail-bottom']/text()").extract_first()
                # up主被关注数量
                rank_item_video_detail['video_detail_up_gz'] = up_info.xpath("//div[@class='up-info--right']/descendant::span[@class='follow-btn-inner']/text()").extract_first().strip()
        
        if await page.locator("//div[contains(@class, 'members-info-container')]/descendant::div[@class='staff-info']/a").count() != 0:
            up_info = selector.xpath("//div[contains(@class, 'members-info-container')]/div[@class='membersinfo-normal']/div[@class='container']/div[@class='membersinfo-upcard-wrap' and position()=1]")
            self.logger.info(up_info.get())
            if up_info is not None:
                # up主个人空间链接
                rank_item_video_detail['video_detail_up_link'] = up_info.xpath("//div[@class='staff-info']/a[contains(@class, 'staff-name')]/@href").extract_first()
                # up主名称
                rank_item_video_detail['video_detail_up_name'] = up_info.xpath("//div[@class='staff-info']/a[contains(@class, 'staff-name')]/text()").extract_first().strip()
                # up主简介
                rank_item_video_detail['video_detail_up_desc'] = ''
                # up主被关注数量
                rank_item_video_detail['video_detail_up_gz'] = ''
        
        
        try:
            await page.locator("//div[@class='video-toolbar-left']").wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for video-toolbar-left timeout')
        
        try:
            await page.locator("//span[@class='video-like-info video-toolbar-item-text']").wait_for(timeout=1000 * 30)
            await page.locator("//span[@class='video-coin-info video-toolbar-item-text']").wait_for(timeout=1000 * 30)
            await page.locator("//span[@class='video-fav-info video-toolbar-item-text']").wait_for(timeout=1000 * 30)
            await page.locator("//span[contains(@class, 'video-share-info-text') or contains(@class, 'video-share-info')]").wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for video-like-info video-coin-info video-fav-info video-share-info timeout')

        
        resp = await page.content()
        selector = Selector(text = resp)
        video_toolbar = selector.xpath("//div[@class='video-toolbar-left']")
        # 视频点赞
        rank_item_video_detail['video_detail_like'] = video_toolbar.xpath("//span[@class='video-like-info video-toolbar-item-text']/text()").extract_first()
        # 视频投币
        rank_item_video_detail['video_detail_coin'] = video_toolbar.xpath("//span[@class='video-coin-info video-toolbar-item-text']/text()").extract_first()
        # 视频收藏
        rank_item_video_detail['video_detail_star'] = video_toolbar.xpath("//span[@class='video-fav-info video-toolbar-item-text']/text()").extract_first()
        # 视频转发
        rank_item_video_detail['video_detail_share'] = video_toolbar.xpath("//span[contains(@class, 'video-share-info-text') or contains(@class, 'video-share-info')]/text()").extract_first()
        
        
        #await page.evaluate("""
        #    window.scrollTo({
        #        top: document.body.scrollHeight,
        #        behavior: 'smooth',
        #    });
        #    """
        #)
        await page.evaluate('''async (delay) => {
            const scrollHeight = document.body.scrollHeight;
            const scrollStep = scrollHeight / 100;
            for(let i = 0; i < 100; i++) {
                window.scrollBy(0, scrollStep);
                await new Promise(resolve => setTimeout(resolve, delay));
            }
        }''', 100)
        try:
            await page.wait_for_load_state(state='networkidle', timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for networkidle timeout')
        await page.wait_for_timeout(3000)
        
        
        try:
            await page.locator("//div[@class='left-container-under-player']").wait_for(timeout=1000 * 30)
        except (TimeoutError, Error):
            self.logger.info('wait for left-container-under-player timeout')
            
        try:
            await page.locator("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link topic-link']/span[@class='tag-txt']").wait_for(timeout=1000 * 10)
            await page.locator("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link newchannel-link van-popover__reference']").wait_for(timeout=1000 * 10)
            await page.locator("//div[@class='tag not-btn-tag']/descendant::a[@class='tag-link']").wait_for(timeout=1000 * 10)
        except (TimeoutError, Error):
            self.logger.info('wait for tag not-btn-tag timeout')
        
        resp = await page.content()
        selector = Selector(text = resp)
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
        
        
        try:
            await page.locator(selector = "//div[@class='reply-header']").wait_for(timeout=1000 * 30)
            await page.locator(selector = "//div[@class='reply-warp']").wait_for(timeout=1000 * 30)
            await page.locator(selector = "//div[@class='reply-list']/descendant::div[@class='root-reply']").first.wait_for(timeout=1000 * 30)
            await page.locator(selector = "//span[@class='total-reply']").wait_for(timeout=1000 * 30)
            
            #await page.screenshot(path='/screenshot_{}_{}.png'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
            #with open(file='/screenshot_{}_{}.html'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
            #    f.write(await page.content())
            tmp_total_reply = await page.locator("//span[@class='total-reply']").inner_text()
            while tmp_total_reply is None or tmp_total_reply == '0' or tmp_total_reply == '':
                self.logger.info('waiting for span total-reply text')
                tmp_total_reply = await page.locator("//span[@class='total-reply']").inner_text()
                continue
        except (TimeoutError, Error):
            self.logger.info('waiting for root-reply timeout')
            
        try:
            await page.locator(selector = "//div[@class='comment-header clearfix']").wait_for(timeout=1000 * 30)
            await page.locator(selector = "//div[@class='comment-list ']").wait_for(timeout=1000 * 30)
            await page.locator(selector = "//div[@class='comment-list ']/descendant::div[contains(@class, 'list-item reply-wrap ')]").first.wait_for(timeout=1000 * 30)
            await page.locator(selector = "//li[@class='total-reply']").wait_for(timeout=1000 * 30)
            
            #await page.screenshot(path='/screenshot_{}_{}.png'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
            #with open(file='/screenshot_{}_{}.html'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
            #    f.write(await page.content())
            tmp_total_reply = await page.locator("//li[@class='total-reply']").inner_text()
            while tmp_total_reply is None or tmp_total_reply == '':
                self.logger.info('waiting for li total-reply text')
                tmp_total_reply = await page.locator("//li[@class='total-reply']").inner_text()
                continue
        except (TimeoutError, Error):
            self.logger.info('waiting for list-item reply-wrap timeout')
        
        await page.wait_for_timeout(1000)
        resp = await page.content()
        selector = Selector(text = resp)
        
        total_reply : str = ''
        rank_item_video_reply_list : list = []
        if selector.xpath(query = "//div[@class='reply-list']/descendant::div[@class='root-reply']") is not None:
            for list_item_selector in selector.xpath(query = "//div[@class='reply-list']/descendant::div[@class='root-reply']"):
                reply_item = BilibiliVideoReply()
                # 评论内容
                #reply_item['video_reply_context'] = list_item_selector.xpath(".//span[@class='reply-content']/text()").extract_first()
                reply_item['video_reply_context'] = list_item_selector.xpath("string(.//span[@class='reply-content'])").extract_first()
                # 评论时间
                reply_item['video_reply_time'] = list_item_selector.xpath(".//span[@class='reply-time']/text()").extract_first()
                # 评论被点赞数
                reply_item['video_reply_like'] = list_item_selector.xpath(".//span[@class='reply-like']/span/text()").extract_first()
                rank_item_video_reply_list.append(reply_item)
            total_reply = selector.xpath(query = "//span[@class='total-reply']/text()").extract_first()
                
        elif selector.xpath(query = "//div[@class='comment-list ']/descendant::div[@class='con ']") is not None:
            for list_item_selector in selector.xpath(query = "//div[@class='comment-list ']/descendant::div[@class='con ']"):
                reply_item = BilibiliVideoReply()
                # 评论内容
                #reply_item['video_reply_context'] = list_item_selector.xpath(".//p[@class='text']/text()").extract_first()
                reply_item['video_reply_context'] = list_item_selector.xpath("string(.//p[@class='text'])").extract_first()
                # 评论时间
                reply_item['video_reply_time'] = list_item_selector.xpath(".//div[@class='info']/span[@class='time-location']/span[@class='reply-time']/text()").extract_first()
                # 评论被点赞数
                reply_item['video_reply_like'] = list_item_selector.xpath(".//div[@class='info']/span[@class='like ']/span/text()").extract_first()
                rank_item_video_reply_list.append(reply_item)
            total_reply = selector.xpath(query = "//li[@class='total-reply']/text()").extract_first()
        
        if total_reply is not None and total_reply != '':
            rank_item_video_detail['video_detail_reply'] = total_reply
        else:
            rank_item_video_detail['video_detail_reply'] = ''
        
        rank_item_video_detail['video_detail_hot_replys'] = rank_item_video_reply_list
        
        dougaRankItem = self.result_by_dict[rank_item_bv]
        dougaRankItem['rank_item_video_detail'] = rank_item_video_detail
        
        #if dougaRankItem['rank_item_video_detail']['video_detail_reply'] is None or dougaRankItem['rank_item_video_detail']['video_detail_reply'] == '':
        #    await page.screenshot(path='/screenshot_{}_{}.png'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
        #    with open(file='/screenshot_{}_{}.html'.format(rank_item_bv, datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
        #        f.write(await page.content())
                
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
                'playwright_context': 'video-up-{}-{}-{}'.format(rank_item_bv, up_link_id, datetime.now().strftime("%Y%m%d%H%M%S")), 
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                 },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'networkidle',
                    'timeout': 1000 * 60 * 10,
                },
                "playwright_page_methods": [
                    PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 10),
                    PageMethod("set_default_timeout", timeout=1000 * 60 * 10),
                ],
                'playwright_include_page': True,
            }, 
            callback = self.up_info_parse,
            errback = self.err_up_callback,
            dont_filter = True,
            cb_kwargs = dict(video_bv=rank_item_bv, up_link_id=up_link_id)
        )
        
        #await page.screenshot(path='/screenshot_{}_{}.png'.format(response.request.url.split('/')[4], datetime.now().strftime("%Y%m%d%H%M%S")), full_page=True)
        #with open(file='/screenshot_{}_{}.html'.format(response.request.url.split('/')[4], datetime.now().strftime("%Y%m%d%H%M%S")), mode='w', encoding='utf-8') as f:
        #    f.write(await page.content())
        
        await page.close()
        await page.context.close()
        
        
    async def up_info_parse(self, response, video_bv : str, up_link_id : str):
        
        self.logger.info('获取视频发布人详情, 请求频道{}，up个人空间链接{}, ua为{}'.format(self.channel_name, response.request.url, response.request.headers['user-agent']))
        page = response.meta['playwright_page']
        
        await page.wait_for_load_state('networkidle')
        
        bilibiliUpInfo = BilibiliUpInfo()
        try:
            await page.locator("//div[@class='geetest_panel geetest_wind']").wait_for(timeout=1000 * 10)
        except (TimeoutError, Error):
            self.logger.info('wait for geetest_panel geetest_wind timeout')
        await page.evaluate('''() => {
            let elements = document.querySelectorAll('.geetest_panel.geetest_wind');
            elements.forEach(element => element.parentNode.removeChild(element));
        }''')
                
        try:
            await page.locator("//div[@class='h-basic']").wait_for(timeout=1000 * 10)
        except (TimeoutError, Error):
            self.logger.info('wait for h-basic timeout')
        
        if await page.locator("//div[@class='h-basic']").count() != 0:
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
            # up关注数
            bilibiliUpInfo['up_gz'] = up_gz_fs_selector.xpath("//div[@class='n-data n-gz']/p[@id='n-gz']/text()").extract_first().strip()
            # up粉丝数
            bilibiliUpInfo['up_fs'] = up_gz_fs_selector.xpath("//div[@class='n-data n-fs']/p[@id='n-fs']/text()").extract_first().strip()
            
            up_uid = await page.locator("//div[@class='info-wrap']/span[@class='info-value']").first.inner_text()
            # up UID
            bilibiliUpInfo['up_uid'] = up_uid
            
            dougaRankItem = self.result_by_dict[video_bv]
            dougaRankItem['rank_item_up_detail'] = bilibiliUpInfo
            
            await page.close()
            await page.context.close()
            
            yield dougaRankItem
        else:
            yield Request(url = response.request.url,
                meta = {
                    'playwright': True, 
                    'playwright_context': 'video-up-{}-{}-{}'.format(video_bv, up_link_id, datetime.now().strftime("%Y%m%d%H%M%S")), 
                    'playwright_context_kwargs': {
                        'ignore_https_errors': True,
                     },
                    'playwright_page_goto_kwargs': {
                        'wait_until': 'networkidle',
                        'timeout': 1000 * 60 * 10,
                    },
                    "playwright_page_methods": [
                        PageMethod("set_default_navigation_timeout", timeout=1000 * 60 * 10),
                        PageMethod("set_default_timeout", timeout=1000 * 60 * 10),
                    ],
                    'playwright_include_page': True,
                }, 
                callback = self.up_info_parse,
                errback = self.err_up_callback,
                dont_filter = True,
                cb_kwargs = dict(video_bv=video_bv, up_link_id=up_link_id)
            )
            await page.close()
            await page.context.close()

    
    async def err_douga_callback(self, failure):
        self.logger.info('err_douga_callback')
        self.logger.info(repr(failure))
        page = failure.request.meta["playwright_page"]
        self.logger.info('页面加载出错 url {}'.format(failure.request.url))
        await page.close()
        await page.context.close()
    
    async def err_video_callback(self, failure):
        self.logger.info('err_video_callback')
        self.logger.info(repr(failure))
        page = failure.request.meta["playwright_page"]
        self.logger.info('页面加载出错 url {}'.format(failure.request.url))
        await page.close()
        await page.context.close()
    
    async def err_up_callback(self, failure):
        self.logger.info('err_up_callback')
        self.logger.info(repr(failure))
        page = failure.request.meta["playwright_page"]
        self.logger.info('页面加载出错 url {}'.format(failure.request.url))
        await page.close()
        await page.context.close()