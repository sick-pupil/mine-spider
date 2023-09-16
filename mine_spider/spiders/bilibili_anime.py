# -*- coding: utf-8 -*-
"""
Created on Sat Sep 16 19:39:43 2023

@author: Administrator
"""

from ..items import BilibiliItem, BilibiliAnimeItem
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
            'mine_spider.pipelines.BilibiliPipeline': 500
        },
    }

    def __init__(self):
        self.channel_name = '番剧'
    
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
        result = BilibiliItem()
        data_list_result = []
        
        
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
                # 小类区名字
                block_name = await block_item.locator("//h4[@class='name']").inner_text()
                # 小类区热门列表时间范围
                block_hot_time_range = await block_item.locator("//span[@class='selected']").inner_text()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                for rank_item in rank_item_list_in_block:
                    # 热门项序号
                    rank_num = await rank_item.locator("//i[@class='ri-num']").inner_text()
                    rank_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
                    # 热门视频链接
                    rank_href = await rank_base_info.get_attribute('href')
                    rank_detail = rank_base_info.locator("//div[@class='ri-detail']")
                    # 热门视频标题
                    rank_detail_title = await rank_detail.locator("//p[@class='ri-title']").inner_text()
                    # 热门视频综合评分
                    rank_detail_point = await rank_detail.locator("//p[@class='ri-point']").inner_text()
                    
                    logging.info('小类区名称：{} 小类区热门时间范围：{} 热门排行：{} 热门项链接：{} 热门项标题：{} 热门项评分：{}'.format(block_name, block_hot_time_range, rank_num, rank_href, rank_detail_title, rank_detail_point))
                    
                    if block_name != '完结动画':
                        await rank_item.hover()
                        await page.wait_for_timeout(500)
                        video_info = page.locator("//div[@class='video-info-module']")
                        # 视频发布时间
                        time = await video_info.locator("//span[@class='time']").inner_text()
                        # 视频播放次数
                        play = await video_info.locator("//span[@class='play']").inner_text()
                        # 视频弹幕数量
                        danmu = await video_info.locator("//span[@class='danmu']").inner_text()
                        # 视频收藏数量
                        star = await video_info.locator("//span[@class='star']").inner_text()
                        # 视频硬币数量
                        coin = await video_info.locator("//span[@class='coin']").inner_text()
                        
                        # 装载item
                        animeItem = BilibiliAnimeItem()
                        animeItem['block_name'] = block_name
                        animeItem['rank_hot_time_range'] = block_hot_time_range
                        animeItem['rank_item_num'] = rank_num
                        animeItem['rank_item_href'] = rank_href
                        animeItem['rank_item_detail_title'] = rank_detail_title
                        animeItem['rank_item_detail_point'] = rank_detail_point
                        animeItem['rank_item_time'] = time
                        animeItem['rank_item_play'] = play
                        animeItem['rank_item_danmu'] = danmu
                        animeItem['rank_item_star'] = star
                        animeItem['rank_item_coin'] = coin

                        data_list_result.append(animeItem)
                                                
                        logging.info('视频发布时间：{} 视频播放次数：{} 视频弹幕数量：{} 视频收藏数量：{} 视频硬币数量：{}'.format(time, play, danmu, star, coin))
                
                # 点击热门时间范围
                await block_item.locator("//div[@class='pgc-rank-dropdown rank-dropdown']").hover()
                await block_item.locator("//li[@class='dropdown-item' and contains(text(), '一周')]").click()
                await page.wait_for_load_state("networkidle")
                
                # 小类区热门列表时间范围
                block_hot_time_range = await block_item.locator("//span[@class='selected']").inner_text()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                for rank_item in rank_item_list_in_block:
                    # 热门项序号
                    rank_num = await rank_item.locator("//i[@class='ri-num']").inner_text()
                    rank_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
                    # 热门视频链接
                    rank_href = await rank_base_info.get_attribute('href')
                    rank_detail = rank_base_info.locator("//div[@class='ri-detail']")
                    # 热门视频标题
                    rank_detail_title = await rank_detail.locator("//p[@class='ri-title']").inner_text()
                    # 热门视频综合评分
                    rank_detail_point = await rank_detail.locator("//p[@class='ri-point']").inner_text()
                    
                    logging.info('小类区名称：{} 小类区热门时间范围：{} 热门排行：{} 热门项链接：{} 热门项标题：{} 热门项评分：{}'.format(block_name, block_hot_time_range, rank_num, rank_href, rank_detail_title, rank_detail_point))
                    
                    if block_name != '完结动画':
                        await rank_item.hover()
                        await page.wait_for_timeout(500)
                        video_info = page.locator("//div[@class='video-info-module']")
                        # 视频发布时间
                        time = await video_info.locator("//span[@class='time']").inner_text()
                        # 视频播放次数
                        play = await video_info.locator("//span[@class='play']").inner_text()
                        # 视频弹幕数量
                        danmu = await video_info.locator("//span[@class='danmu']").inner_text()
                        # 视频收藏数量
                        star = await video_info.locator("//span[@class='star']").inner_text()
                        # 视频硬币数量
                        coin = await video_info.locator("//span[@class='coin']").inner_text()
                        
                        # 装载item
                        animeItem = BilibiliAnimeItem()
                        animeItem['block_name'] = block_name
                        animeItem['rank_hot_time_range'] = block_hot_time_range
                        animeItem['rank_item_num'] = rank_num
                        animeItem['rank_item_href'] = rank_href
                        animeItem['rank_item_detail_title'] = rank_detail_title
                        animeItem['rank_item_detail_point'] = rank_detail_point
                        animeItem['rank_item_time'] = time
                        animeItem['rank_item_play'] = play
                        animeItem['rank_item_danmu'] = danmu
                        animeItem['rank_item_star'] = star
                        animeItem['rank_item_coin'] = coin

                        data_list_result.append(animeItem)

                        logging.info('视频发布时间：{} 视频播放次数：{} 视频弹幕数量：{} 视频收藏数量：{} 视频硬币数量：{}'.format(time, play, danmu, star, coin))
            
            result['channel_type'] = self.channel_name
            result['data_list'] = data_list_result
            
        await page.close()
        yield result

    async def err_anime_callback(self, failure):
        page = failure.request.meta["playwright_page"]
        logging.error('番剧页面加载出错')
        await page.close()
    
