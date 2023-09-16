#from ..random_user_agent import RandomUserAgent
from ..items import BilibiliItem, BilibiliAnimeItem
from scrapy import Spider, Selector, Request
from scrapy_playwright.page import PageMethod
import logging
from datetime import datetime
import os

class BilibiliSpider(Spider):
   
    name: str = 'bilibili'
    allowed_domains: list = ['bilibili.com']
    start_url: str = 'https://www.bilibili.com'
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
        self.url_schema = 'https:'
        self.all_channels_name_href = {}
        self.all_channels_name_func = {'番剧': self.anime_parse, '国创': self.guochuang_parse, 
                                       '综艺': self.variety_parse, '动画': self.douga_parse, 
                                       '鬼畜': self.kichiku_parse, '娱乐': self.ent_parse, 
                                       '科技': self.tech_parse, '美食': self.food_parse, 
                                       '电影': self.movie_parse, '电视剧': self.tv_parse, 
                                       '游戏': self.game_parse, '音乐': self.music_parse, 
                                       '影视': self.cinephile_parse, '生活': self.life_parse}
        self.all_channels_name_transform = {'番剧': 'anime', '国创': 'guochuang', 
                                           '综艺': 'variety', '动画': 'douga', 
                                           '鬼畜': 'kichiku', '娱乐': 'ent', 
                                           '科技': 'tech', '美食': 'food', 
                                           '电影': 'movie', '电视剧': 'tv', 
                                           '游戏': 'game', '音乐': 'music', 
                                           '影视': 'cinephile', '生活': 'life'}
        self.target_channels = ('番剧', '国创')
        #self.target_channels = ('番剧', '国创', '动画', '鬼畜', '娱乐', '科技', '美食', '游戏', '音乐', '生活')
        #target_channels = ('番剧', '国创', '综艺', '动画', '鬼畜', '娱乐', '科技', '美食', '电影', '电视剧', '游戏', '音乐', '影视', '生活')
        #target_channels = ('番剧', '国创', '动画', '鬼畜', '娱乐', '科技', '美食', '游戏', '音乐', '生活')
        self.current_timestamp = datetime.now().strftime('%Y%m%d%H%M%S%f')
        self.screenshot_path = self.get_screenshot_path()

    def start_requests(self):
        yield Request(url = self.start_url, 
            meta = {
                'playwright': True, 
                'playwright_context': 'bilibili', 
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                 },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'load',    
                },
                'playwright_include_page': True,
            }, 
            callback = self.index_parse,
            dont_filter = True,
        )

    async def index_parse(self, response):
        logging.info('本次访问bilibili主页随机UA为{}'.format(response.request.headers['user-agent']))
        
        page = response.meta["playwright_page"]
        resp = await page.content()
        await page.screenshot(path = self.get_channel_screenshot_png_path('index'), full_page = True)
        with open(file = self.get_channel_screenshot_html_path('index'), mode = 'w', encoding = 'utf-8') as f:
            f.write(resp)

        logging.info('首页html保存为{}'.format(self.get_channel_screenshot_html_path('index')))
        logging.info('首页截图保存为{}'.format(self.get_channel_screenshot_html_path('index')))
        
        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('浏览器版本过低，出现下载建议')
            self.start_requests(self)

        # 兼容浏览器版本获取各个频道标题与链接
        channels_text = []
        channels_href = []
        channels_text = selector.xpath("//div[contains(@class,'item') and contains(@class,'van-popover__reference')]/a/span/text()")
        if len(channels_text) != 0:
            channels_href = selector.xpath("//div[contains(@class,'item') and contains(@class,'van-popover__reference')]/a/@href")
        else:
            channels_text = selector.xpath("//a[@class='channel-link']/text()")
            if len(channels_text) != 0:
                channels_href = selector.xpath("//a[@class='channel-link']/@href")
        
        # 整合频道标题与频道链接
        for i in range(len(channels_text)):
            channels_final_href: str
            if 'http' in channels_href[i].extract():
                channels_final_href = channels_href[i].extract()
            else:
                channels_final_href = self.url_schema + channels_href[i].extract()
            self.all_channels_name_href[channels_text[i].extract()] = channels_final_href
            logging.info('整理出的频道 {}, url {}'.format(channels_text[i].extract(), channels_final_href))
        
        # 开始请求每个目标频道的主页
        for channel_name in self.target_channels:
            logging.info('请求 {} 频道'.format(channel_name))
            # 需要兼容版本，有些B站首页不存在（综艺、美食、电视剧）
            if channel_name in self.all_channels_name_href and channel_name in self.all_channels_name_func:
                yield self.get_channel_request(channel_name)
                pass
        await page.close()

    def errcallback(self, failure):
        page = failure.request.meta["playwright_page"]
        page.close()
        logging.error('页面加载出错')

    # 番剧
    async def anime_parse(self, response):
        result = BilibiliItem()
        data_list_result = []
        
        channel_name = '番剧'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))
                
            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
                
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
            
            result['channel_type'] = channel_name
            result['data_list'] = data_list_result
            
        await page.close()
       # logging.info(result) 
        yield result

    # 国创
    async def guochuang_parse(self, response):
        channel_name = '国创'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()

    # 动画
    async def douga_parse(self, response):
        channel_name = '动画'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
    
    # 综艺
    async def variety_parse(self, response):
        channel_name = '综艺'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
    
    # 鬼畜
    async def kichiku_parse(self, response):
        channel_name = '鬼畜'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 娱乐
    async def ent_parse(self, response):
        channel_name = '娱乐'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 科技
    async def tech_parse(self, response):
        channel_name = '科技'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 美食
    async def food_parse(self, response):
        channel_name = '美食'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 电影
    async def movie_parse(self, response):
        channel_name = '电影'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 电视剧
    async def tv_parse(self, response):
        channel_name = '电视剧'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 游戏
    async def game_parse(self, response):
        channel_name = '游戏'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
    
    # 音乐
    async def music_parse(self, response):
        channel_name = '音乐'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 影视
    async def cinephile_parse(self, response):
        channel_name = '影视'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
        
    # 生活
    async def life_parse(self, response):
        channel_name = '生活'
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()

    async def channel_parse(self, response, channel_name):
        logging.info('请求频道{}， ua为{}'.format(channel_name, response.request.headers['user-agent']))
        page = response.meta['playwright_page']

        resp = await page.content()

        selector = Selector(text = resp)
        browser_version_toolow_tips = selector.xpath("//div[contains(@class, 'pic-box')]/p/text()").extract()
        # 如果浏览器版本过低，无法正常访问此页面
        if len(browser_version_toolow_tips) != 0 and '浏览器版本过低' in browser_version_toolow_tips[0] and '已无法正常访问此页面' in browser_version_toolow_tips[0]:
            logging.error('访问{}频道浏览器版本过低，出现下载建议，使用的ua为 {}'.format(channel_name, response.request.headers['user-agent']))
            yield self.get_channel_request(channel_name)
        # 可以正常访问
        else:
            # 处于新版页面，需要回退到旧版页面
            if selector.xpath("//div[@class='new-q section' and contains(text(), '回到旧版')]").extract_first() is not None:
                await page.locator(selector = "//div[@class='new-q section' and contains(text(), '回到旧版')]").click()
                await page.wait_for_load_state('load')
                logging.info('{}频道回到旧版页面'.format(channel_name))

            for headline in await page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
                await headline.scroll_into_view_if_needed()
            await page.wait_for_load_state('networkidle')
            
            old_page_resp = await page.content()

            await page.screenshot(path = self.get_channel_screenshot_png_path(self.all_channels_name_transform[channel_name]), full_page = True)
            with open(file = self.get_channel_screenshot_html_path(self.all_channels_name_transform[channel_name]), mode = 'w', encoding = 'utf-8') as f:
                f.write(old_page_resp)
            
            # 开始解析
            pass
            
        await page.close()
    
    def get_screenshot_path(self):
        result_path = None
        current_path = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        if (os.path.exists(current_path)):
            #获取该目录下的所有文件或文件夹目录
                files = os.listdir(current_path)
                for file in files:
                    ab_path = os.path.join(current_path, file)
                    if (os.path.isdir(ab_path) and os.path.split(ab_path)[-1] == 'screenshot'):
                        result_path = ab_path
                        break
        return result_path

    def get_channel_screenshot_png_path(self, channel_name):
        return os.path.join(self.screenshot_path, channel_name, '{}_{}_screenshot.png'.format(channel_name, self.current_timestamp))
    
    def get_channel_screenshot_html_path(self, channel_name):
        return os.path.join(self.screenshot_path, channel_name, '{}_{}_screenshot.html'.format(channel_name, self.current_timestamp))

    def get_channel_request(self, channel_name):
        return Request(url = self.all_channels_name_href[channel_name],
            meta = {
                'playwright': True,
                'playwright_context': 'bilibili',
                'playwright_context_kwargs': {
                    'ignore_https_errors': True,
                },
                'playwright_page_goto_kwargs': {
                    'wait_until': 'load',
                },
                'playwright_include_page': True,
                'playwright_page_methods': [
                    PageMethod("set_default_timeout", timeout=60000)
                ],
            },
            callback = self.all_channels_name_func[channel_name],
            dont_filter = True,
        )


