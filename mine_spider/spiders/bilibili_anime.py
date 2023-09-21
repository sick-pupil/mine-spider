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
        'PLAYWRIGHT_BROWSER_TYPE': 'firefox',
        'PLAYWRIGHT_LAUNCH_OPTIONS': {'headless': True, 'timeout': 60 * 1000}, 
        'PLAYWRIGHT_MAX_CONTEXTS': 1,
        'PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT': 60 * 1000,
        'PLAYWRIGHT_MAX_PAGES_PER_CONTEXT': 30,
        'ITEM_PIPELINES': {
            'mine_spider.pipelines.BilibiliAnimePipeline': 500
        },
        'DOWNLOADER_MIDDLEWARES': {
            'mine_spider.middlewares.MineSpiderDownloaderMiddleware': 900,
            'mine_spider.middlewares.BilibiliAnimeVideoDownloaderMiddleware': 910,
        }
    }

    def __init__(self):
        self.channel_name = '番剧'
        self.result = Result()
        self.bilibili_anime = BilibiliChannel()
        self.bilibili_anime['channel_name'] = self.channel_name
        self.bilibili_anime['rank_list'] = list()
        self.result['data'] = self.bilibili_anime
        
        self.ua_by_get_reply_list = [
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.1 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2227.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2226.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.4; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2225.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2224.3 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/40.0.2214.93 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.124 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 4.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2049.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.67 Safari/537.36",
            "Mozilla/5.0 (X11; OpenBSD i386) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1944.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.3319.102 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2309.372 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.2117.157 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1866.237 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.137 Safari/4E423F",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.116 Safari/537.36 Mozilla/5.0 (iPad; U; CPU OS 3_2 like Mac OS X; en-us) AppleWebKit/531.21.10 (KHTML, like Gecko) Version/4.0.4 Mobile/7B334b Safari/531.21.10",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.517 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1667.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/32.0.1664.3 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1650.16 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1623.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36",
            "Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.2 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1467.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1500.55 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.93 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.90 Safari/537.36",
            "Mozilla/5.0 (X11; NetBSD) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36",
            "Mozilla/5.0 (X11; CrOS i686 3912.101.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.116 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.60 Safari/537.17",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.15 (KHTML, like Gecko) Chrome/24.0.1295.0 Safari/537.15",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.14 (KHTML, like Gecko) Chrome/24.0.1292.0 Safari/537.14",
            "Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16",
            "Opera/9.80 (Windows NT 6.0) Presto/2.12.388 Version/12.14",
            "Mozilla/5.0 (Windows NT 6.0; rv:2.0) Gecko/20100101 Firefox/4.0 Opera 12.14",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0) Opera 12.14",
            "Mozilla/5.0 (Windows NT 5.1) Gecko/20100101 Firefox/14.0 Opera/12.0",
            "Opera/9.80 (Windows NT 6.1; WOW64; U; pt) Presto/2.10.229 Version/11.62",
            "Opera/9.80 (Windows NT 6.0; U; pl) Presto/2.10.229 Version/11.62",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
            "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; de) Presto/2.9.168 Version/11.52",
            "Opera/9.80 (Windows NT 5.1; U; en) Presto/2.9.168 Version/11.51",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; de) Opera 11.51",
            "Opera/9.80 (X11; Linux x86_64; U; fr) Presto/2.9.168 Version/11.50",
            "Opera/9.80 (X11; Linux i686; U; hu) Presto/2.9.168 Version/11.50",
            "Opera/9.80 (X11; Linux i686; U; ru) Presto/2.8.131 Version/11.11",
            "Opera/9.80 (X11; Linux i686; U; es-ES) Presto/2.8.131 Version/11.11",
            "Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/5.0 Opera 11.11",
            "Opera/9.80 (X11; Linux x86_64; U; bg) Presto/2.8.131 Version/11.10",
            "Opera/9.80 (Windows NT 6.0; U; en) Presto/2.8.99 Version/11.10",
            "Opera/9.80 (Windows NT 5.1; U; zh-tw) Presto/2.8.131 Version/11.10",
            "Opera/9.80 (Windows NT 6.1; Opera Tablet/15165; U; en) Presto/2.8.149 Version/11.1",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1",
            "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10; rv:33.0) Gecko/20100101 Firefox/33.0",
            "Mozilla/5.0 (X11; Linux i586; rv:31.0) Gecko/20100101 Firefox/31.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20130401 Firefox/31.0",
            "Mozilla/5.0 (Windows NT 5.1; rv:31.0) Gecko/20100101 Firefox/31.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:29.0) Gecko/20120101 Firefox/29.0",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/29.0",
            "Mozilla/5.0 (X11; OpenBSD amd64; rv:28.0) Gecko/20100101 Firefox/28.0",
            "Mozilla/5.0 (X11; Linux x86_64; rv:28.0) Gecko/20100101  Firefox/28.0",
            "Mozilla/5.0 (Windows NT 6.1; rv:27.3) Gecko/20130101 Firefox/27.3",
            "Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:27.0) Gecko/20121011 Firefox/27.0",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:25.0) Gecko/20100101 Firefox/25.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:25.0) Gecko/20100101 Firefox/25.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:24.0) Gecko/20100101 Firefox/24.0",
            "Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0) Gecko/20100101 Firefox/24.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:24.0) Gecko/20100101 Firefox/24.0",
            "Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/23.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20130406 Firefox/23.0",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:23.0) Gecko/20131011 Firefox/23.0",
            "Mozilla/5.0 (Windows NT 6.2; rv:22.0) Gecko/20130405 Firefox/22.0",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:22.0) Gecko/20130328 Firefox/22.0",
            "Mozilla/5.0 (Windows NT 6.1; rv:22.0) Gecko/20130405 Firefox/22.0",
            "Mozilla/5.0 (Microsoft Windows NT 6.2.9200.0); rv:22.0) Gecko/20130405 Firefox/22.0",
            "Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/21.0.1",
            "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:16.0.1) Gecko/20121011 Firefox/21.0.1",
            "Mozilla/5.0 (Windows NT 6.2; Win64; x64; rv:21.0.0) Gecko/20121011 Firefox/21.0.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20130331 Firefox/21.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (X11; Linux i686; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20130514 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.2; rv:21.0) Gecko/20130326 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130401 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130331 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20130330 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20130401 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20130328 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.1; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130401 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20130331 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 5.1; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 5.0; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.8; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Windows NT 6.2; Win64; x64;) Gecko/20100101 Firefox/20.0",
            "Mozilla/5.0 (Windows x86; rv:19.0) Gecko/20100101 Firefox/19.0",
            "Mozilla/5.0 (Windows NT 6.1; rv:6.0) Gecko/20100101 Firefox/19.0",
            "Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/18.0.1",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:18.0)  Gecko/20100101 Firefox/18.0",
            "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:17.0) Gecko/20100101 Firefox/17.0.6",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; AS; rv:11.0) like Gecko",
            "Mozilla/5.0 (compatible, MSIE 11, Windows NT 6.3; Trident/7.0;  rv:11.0) like Gecko",
            "Mozilla/5.0 (compatible; MSIE 10.6; Windows NT 6.1; Trident/5.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727) 3gpp-gba UNTRUSTED/1.0",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 7.0; InfoPath.3; .NET CLR 3.1.40767; Trident/6.0; en-IN)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/5.0)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/4.0; InfoPath.2; SV1; .NET CLR 2.0.50727; WOW64)",
            "Mozilla/5.0 (compatible; MSIE 10.0; Macintosh; Intel Mac OS X 10_7_3; Trident/6.0)",
            "Mozilla/4.0 (Compatible; MSIE 8.0; Windows NT 5.2; Trident/6.0)",
            "Mozilla/4.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/5.0)",
            "Mozilla/1.22 (compatible; MSIE 10.0; Windows 3.1)",
            "Mozilla/5.0 (Windows; U; MSIE 9.0; WIndows NT 9.0; en-US))",
            "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 7.1; Trident/5.0)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; Media Center PC 6.0; InfoPath.3; MS-RTC LM 8; Zune 4.7",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; InfoPath.3; MS-RTC LM 8; .NET4.0C; .NET4.0E)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; chromeframe/12.0.742.112)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 4.0; Tablet PC 2.0; InfoPath.3; .NET4.0C; .NET4.0E)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; yie8)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; InfoPath.2; .NET CLR 1.1.4322; .NET4.0C; Tablet PC 2.0)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; FunWebProducts)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; chromeframe/13.0.782.215)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; chromeframe/11.0.696.57)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0) chromeframe/10.0.648.205",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.1; SV1; .NET CLR 2.8.52393; WOW64; en-US)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; chromeframe/11.0.696.57)",
            "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/4.0; GTB7.4; InfoPath.3; SV1; .NET CLR 3.1.76908; WOW64; en-US)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.1; Trident/4.0; GTB7.4; InfoPath.2; SV1; .NET CLR 3.3.69573; WOW64; en-US)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; InfoPath.1; SV1; .NET CLR 3.8.36217; WOW64; en-US)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; .NET CLR 2.7.58687; SLCC2; Media Center PC 5.0; Zune 3.4; Tablet PC 3.6; InfoPath.3)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.2; Trident/4.0; Media Center PC 4.0; SLCC1; .NET CLR 3.0.04320)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 1.1.4322)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; InfoPath.2; SLCC1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 2.0.50727)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.1; SLCC1; .NET CLR 1.1.4322)",
            "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 5.0; Trident/4.0; InfoPath.1; SV1; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729; .NET CLR 3.0.04506.30)",
            "Mozilla/5.0 (compatible; MSIE 7.0; Windows NT 5.0; Trident/4.0; FBSMTWB; .NET CLR 2.0.34861; .NET CLR 3.0.3746.3218; .NET CLR 3.5.33652; msn OptimizedIE8;ENUS)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.2; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; Media Center PC 6.0; InfoPath.2; MS-RTC LM 8)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; Media Center PC 6.0; InfoPath.2; MS-RTC LM 8",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; Media Center PC 6.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET4.0C)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; InfoPath.3; .NET4.0C; .NET4.0E; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MS-RTC LM 8)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; InfoPath.2)",
            "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Zune 3.0)",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
            "Mozilla/5.0 (iPad; CPU OS 6_0 like Mac OS X) AppleWebKit/536.26 (KHTML, like Gecko) Version/6.0 Mobile/10A5355d Safari/8536.25",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_8) AppleWebKit/537.13+ (KHTML, like Gecko) Version/5.1.7 Safari/534.57.2",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/534.55.3 (KHTML, like Gecko) Version/5.1.3 Safari/534.53.10",
            "Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko ) Version/5.1 Mobile/9B176 Safari/7534.48.3",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; de-at) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; da-dk) AppleWebKit/533.21.1 (KHTML, like Gecko) Version/5.0.5 Safari/533.21.1",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; tr-TR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; ko-KR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; fr-FR) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; cs-CZ) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; ja-JP) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; zh-cn) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; PPC Mac OS X 10_5_8; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; zh-cn) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; sv-se) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ko-kr) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; ja-jp) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; it-it) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; fr-fr) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; es-es) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-us) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-gb) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; de-de) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.4 Safari/533.20.27",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; sv-SE) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; ja-JP) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; de-DE) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; hu-HU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; de-DE) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; ja-JP) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; it-IT) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/533.20.25 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_7; en-us) AppleWebKit/534.16+ (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; fr-ch) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_5; de-de) AppleWebKit/534.15+ (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_5; ar) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.3 Safari/533.19.4",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; zh-HK) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
            "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; tr-TR) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; nb-NO) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
            "Mozilla/5.0 (Windows; U; Windows NT 6.0; fr-FR) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-TW) AppleWebKit/533.19.4 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
            "Mozilla/5.0 (Windows; U; Windows NT 5.1; ru-RU) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
            "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_8; zh-cn) AppleWebKit/533.18.1 (KHTML, like Gecko) Version/5.0.2 Safari/533.18.5",
        ]
    
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
                # block_name = await block_item.locator("//h4[@class='name']").inner_text()
                block_name = block_item_selector.xpath(query = "//h4[@class='name']/text()").extract_first()
                # 小类区热门排行时间范围
                # block_hot_time_range = await block_item.locator("//span[@class='selected']").inner_text()
                block_hot_time_range = block_item_selector.xpath(query = "//span[@class='selected']/text()").extract_first()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                                
                for rank_item in rank_item_list_in_block:
                    rank_item_selector = Selector(text = await rank_item.inner_html())
                    # 热门排行项序号
                    # rank_item_num = await rank_item.locator("//i[@class='ri-num']").inner_text()
                    rank_item_num = rank_item_selector.xpath(query = "//i[@class='ri-num']/text()").extract_first()
                    # rank_item_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
                    # 热门排行项视频链接
                    # rank_item_href = await rank_item_base_info.get_attribute('href')
                    # rank_item_detail = rank_item_base_info.locator("//div[@class='ri-detail']")
                    rank_item_href = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/@href")
                    # 热门排行项视频标题
                    # rank_item_detail_title = await rank_item_detail.locator("//p[@class='ri-title']").inner_text()
                    rank_item_detail_title = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-title']/p[@class='ri-title']/text()").extract_first()
                    # 热门排行项综合评分
                    # rank_item_detail_point = await rank_item_detail.locator("//p[@class='ri-point']").inner_text()
                    rank_item_detail_point = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-title']/p[@class='ri-point']/text()").extract_first()
                    
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
                        #video_info = page.locator("//div[@class='video-info-module']")
                        video_info_html = await page.locator("//div[@class='video-info-module']").inner_html()
                        video_info_selector = Selector(text = video_info_html)
                        # 热门排行项发布时间
                        # rank_item_time = await video_info.locator("//span[@class='time']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='time']/text()").extract_first()
                        # 热门排行项播放次数
                        # rank_item_play = await video_info.locator("//span[@class='play']").inner_text()
                        rank_item_play = video_info_selector.xpath(query = "//span[@class='play']/text()").extract_first()
                        # 热门排行项弹幕数量
                        # rank_item_danmu = await video_info.locator("//span[@class='danmu']").inner_text()
                        rank_item_danmu = video_info_selector.xpath(query = "//span[@class='danmu']/text()").extract_first()
                        # 热门排行项收藏数量
                        # rank_item_star = await video_info.locator("//span[@class='star']").inner_text()
                        rank_item_star = video_info_selector.xpath(query = "//span[@class='star']/text()").extract_first()
                        # 热门排行项硬币数量
                        # rank_item_coin = await video_info.locator("//span[@class='coin']").inner_text()
                        rank_item_coin = video_info_selector.xpath(query = "//span[@class='coin']/text()").extract_first()
                        # 热门排行项发布人
                        # rank_item_pubman = await video_info.locator("//span[@class='name']").inner_text()
                        rank_item_pubman = video_info_selector.xpath(query = "//span[@class='name']/text()").extract_first()
                        # 热门排行项简介
                        # rank_item_desc = await video_info.locator("//p[@class='txt']").inner_text()
                        rank_item_desc = video_info_selector.xpath(query = "//p[@class='txt']/text()").extract_first()

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
                    
                    #yield Request(url = rank_item_href, 
                    #    meta = {
                    #        'playwright': True, 
                    #        'playwright_context': 'bilibili-anime', 
                    #        'playwright_context_kwargs': {
                    #            'ignore_https_errors': True,
                    #         },
                    #        'playwright_page_goto_kwargs': {
                    #            'wait_until': 'load',
                    #        },
                    #        'playwright_include_page': True,
                    #    }, 
                    #    callback = self.anime_video_parse,
                    #    errback = self.err_anime_callback,
                    #    dont_filter = True,
                    #)
                
                # 点击热门时间范围
                await block_item.locator("//div[@class='pgc-rank-dropdown rank-dropdown']").hover()
                await block_item.locator("//li[@class='dropdown-item' and contains(text(), '一周')]").click()
                await page.wait_for_load_state("networkidle")
                block_item_selector = Selector(text = await block_item.inner_html())
                
                # 小类区热门排行时间范围
                # block_hot_time_range = await block_item.locator("//span[@class='selected']").inner_text()
                block_hot_time_range = block_item_selector.xpath(query = "//span[@class='selected']/text()").extract_first()
                # 小类区热门列表
                rank_item_list_in_block = await block_item.locator("//li[contains(@class, 'rank-item')]").all()
                for rank_item in rank_item_list_in_block:
                    rank_item_selector = Selector(text = await rank_item.inner_html())
                    # 热门排行项序号
                    # rank_item_num = await rank_item.locator("//i[@class='ri-num']").inner_text()
                    rank_item_num = rank_item_selector.xpath(query = "//i[@class='ri-num']/text()").extract_first()
                    # rank_item_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
                    # 热门排行项视频链接
                    # rank_item_href = await rank_item_base_info.get_attribute('href')
                    rank_item_href = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/@href")
                    # rank_item_detail = rank_item_base_info.locator("//div[@class='ri-detail']")
                    # 热门排行项视频标题
                    # rank_item_detail_title = await rank_item_detail.locator("//p[@class='ri-title']").inner_text()
                    rank_item_detail_title = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-title']/p[@class='ri-title']/text()").extract_first()
                    # 热门排行项综合评分
                    # rank_item_detail_point = await rank_item_detail.locator("//p[@class='ri-point']").inner_text()
                    rank_item_detail_point = rank_item_selector.xpath(query = "//a[@class='ri-info-wrap clearfix']/div[@class='ri-title']/p[@class='ri-point']/text()").extract_first()
                    
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
                        # video_info = page.locator("//div[@class='video-info-module']")
                        video_info_html = await page.locator("//div[@class='video-info-module']").inner_html()
                        video_info_selector = Selector(text = video_info_html)
                        # 热门排行项发布时间
                        # rank_item_time = await video_info.locator("//span[@class='time']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='time']/text()").extract_first()
                        # 热门排行项播放次数
                        # rank_item_play = await video_info.locator("//span[@class='play']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='play']/text()").extract_first()
                        # 热门排行项弹幕数量
                        # rank_item_danmu = await video_info.locator("//span[@class='danmu']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='danmu']/text()").extract_first()
                        # 热门排行项收藏数量
                        # rank_item_star = await video_info.locator("//span[@class='star']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='star']/text()").extract_first()
                        # 热门排行项硬币数量
                        # rank_item_coin = await video_info.locator("//span[@class='coin']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='coin']/text()").extract_first()
                        # 热门排行项发布人
                        # rank_item_pubman = await video_info.locator("//span[@class='name']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//span[@class='name']/text()").extract_first()
                        # 热门排行项简介
                        # rank_item_desc = await video_info.locator("//p[@class='txt']").inner_text()
                        rank_item_time = video_info_selector.xpath(query = "//p[@class='txt']/text()").extract_first()
                        
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
                    
                    #yield Request(url = rank_item_href, 
                    #    meta = {
                    #        'playwright': True, 
                    #        'playwright_context': 'bilibili-anime', 
                    #        'playwright_context_kwargs': {
                    #            'ignore_https_errors': True,
                    #         },
                    #        'playwright_page_goto_kwargs': {
                    #            'wait_until': 'load',
                    #        },
                    #        'playwright_include_page': True,
                    #    }, 
                    #    callback = self.anime_video_parse,
                    #    errback = self.err_anime_callback,
                    #    dont_filter = True,
                    #)

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
    
    async def anime_video_parse(self, response):
        pass

    async def err_anime_callback(self, failure):
        page = failure.request.meta["playwright_page"]
        logging.error('番剧页面加载出错')
        await page.close()
    
