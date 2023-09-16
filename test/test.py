from playwright.sync_api import sync_playwright
import pymysql

'''
with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    page.goto("https://www.bilibili.com/anime")
    
    for headline in page.locator("//div[contains(@class, 'headline') and contains(@class, 'clearfix')]").all():
        headline.scroll_into_view_if_needed()
    page.wait_for_load_state('networkidle')
    
    block_list = page.locator(selector = "//div[@class='block-area block-bangumi']", has = page.locator(selector = "//h4[@class='name']")).all()
    for block_item in block_list:
        block_name = block_item.locator("//h4[@class='name']").inner_text()
        block_hot_time_range = block_item.locator("//span[@class='selected']").inner_text()
        rank_item_list_in_block = block_item.locator("//li[contains(@class, 'rank-item')]").all()
        for rank_item in rank_item_list_in_block:
            rank_num = rank_item.locator("//i[@class='ri-num']").inner_text()
            rank_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
            rank_href = rank_base_info.get_attribute('href')
            rank_detail = rank_base_info.locator("//div[@class='ri-detail']")
            rank_detail_title = rank_detail.locator("//p[@class='ri-title']").inner_text()
            rank_detail_point = rank_detail.locator("//p[@class='ri-point']").inner_text()
            
            print('{} {} {} {} {} {}'.format(block_name, block_hot_time_range, rank_num, rank_href, rank_detail_title, rank_detail_point))
            
            if block_name != '完结动画':
                rank_item.hover()
                page.wait_for_timeout(500)
                video_info = page.locator("//div[@class='video-info-module']")
                time = video_info.locator("//span[@class='time']").inner_text()
                play = video_info.locator("//span[@class='play']").inner_text()
                danmu = video_info.locator("//span[@class='danmu']").inner_text()
                star = video_info.locator("//span[@class='star']").inner_text()
                coin = video_info.locator("//span[@class='coin']").inner_text()
                
                print('{} {} {} {} {}'.format(time, play, danmu, star, coin))
    
        block_item.locator("//div[@class='pgc-rank-dropdown rank-dropdown']").hover()
        block_item.locator("//li[@class='dropdown-item' and contains(text(), '一周')]").click()
        page.wait_for_load_state("networkidle")
        
        for rank_item in rank_item_list_in_block:
            rank_num = rank_item.locator("//i[@class='ri-num']").inner_text()
            rank_base_info = rank_item.locator("//a[@class='ri-info-wrap clearfix']")
            rank_href = rank_base_info.get_attribute('href')
            rank_detail = rank_base_info.locator("//div[@class='ri-detail']")
            rank_detail_title = rank_detail.locator("//p[@class='ri-title']").inner_text()
            rank_detail_point = rank_detail.locator("//p[@class='ri-point']").inner_text()
            
            print('{} {} {} {} {} {}'.format(block_name, block_hot_time_range, rank_num, rank_href, rank_detail_title, rank_detail_point))
            
            if block_name != '完结动画':
                rank_item.hover()
                page.wait_for_timeout(500)
                video_info = page.locator("//div[@class='video-info-module']")
                time = video_info.locator("//span[@class='time']").inner_text()
                play = video_info.locator("//span[@class='play']").inner_text()
                danmu = video_info.locator("//span[@class='danmu']").inner_text()
                star = video_info.locator("//span[@class='star']").inner_text()
                coin = video_info.locator("//span[@class='coin']").inner_text()
                
                print('{} {} {} {} {}'.format(time, play, danmu, star, coin))
                
    page.close()
    browser.close()
'''

db_conn = pymysql.connect(host = '192.168.1.102', port = 3306, db = 'spider', user = 'root', passwd = '56962438', charset = 'utf8')
db_cursor = db_conn.cursor()
