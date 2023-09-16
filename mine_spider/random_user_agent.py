# -*- coding: utf-8 -*-

from fake_useragent import UserAgent
import threading

class RandomUserAgent():
    
    ua = None
    lock = threading.Lock()
    
    @classmethod
    def getUA(cls):
        if cls.ua is None:
            with cls.lock:
                if cls.ua is None:
                    cls.ua = UserAgent(path=r'fake_useragent.json', use_cache_server=False, cache=False, verify_ssl=False)
                    pass
        return cls.ua