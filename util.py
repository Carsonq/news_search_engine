# -*- coding: utf-8 -*-
__author__ = 'zhen'


import time
import random
from pymongo import MongoClient
from config import Configure


class Util:
    @staticmethod
    def dbconnect(tablename):
        def outerwrapper(func):
            def wrapper(*args, **kwargs):
                cfg = Configure('./config/config.cfg')
                mongo_url = cfg.get_config('mongodb', 'url')
                mongo_port = int(cfg.get_config('mongodb', 'port'))
                dbname = cfg.get_config('mongodb', 'dbname')
                collection_name = cfg.get_config('mongodb', tablename)

                client = MongoClient(mongo_url, mongo_port)
                db=client[dbname]
                collection = db[collection_name]

                res = func(*args, c = collection, **kwargs)

                return res

            return wrapper

        return outerwrapper


    @staticmethod
    def timer(func):
        def wrapper(*args, **kwargs):
            interval = random.randint(10,30)
            time.sleep(interval)
            res = func(*args, **kwargs)

            return res

        return wrapper


