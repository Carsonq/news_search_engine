# -*- coding: utf-8 -*-
__author__ = 'zhen'

import sys
reload(sys)
sys.setdefaultencoding('utf-8') 

import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))

import hashlib
import time
import newspaper
import feedparser
from datetime import datetime
from config import Configure

from util import Util


CFG = Configure('./config/config.cfg')
ARTICLEREPO = os.path.join(CFG.get_config('running', 'article_repo'), datetime.today().strftime('%m%d%Y'))
if not os.path.isdir(ARTICLEREPO):
    os.makedirs(ARTICLEREPO)

def crawl_web_categorypage(url):
    paper = newspaper.build(url, memoize_articles=False, language='en')

    for category in paper.category_urls():
        crawl_web(category)

def crawl_web(url):
    paper = newspaper.build(url, memoize_articles=False, language='en')

    for content in paper.articles:
        if check_exist_url(content.link):
            article, text = crawl_web_page(content)
            if article and text and check_exist(article['id']):
                load_to_disk(article['id'], text)
                load_to_db(article)

@Util.timer
def crawl_web_page(content):
    article = {}
    try:
        content.download()
        content.parse()

        article = {}
        article['url'] = content.url
        article['published'] = content.publish_date.isoformat() if content.publish_date else ''
        article['title'] = content.title
        article['id'] = hashlib.md5(content.text).hexdigest()
        article['crawl_time'] = time.time()
        article['description'] = content.text[:180]

        print '%s\t%s\t%s' % (str(datetime.now()), article['url'], article['id'])
        return article, content.text

    except:
        return None, None

def crawl_rss(url):
    page = feedparser.parse(url)
    if page.status == 200:
        for entry in page.entries:
            if check_exist_url(entry.link):
                article, content = crawl_rss_page(entry)
                if article and content and check_exist(article['id']):
                    load_to_disk(article['id'], content)
                    load_to_db(article)

@Util.timer
def crawl_rss_page(entry):
    article = {}
    try:
        article['url'] = entry.link
        date = entry.published_parsed
        article['publish_time'] = datetime.fromtimestamp(time.mktime(date)).isoformat()

        content = newspaper.Article(entry.link)
        content.download()
        content.parse()
    except:
        return None, None

    article['title'] = content.title
    article['id'] = hashlib.md5(content.text).hexdigest()
    article['crawl_time'] = time.time()
    article['description'] = content.text[:180]

    print '%s\t%s\t%s' % (str(datetime.now()), article['url'], article['id'])
    return article, content.text

@Util.dbconnect('articles')
def check_exist_url(url, c):
    record = c.find_one({'url': url})

    return record == None

@Util.dbconnect('articles')
def check_exist(keyid, c):
    record = c.find_one({'id': keyid})

    return record == None

@Util.dbconnect('articles')
def load_to_db(record, c):
    c.insert_one(record)

def load_to_disk(fileid, content):
    filename = os.path.join(ARTICLEREPO, fileid)
    with open(filename, 'w') as fileobj:
        fileobj.write(content)

def crawl():
    print '%s\tstart crawling' % str(datetime.now())
    for source_name in CFG.get_sections():
        rss_urls = CFG.get_config(source_name, 'rss')
        if rss_urls:
            for rss_url in rss_urls.split(';'):
                crawl_rss(rss_url)

        web_urls = CFG.get_config(source_name, 'web')
        if web_urls:
            for web_url in web_urls.split(';'):
                crawl_web(web_url)
                crawl_web_categorypage(web_url)
    print '%s\tend crawling' % str(datetime.now())


if __name__ == '__main__':
    crawl()
