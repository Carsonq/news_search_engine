# -*- coding: utf-8 -*-
__author__ = 'zhen'

import sys
reload(sys)
sys.setdefaultencoding('utf-8') 

import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))

import string
import math
import collections
from datetime import datetime
from nltk.stem.porter import PorterStemmer
from config import Configure

from util import Util

def stemmer(words):
    stemmer = PorterStemmer()
    return [stemmer.stem(w) for w in words]

def process(filepath, keyid):
    with open(filepath) as fileobj:
        article_words = []
        for line in fileobj:
            line_info = line.strip()
            tbl = string.maketrans(string.punctuation, ' '*len(string.punctuation))
            words = line_info.translate(tbl).split()
            
            if words:
                article_words += stemmer(words)

        article_word_count = collections.Counter(article_words)
        update_index(article_word_count, keyid)

@Util.dbconnect('wordindex')
def update_index(doc_info, keyid, c):
    doc_url, doc_desc = get_doc_url(keyid)
    for w in doc_info:
        c.update_one({'term': w}, {'$inc': {'df': 1, 'tf': doc_info[w]}, '$push': {'detail': {'doc_id': keyid, 'doc_url': doc_url, 'doc_desc': doc_desc, 'count': doc_info[w]}}}, True)

@Util.dbconnect('wordindex')
def update_tfidf(new_doc_count, c):
    records = c.find({}, {'term': 1, 'total_doc_count': 1, 'df': 1, 'detail.count': 1, 'detail.doc_id': 1, '_id': 0 })
    for i in records:
        x = new_doc_count+i.get('total_doc_count', 0)
        idf = math.log(float(x)/i['df'], 2) if x else 0
        c.update_one({'term': i['term']}, {'$set': {'idf': idf}, '$inc': {'total_doc_count': new_doc_count}})
        for j in i['detail']:
            c.update_one({'term': i['term'], 'detail.doc_id': j['doc_id']}, {'$set': {'detail.$.tfidf': idf * j['count']}})

@Util.dbconnect('articles')
def get_doc_url(keyid, c):
    record = c.find_one({'id': keyid})
    if record:
        return record['url'], record['description']

def calculate_index():
    print '%s\tstart update index' % str(datetime.now())
    cfg = Configure('./config/config.cfg')
    article_repo = cfg.get_config('running', 'article_repo')
    file_names = os.listdir(os.path.join(article_repo, datetime.today().strftime('%m%d%Y')))
    doc_count = 0
    for filename in file_names:
        if not filename.endswith('_DONE'):
            process(os.path.join(article_repo, filename), filename)
            os.rename(os.path.join(article_repo, filename), os.path.join(article_repo, filename+'_DONE'))
            doc_count += 1
    
    if doc_count:
        update_tfidf(doc_count)
    print '%s\tend update index' % str(datetime.now())


if __name__ == '__main__':
    calculate_index()
