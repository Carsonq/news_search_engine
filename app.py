__author__ = 'zhen'

import sys
reload(sys)
sys.setdefaultencoding('utf-8') 

import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))

from news import crawl
from stem_index import calculate_index

if __name__ == '__main__':
	crawl()
	calculate_index()