# -*- coding: utf-8 -*-
__author__ = 'zhen'

import sys
import ConfigParser

class Configure:
    def __init__(self, config_file):
        self.config = ConfigParser.ConfigParser()
        self.config.read(config_file)

    def get_config(self, source_name, source_type):
        try:
            source_url = self.config.get(source_name, source_type)
        except ConfigParser.NoOptionError:
            return

        return source_url

    def get_sections(self):
        return self.config.sections()

if __name__ == '__main__':
    config = Config()
    config.get_config('google', 'web')
