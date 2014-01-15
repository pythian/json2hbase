#!/usr/bin/env python

""" Imports JSON into HBase """

import json
import argparse

TOPLEVELCF = 'CallCF'

class HBaseColumns(object):

    def __init__(self, json_dict):
        self._columns = []
        self._build_columns(json_dict)

    def _build_columns(self, json_obj, level=0, qualifier=''):
        if type(json_obj) == dict:
            for key in json_obj:
                if level == 0:
                    if type(json_obj[key]) ==  dict:
                        new_qualifier = "%s:" % key
                    else:
                        new_qualifier = "%s:%s" % (TOPLEVELCF, key)
                else:
                    if qualifier[-1] == ":":
                        new_qualifier = '%s%s' % (qualifier, key)
                    else:
                        new_qualifier = '%s.%s' % (qualifier, key)
                self._build_columns(json_obj[key], level+1, new_qualifier)
        elif type(json_obj) == list:
            i = 1
            for item in json_obj:
                new_qualifier = '%s%d' % (qualifier, i)
                self._build_columns(item, level+1, new_qualifier)
                i += 1
        else:
            self._columns.append((qualifier, json_obj))

    def get_columns(self):
        return self._columns



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Imports JSON into HBase')
    parser.add_argument('path', help="path to json file")
    args = parser.parse_args()

    json_dict = json.load(open(args.path,'r'))
    for i in HBaseColumns(json_dict).get_columns():
        print i
