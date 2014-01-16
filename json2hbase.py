#!/usr/bin/env python

""" Imports JSON into HBase """

import json
import argparse

TOPLEVELCF = 'CallCF'

class Json2Hbase(object):

    def __init__(self, json_obj):
        self.json_obj = json_obj

    def _is_list(self, json_obj):
        return type(json_obj) == list

    def _is_dict(self, json_obj):
        return type(json_obj) == dict

    def get_hbase_columns(self):
        return self._build_columns(self.json_obj)

    def _build_columns(self, json_obj, level=0, qualifier=''):
        if self._is_dict(json_obj):
            for key in json_obj:
                if level == 0:
                    if self._is_list(json_obj[key]) or\
                             self._is_dict(json_obj[key]):
                                new_qualifier = "%s:" % key\
                                         if self._is_dict(json_obj[key])\
                                            else "%s:%s" % (key, key)
                    else:
                        new_qualifier = "%s:%s" % (TOPLEVELCF, key)
                else:
                    if qualifier[-1] == ":":
                        new_qualifier = '%s%s' % (qualifier, key)
                    else:
                        new_qualifier = '%s.%s' % (qualifier, key)
                for t in self._build_columns(json_obj[key],level+1, new_qualifier):
                    yield t
        elif self._is_list(json_obj):
            i = 1
            for item in json_obj:
                new_qualifier = '%s%d' % (qualifier, i)
                for t in self._build_columns(item, level+1, new_qualifier):
                    yield t
                i += 1
        else:
            yield((qualifier, json_obj))



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Imports JSON into HBase')
    parser.add_argument('path', help="path to json file")
    parser.add_argument('--top-level-cf', dest='top_level_cf',\
                        default="TopLevelCF", help="Name of the top level column family")
    args = parser.parse_args()

    json_dict = json.load(open(args.path,'r'))
    for i in Json2Hbase(json_dict).get_hbase_columns():
        print i
