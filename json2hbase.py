#!/usr/bin/env python

""" Imports JSON into HBase """

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
import json
import argparse
import os

TOP_LEVEL_CF = 'X'
CONFIG_FILE = './conf/json2hbase-config.json';

class Json2Hbase(object):

    def __init__(self, site_config, table_name, top_level_cf, json_obj):
        self.json_obj = json_obj
        self.table_name = table_name
        self.top_level_cf = top_level_cf
        self.hbase_host = site_config['host']
        self.hbase_port = int(site_config['port'])
        self.batch_size = int(site_config['batchSize'])

    def _is_list(self, json_obj):
        return type(json_obj) == list

    def _is_dict(self, json_obj):
        return type(json_obj) == dict

    def get_hbase_columns(self):
        return self._build_columns(self.json_obj)

    def get_hbase_column_families(self):
        cfs = set()
        for c in self.get_hbase_columns():
           cfs.add(c[0])
        for cf in cfs:
           yield cf

    def _build_columns(self, json_obj, level=0, cf='', qualifier=''):
        if self._is_dict(json_obj):
            for key in json_obj:
                if level == 0:
                    if self._is_list(json_obj[key]) or self._is_dict(json_obj[key]):
                        new_cf = key
                        new_qualifier = '%s:' % key
                    else:
                        new_cf = self.top_level_cf
                        new_qualifier = '%s:%s' % (self.top_level_cf, key)
                else:
                    new_cf = cf
                    if qualifier[-1] == ':':
                        new_qualifier = '%s%s' % (qualifier, key)
                    else:
                        new_qualifier = '%s.%s' % (qualifier, key)
                for t in self._build_columns(json_obj[key], level+1, new_cf, new_qualifier):
                    yield t
        elif self._is_list(json_obj):
            i = 1
            for item in json_obj:
                new_qualifier = '%s%d' % (qualifier, i)
                for t in self._build_columns(item, level+1, cf, new_qualifier):
                    yield t
                i += 1
        else:
            yield((cf, qualifier, json_obj))


    def _open_connection(self):
        # Connect to HBase Thrift server
        self.thrift_transport = TTransport.TBufferedTransport(TSocket.TSocket(self.hbase_host, self.hbase_port))
        self.thrift_protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.thrift_transport)

        # Create and open the client connection
        self.hbase_client = Hbase.Client(self.thrift_protocol)
        self.thrift_transport.open()

    def _close_connection(self):
        self.thrift_transport.close()

    def _ensure_table(self):
        tables = self.hbase_client.getTableNames()
        # if table does not exist, create it
        if not self.table_name in tables:
            cfs = map(lambda x: Hbase.ColumnDescriptor(name=x), list(self.get_hbase_column_families()))
            self.hbase_client.createTable(self.table_name, cfs)
        # if table exists, verifies if it contains all the column families
        else:
            table_cfs = map(lambda x: x[:-1], self.hbase_client.getColumnDescriptors(self.table_name))
            missing_cfs = []
            for cf in self.get_hbase_column_families():
                if not cf in table_cfs:
                    missing_cfs.append(cf)
            if len(missing_cfs) > 0:
                raise Exception("The table already exists but does not contain these column familie: %s" % (missing_cfs))

    def load_data(self):
        self._open_connection()
        self._ensure_table()
        self._close_connection()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Imports JSON into HBase')
    parser.add_argument('path', help='path to json file')
    parser.add_argument('--top-level-cf', dest='top_level_cf', \
                        default=TOP_LEVEL_CF, help='Name of the top level column family')
    parser.add_argument('--config-file', dest='config_file', \
                        default=CONFIG_FILE, help='Path to configuration file')
    parser.add_argument('--site', dest='site', \
                        help='Name of the site for which to run the load', required=True)
    parser.add_argument('--table-name', dest='table_name', \
                        help='Name of the table that will receive the data', required=True)
    options = parser.parse_args()

    # load config file

    if not os.path.exists(options.config_file):
        raise Exception('Cannot find configuration file %s' % (options.config_file))
    else:
        config = json.load( open(options.config_file) )
        if not options.site in config:
            raise Exception('\nThe configuration file %s doesn\'t contain configuration for site [%s]'\
                            % (options.config_file, options.site) +\
                            '\nAvailable site configurations: %s' % (', '.join(config)))
        else:
            site_config = config[options.site]

    # load data file

    json_dict = json.load(open(options.path, 'r'))
    #for i in Json2Hbase(site_config, options.table_name, options.top_level_cf, json_dict).get_hbase_columns():
    for i in Json2Hbase(site_config, options.table_name, options.top_level_cf, json_dict).get_hbase_column_families():
        print i
    Json2Hbase(site_config, options.table_name, options.top_level_cf, json_dict).load_data()

