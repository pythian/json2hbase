#!/usr/bin/env python

""" Imports JSON into HBase """

from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from hbase import Hbase
from datetime import datetime
import json
import logging
import argparse
import os
import struct

DEFAULT_TOP_LEVEL_CF = 'X'
DEFAULT_CONFIG_FILE = './conf/json2hbase-config.json';

class Json2Hbase(object):
    
    def __init__(self, site_config, table_name, top_level_cf):
        self.table_name = table_name
        self.top_level_cf = top_level_cf
        self.hbase_host = site_config['host']
        self.hbase_port = int(site_config['port'])
        self.batch_size = int(site_config['batchSize'])
        self.mutation_batch = []
        self.mutations = 0
        self.table_exists = False

    def _is_list(self, json_obj):
        return type(json_obj) == list

    def _is_dict(self, json_obj):
        return type(json_obj) == dict

    def _is_int(self, json_obj):
        return type(json_obj) == int

    def _is_float(self, json_obj):
        return type(json_obj) == float

    def _is_bool(self, json_obj):
        return type(json_obj) == bool

    def _is_datetime(self, json_obj):
        return type(json_obj) == datetime

    def _encode(self, n):
        if self._is_int(n) or self._is_float(n) or self._is_bool(n) or self._is_datetime(n):
            return str(n)
        elif self._is_list(n):
            return json.dumps(n)
        else:
            return n.encode('utf-8')

    def get_hbase_columns(self, data):
        return self._build_columns(data)

    def get_hbase_column_families(self, data):
        cfs = set()
        for c in self.get_hbase_columns(data):
           cfs.add(c[0])
        for cf in cfs:
           yield cf

    def _build_columns(self, json_obj, level=0, cf='', qualifier=''):
        if self._is_dict(json_obj):
            for key in json_obj:
                if level == 0:
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
        else:
            yield((cf, qualifier, self._encode(json_obj)))


    def open_connection(self):
        # Connect to HBase Thrift server
        self.thrift_transport = TTransport.TBufferedTransport(TSocket.TSocket(self.hbase_host, self.hbase_port))
        self.thrift_protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.thrift_transport)

        # Create and open the client connection
        self.hbase_client = Hbase.Client(self.thrift_protocol)
        self.thrift_transport.open()

    def _apply_mutations(self):
        logging.debug("Mutating %s records"%self.mutations)
        self.hbase_client.mutateRows(self.table_name, self.mutation_batch, None)
        self.mutations=0
        self.mutations_batch=[]

    def close_connection(self):
        if self.mutations > 0:
            self._apply_mutations()
        self.thrift_transport.close()

    def _ensure_table(self, data):
        tables = self.hbase_client.getTableNames()
        # if table does not exist, create it
        if not self.table_name in tables:
            logging.debug("Creating table %s" % (self.table_name))
            # add fixed CF for audio content
            cfNames = list(self.get_hbase_column_families(data))
            cfNames.append('AUDIO')
            # transform into list of HBase columns
            cfs = map(lambda x: Hbase.ColumnDescriptor(name=x), cfNames)
            self.hbase_client.createTable(self.table_name, cfs)
        # if table exists, verifies if it contains all the column families
        else:
            table_cfs = map(lambda x: x[:-1], self.hbase_client.getColumnDescriptors(self.table_name))
            missing_cfs = []
            for cf in self.get_hbase_column_families(data):
                if not cf in table_cfs:
                    missing_cfs.append(cf)
            if len(missing_cfs) > 0:
                raise Exception("The table already exists but does not contain these column familie: %s" % (missing_cfs))


    def load_data(self, data):
        # ensure table exists
        if not self.table_exists:
            self._ensure_table(data)
            self.table_exists = True

        rowkey = ""
        mutations = []
        for c in self.get_hbase_columns(data):
            qualifier = c[1]
            value = c[2]

            if qualifier == self.top_level_cf + ":_id":
                rowkey = value
            
            mutations.append( Hbase.Mutation(column=qualifier, value=value) )
   
        self.mutation_batch.append( Hbase.BatchMutation(row=rowkey, mutations=mutations) )
        self.mutations +=1
        if self.mutations > self.batch_size: 
            self._apply_mutations()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Imports JSON into HBase')
    parser.add_argument('path', help='path to json file')
    parser.add_argument('--top-level-cf', dest='top_level_cf', \
                        default=DEFAULT_TOP_LEVEL_CF, help='Name of the top level column family')
    parser.add_argument('--config-file', dest='config_file', \
                        default=DEFAULT_CONFIG_FILE, help='Path to configuration file')
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
#    for i in Json2Hbase(site_config, options.table_name, options.top_level_cf, json_dict).get_hbase_column_families():
#        print i
    loader = Json2Hbase(site_config, options.table_name, options.top_level_cf)
    loader.open_connection()
    loader.load_data(json_dict)
    loader.close_connection()

