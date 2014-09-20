#!/usr/bin/env python

""" Imports JSON into HBase """

from __future__ import print_function
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
import sys

DEFAULT_TOP_LEVEL_CF = 'data'
DEFAULT_CONFIG_FILE = './conf/json2hbase-config.json'
ROWKEY_FIELD = '_rowkey'

class Json2Hbase(object):
    
    def __init__(self, host, port, batch_size, table_name, top_level_cf, additional_cfs, cf_mapping={}):
        self.table_name = table_name
        self.top_level_cf = top_level_cf
        self.additional_cfs = additional_cfs
        self.cf_mapping = cf_mapping
        self.hbase_host = host
        self.hbase_port = port
        self.batch_size = batch_size
        self.mutation_batch = []
        #self.mutation_batch_debug = ''
        self.table_exists = False

    def _is_list(self, json_obj):
        return type(json_obj) == list

    def _is_dict(self, json_obj):
        return type(json_obj) == dict

    def _is_int(self, json_obj):
        return type(json_obj) == int

    def _is_long(self, json_obj):
        return type(json_obj) == long

    def _is_float(self, json_obj):
        return type(json_obj) == float

    def _is_bool(self, json_obj):
        return type(json_obj) == bool

    def _is_datetime(self, json_obj):
        return type(json_obj) == datetime

    def _is_unicode(self, json_obj):
        return type(json_obj) == unicode

    def _encode(self, n):
        if self._is_int(n) or self._is_float(n) or self._is_bool(n) or self._is_datetime(n) or self._is_long(n):
            return str(n)
        elif self._is_list(n):
            return json.dumps(n)
        elif self._is_unicode(n):
            return n.encode('utf-8')
        else:
            return n

    def add_cf_mappings(self, qualifiers, cf):
        for qual in qualifiers:
            self.cf_mapping[qual] = cf

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
                    if key in self.cf_mapping:
                        cf = self.cf_mapping[key]
                    else:
                        cf = self.top_level_cf
                    new_qualifier = '%s:%s' % (cf, key)
                else:
                    if qualifier[-1] == ':':
                        new_qualifier = '%s%s' % (qualifier, key)
                    else:
                        new_qualifier = '%s.%s' % (qualifier, key)
                for t in self._build_columns(json_obj[key], level+1, cf, new_qualifier):
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
        #logging.debug("Mutating %s records" % len(self.mutation_batch))
        print("Mutating %s records" % len(self.mutation_batch), file=sys.stderr)
        self.hbase_client.mutateRows(self.table_name, self.mutation_batch, None)
        self.mutation_batch = []
        #self.mutation_batch_debug = ''
        self.cf_mapping = {}

        # FOR DEBUGGING:
        #self.batch_size = 1000

    def close_connection(self):
        if len(self.mutation_batch) > 0:
            self._apply_mutations()
        self.thrift_transport.close()

    def _ensure_table(self, data):
        tables = self.hbase_client.getTableNames()
        # if table does not exist, create it
        if not self.table_name in tables:
            logging.debug("Creating table %s" % (self.table_name))
            # add fixed CF for audio content
            cfNames = list(self.get_hbase_column_families(data))
            for cf_name in self.additional_cfs:
                cfNames.append(cf_name)
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
        #mutations_debug = ''
        for c in self.get_hbase_columns(data):
            qualifier = c[1]
            value = c[2]

            if qualifier == self.top_level_cf + ":" + ROWKEY_FIELD:
                rowkey = value
                #if self.batch_size <= 1000:
                    #mutations_debug = '<ROWKEY>' + value + mutations_debug
            else:
                mutations.append( Hbase.Mutation(column=qualifier, value=value) )
                #if self.batch_size <= 1000:
                    #mutations_debug = mutations_debug + '<QUAL>' + qualifier + '<VALUE>' + str(value)
   
        if len(mutations) > 0:
            self.mutation_batch.append( Hbase.BatchMutation(row=rowkey, mutations=mutations) )
            #if self.batch_size <= 1000:
                #self.mutation_batch_debug = self.mutation_batch_debug + '<BATCH>' + mutations_debug
            if len(self.mutation_batch) >= self.batch_size: 
                self._apply_mutations()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Imports JSON into HBase')
    parser.add_argument('path', help='path to json file')
    parser.add_argument('--top-level-cf', dest='top_level_cf', \
                        default=DEFAULT_TOP_LEVEL_CF, help='Name of the top level column family')
    parser.add_argument('--additional-cfs', dest='additional_cfs', \
                        default=None, help='Comma-separated of column families to be added to the table upon creation')
    parser.add_argument('--config-file', dest='config_file', \
                        default=DEFAULT_CONFIG_FILE, help='Path to configuration file')
    parser.add_argument('--site', dest='site', \
                        help='Name of the site for which to run the load', required=True)
    parser.add_argument('--table-name', dest='table_name', \
                        help='Name of the table that will receive the data', required=True)
    options = parser.parse_args()

    if additional_cfs == None:
        additional_cfs = []
    else:
        additional_cfs = options.additional_cfs.split(',')

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
    loader = Json2Hbase(site_config['host'], int(site_config['port']), int(site_config['batchSize']),
        options.table_name, options.top_level_cf, additional_cfs)
    loader.open_connection()
    loader.load_data(json_dict)
    loader.close_connection()

