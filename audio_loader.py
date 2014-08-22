#!/usr/bin/env python

import argparse
import fnmatch
import json
import logging
import os
import re
import sys
import time
from hbase import Hbase
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport import TTransport
from multiprocessing import Pool, Process, Queue

CONFIG_FILE = './conf/json2hbase-config.json'
DEFAULT_AUDIO_CF = 'AUDIO'
DEFAULT_PATTERN = '*'
DEFAULT_BATCH_SIZE = 1000
DEFAULT_WORKERS = 1

# logging facility
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
LOG = logging.getLogger('audio_loader')

class AudioLoader(object):

    def __init__(self, path, pattern, table_name, audio_cf, batch_size, workers, host, port):
        self.path = path
        self.pattern = pattern
        self.table_name = table_name
        self.audio_cf = audio_cf
        self.batch_size = batch_size
        self.workers = workers
        self.host = host
        self.port = port

        self.queue = Queue()

        self.hbase_client = [None] * self.workers
        self.thrift_transport = [None] * self.workers
                    
    def _open_connection(self, worker_id):
        # Connect to HBase Thrift server
        self.thrift_transport[worker_id] = TTransport.TBufferedTransport(TSocket.TSocket(self.host, self.port))

        # Create and open the client connection
        thrift_protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.thrift_transport[worker_id])
        self.hbase_client[worker_id] = Hbase.Client(thrift_protocol)
        self.thrift_transport[worker_id].open()

        return self.hbase_client[worker_id]

    def _close_connection(self, worker_id):
        self.thrift_transport[worker_id].close()

    def _load_file(self, file_path, batch, hbclient):
        basename = os.path.basename(file_path)
        rowkey = re.sub('\.[^.]*$', '', basename)
        qualifier = '%s:.' % (self.audio_cf)
        audio_file = open(file_path, 'r')
        audio_content = audio_file.read()
        if len(audio_content) <= 0:
            LOG.info('FILE: %s, ROWKEY: %s, QUAL: %s, AUDIO_LEN: %d' % (file_path, rowkey, qualifier, len(audio_content)))
        batch.append( Hbase.BatchMutation(row=rowkey, mutations=[ Hbase.Mutation(column=qualifier, value=audio_content) ]) )
        audio_file.close()

        if len(batch) >= self.batch_size:
            try:
                self._flush_files(batch, hbclient)
            except:
                LOG.error('FILE:'+file)

    def _flush_files(self, batch, hbclient):
        hbclient.mutateRows(self.table_name, batch, None)
        del batch[:]

    def _get_all_files(self, path):
        if os.path.isfile(path):
            yield path
        else:
            for root, dir, files in os.walk(self.path):
                filtered_file = fnmatch.filter(files, self.pattern)
                for file in fnmatch.filter(files, self.pattern):
                    yield os.path.join(root, file)

    def _worker(self, worker_id):
        hbclient = self._open_connection(worker_id)

        local_mutations_batch = []
        file = self.queue.get()
        files = 0
        while file != 'EOF':
            self._load_file(file, local_mutations_batch, hbclient)
            files += 1
            file = self.queue.get()
        self._flush_files(local_mutations_batch, hbclient)

        self._close_connection(worker_id)
        LOG.info("Worker #%s processed %d files" % (worker_id, files))

    def load_data(self):

        workers = map(lambda x: Process(target=self._worker, args=(x,)), range(0, self.workers))
        map(lambda w: w.start(), workers)
        for file in self._get_all_files(self.path):
            self.queue.put(file)
        map(lambda w: self.queue.put('EOF'), workers)

        queue_size = 0
        while self.queue.qsize() > 0:
            if self.queue.qsize() != queue_size:
                queue_size = self.queue.qsize()
                LOG.info("Queue size: %d" % (queue_size))
            time.sleep(1)

        map(lambda w: w.join(), workers)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import Audio file into HBase')
    parser.add_argument('--path', dest='path', help='path to audio file or directory')
    parser.add_argument('--pattern', dest='pattern', default=DEFAULT_PATTERN, help='pattern of file names to be loaded')
    parser.add_argument('--audio-cf', dest='audio_cf', default=DEFAULT_AUDIO_CF, help='Name of the top level column family')
    parser.add_argument('--batch-size', dest='batch_size', default=DEFAULT_BATCH_SIZE, help='Maximum number of files to load in a single batch')
    parser.add_argument('--workers', dest='workers', default=DEFAULT_WORKERS, help='Number of workers to process the load')
    parser.add_argument('--config-file', dest='config_file', default=CONFIG_FILE, help='Path to configuration file')
    parser.add_argument('--site', dest='site', help='Name of the site for which to run the load', required=True)
    parser.add_argument('--table-name', dest='table_name', help='Name of the table that will receive the data', required=True)
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

    # load data file

    AudioLoader(
        options.path,
        options.pattern,
        options.table_name,
        options.audio_cf,
        int(options.batch_size),
        int(options.workers),
        options.site['host'],
        int(options.site['port'])
        ).load_data()

