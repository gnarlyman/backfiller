#!/usr/bin/env python
#########################################
#
#########################################

import sys
import os
import logging, logging.handlers
import argparse
import traceback
import csv
import time
from datetime import datetime, timedelta
from pprint import pprint
from itertools import groupby
from operator import itemgetter

import splunklib.client as client
import splunklib.results as results

from croniter import croniter
from timeparser import timeParser

CWD = os.path.dirname(os.path.abspath(__file__))
LOG_FORMAT = '%(levelname)s: %(message)s'
logging.basicConfig(format=LOG_FORMAT)
logger = logging.getLogger(__name__)
formatter = logging.Formatter(LOG_FORMAT)
handler = logging.handlers.RotatingFileHandler(os.path.join(CWD,'backfill.log'), maxBytes=10**6, backupCount=3)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

#############################################
#   TODO
#   
#   Support for multiple savedsearches
#       Each one should be done in order specified
#       Same start/end times should apply
#
#   CSV writing support, an alternative to summary indexing
#       Identify todo's
#
#############################################

def uncaught_exception_handler(ex_cls, ex, tb):
    logger.error(''.join(traceback.format_tb(tb)))
    logger.error("Exception: {0}: {1}".format(str(ex_cls), str(ex)))
sys.excepthook = uncaught_exception_handler

def splunk_auth(host='localhost'):
    token = None
    # Splunk native
    try:
        import splunk.entity
        sessionKey = sys.stdin.readline().strip()
        if len(sessionKey) == 0:
            logger.error("Did not receive a session key from splunkd. "
                            "Please enable passAuth in inputs.conf for this "
                            "script\n")
            sys.exit(2)
        token = sessionKey
    except ImportError:
        from getpass import getpass
        logger.debug('connecting to %s:8089 for app eam2...' % host)
        print 'Splunk Login:'
        username = raw_input('username: ')
        password = getpass()

        service = client.connect(host=host, port=8089,
                           username=username, password=password)
        token = service.token

    assert token is not None, 'token is null, auth must have failed'
    return client.Service(token=token, app='eam2', host=host)

def get_results(result_stream, offset, count):
    reader = results.ResultsReader(result_stream.results(count=count, offset=offset))
    return map(dict, reader)

def load_time(ss):
    if os.path.isfile(os.path.join(CWD,ss+'.dat')):
        with open(os.path.join(CWD,ss+'.dat')) as f:
            return f.read()

def save_time(time, ss):
    assert isinstance(time, str), 'must be str'
    with open(os.path.join(CWD,ss+'.dat'),'w') as f:
        f.write(time)

def getArgs():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',dest='debug',help='enable debug',action='store_true')
    parser.add_argument('--stdout',dest='stdout',help='log to stdout',action='store_true')
    parser.add_argument('--full',dest='full',help='full update',action='store_true')
    parser.add_argument('--csv',dest='csv',help='write to csv it true, otherwise write to summary index', action='store_true')
    parser.add_argument('--host',dest='host',help='host to connect to, (default = localhost)', default='localhost')
    parser.add_argument('-s', '--searches',dest='searches',help='savedsearch name to backfill',required=True,nargs='*')
    parser.add_argument('-et',dest='earliest',help='time to start backfilling at (splunk relative time format)',required=True)
    parser.add_argument('-lt',dest='latest',help='time to stop backfilling at (default: now) (splunk relative time format)',required=False, default='now')
    parser.add_argument('-j',dest='jobs',help='how many concurrent jobs', required=False, default=1, type=int)

    
    return parser.parse_args()

def interval(cron, start, end):
    cron = croniter(cron, start_time=start)
    prev = None
    for c in cron.all_next():
        if prev is None:
            prev = c
            continue
        if int(c) > int(end):
            break
        yield prev, c
        prev = c

def get_epoch_from_delta(**options):
    reltime = datetime.now() + timedelta(**options)
    return time.mktime(reltime.timetuple())

def get_epoch_from_splunk_relative_time(rel_string):
    reltime = timeParser(ts=rel_string)
    return time.mktime(reltime.timetuple())

class JobManager(object):
    jobs = []
    earliest = None
    start = None
    end = None
    max_jobs = 1
    resume = None
    search_results = []
    csv_in = []
    write_to_csv = False
    lookup_file = None
    def __init__(self, **kwargs):
        self.search = kwargs['search']
        self.cron = kwargs['cron']
        self.service = kwargs['service']
        self.write_to_csv = kwargs['csv']
        self.start = kwargs.get('start') #optionally specify start time upon construction
        self.end = kwargs.get('end') #optionally specify end time upon construction

    def __enter__(self):
        return self

    def __exit__(self, type, value, tb):
        if isinstance(self.resume, float):
            save_time(str(int(self.resume)), self.search.name)

    def create_jobs(self):
        assert self.start is not None, 'must specify start time'
        logger.info('spawning jobs for savedsearch: %s' % self.search.name)
        for et,lt in interval(self.cron, self.start, self.end):
            if self.earliest < et:
                self.earliest = et

            et = str(int(et))
            lt = str(int(lt))
            options = {
                "dispatch.earliest_time":et,
                "dispatch.latest_time":lt,
                "dispatch.count":"0",
            }
            if not self.write_to_csv:
                options.update({'trigger_actions':'1'})
            job = self.search.dispatch(**options)
            self.jobs.append(job)
            if len(self.jobs) >= self.max_jobs:
                self.monitor_jobs()
        self.process()

    def monitor_jobs(self):
        logger.debug('Jobs: %d' % len(self.jobs))
        while len(self.jobs):
            for j in self.jobs[:]:
                try:
                    j.refresh()
                except AttributeError, e:
                    logger.warn(str(e))
                    break
                done = int(j['isDone'])
                if done:
                    if self.write_to_csv:
                        self.handle_results(j)
                    try:
                        self.jobs.remove(j)
                    except Exception, e:
                        logger.warn(str(e))
                        break
                    logger.debug('Job Completed: %s - %s = %s Results' % (j.earliestTime,j.latestTime,j.resultCount))
            time.sleep(2)

    def handle_results(self, job):
        """ read results from search job, used when writing to csv """
        resultCount = int(job.resultCount)
        if resultCount:
            offset = 0
            count = 10000
            while offset < resultCount:
                data = get_results(job, offset, count)
                offset += count
                self.search_results.extend(data)

        if self.resume < self.earliest:
            self.resume = self.earliest

    def process(self):
        logger.debug('Total Results: %d' % len(self.search_results))
        grouper = itemgetter('aid')
        for key, grp in groupby(sorted(self.search_results, key=grouper),grouper):
            temp_dict = dict(zip(['aid'], key))
            print key, len(list(grp))

def main():
    args = getArgs()
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
    logger.debug('started')

    service = splunk_auth(host=args.host)
    
    earliest = get_epoch_from_splunk_relative_time(args.earliest)
    latest = get_epoch_from_splunk_relative_time(args.latest)

    for search in args.searches:
        ss = service.saved_searches[search]
        with JobManager(
            search=ss, 
            cron=ss['cron_schedule'], 
            service=service,
            csv=args.csv
        ) as jm:
            jm.max_jobs = args.jobs
            jm.start = earliest
            jm.end = latest
            jm.create_jobs()

if __name__ == '__main__':
    main()
    logger.debug('stopped')