"""
Copyright (c) 2016 Cisco and/or its affiliates.
This software is licensed to you under the terms of the Apache License, Version 2.0 (the "License").
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
The code, technical concepts, and all information contained herein, are the property of Cisco Technology, Inc.
and/or its affiliated entities, under various laws including copyright, international treaties, patent,
and/or contract. Any use of the material herein must be in accordance with the terms of the License.
All rights not expressly granted by the License are reserved.
Unless required by applicable law or agreed to separately in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied.

Name:       json_data_handler
Purpose:    JSON data handler implementation
"""
import json
from platformlibs.data_handler import DataHandler

class JsonDataHandler(DataHandler):
    '''
    Json Data Handler assumed 'rawdata' in well-formatted json format
    '''
    def __init__(self,
                 spark_context,
                 datasource,
                 path):
        '''
        Constructor
        :param sc:
        :param datasource:
        :param path:
        :param cluster_name:
        :param manager_hostname:
        '''
        DataHandler.__init__(self,
                             spark_context,
                             datasource,
                             path)

    @staticmethod
    def preprocess(data_dict):
        """ decode raw payload using 'utf-8'
        Args:
            data_dict: the raw data rdd
        """
        raw_data_str = data_dict.pop('rawdata').decode('utf-8')
        data_dict['rawdata'] = json.loads(raw_data_str)
        return data_dict

    def list_host_ips(self):
        """ return list of monitored host ips """
        result = self.rdd.map(lambda x: (str(x['host_ip']), 1)) \
                         .reduceByKey(lambda x, y: x + y) \
                         .collect()
        return sorted(result, key=lambda x: x[1], reverse=True)

    def list_metric_ids(self, limit=-1, filters=None):
        """ return list of metric statistics """

        #embedded function declaration
        def func(item):
            return item

        def get_count(item):
            return item[1]

        t_rdd = self.rdd

        if filters:
            if filters.has_key('host_ips'):
                # filter by host ips
                t_rdd = t_rdd.filter(lambda x: (x['host_ip'] in filters['host_ips']))

        t_rdd = t_rdd.map(lambda x: (str(x['host_ip']), x['rawdata'].keys())) \
             .flatMapValues(func) \
             .map(lambda x: (x, 1)) \
             .reduceByKey(lambda x, y: x + y) \
             .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
             .groupByKey() \
             .mapValues(list) \
             .map(lambda x: (x[0], sorted(x[1], key=get_count)))

        if limit > 0:
            t_rdd = t_rdd.map(lambda x: (x[0], x[1][0:limit]))

        return t_rdd.collect()

    def execute_query(self, filters=None):
        '''
        return time-series measurements of metric(s)
        Args:
            filters: filtering attributes
        '''
        #embedded function declaration
        def func(item):
            return item

        def get_ts(item):
            return item[0]

        t_rdd = self.rdd

        if filters:
            #apply filtering rules
            if filters.has_key('host_ips'):
                # filter by host ips
                t_rdd = t_rdd.filter(lambda x: (x['host_ip'] in filters['host_ips']))
            if filters.has_key('start_ts'):
                # filter by timestamps
                t_rdd = t_rdd.filter(lambda x: (int(x['timestamp']) >= filters['start_ts']))
            if filters.has_key('end_ts'):
                #filter by timestamps
                t_rdd = t_rdd.filter(lambda x: (int(x['timestamp']) <= filters['end_ts']))
            if filters.has_key('data_sources'):
                #filter by src
                t_rdd = t_rdd.filter(lambda x: (x['src'] in filters['data_sources']))

        t_rdd = t_rdd.map(lambda x: ((x['host_ip'], x['timestamp']), x['rawdata'].items())) \
                      .flatMapValues(func) \
                      .map(lambda x: x[1]+x[0])

        if filters and filters.has_key('metrics'):
            t_rdd = t_rdd.filter(lambda x: x[0] in filters['metrics'])

        t_rdd = t_rdd.distinct() \
                     .map(lambda x: ((x[0], x[2]), (x[3], x[1]))) \
                     .groupByKey() \
                     .mapValues(list) \
                     .map(lambda x: (x[0], sorted(x[1], key=get_ts)))

        return t_rdd.collect()
