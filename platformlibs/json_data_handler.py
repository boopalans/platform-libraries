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
from platformlibs.common_helpers import flatten_dict
from platformlibs.data_handler import DataHandler

class JsonDataHandler(DataHandler):
    '''
    Json Data Handler assumed 'rawdata' in well-formatted json format
    '''
    def __init__(self,
                 spark_context,
                 datasource,
                 path,
                 isTopic=False):
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
                             path,
                             isTopic)

    @staticmethod
    def preprocess(data_dict):
        """ decode raw payload using 'utf-8'
        Args:
            data_dict: the raw data rdd
        """
        raw_data_str = data_dict.pop('rawdata').decode('utf-8')
        data_dict['rawdata'] = flatten_dict(json.loads(raw_data_str))
        return data_dict

    def list_metric_ids(self, limit=-1, filters=None):
        """ return list of metric statistics """

        #embedded function declaration
        def get_count(item):
            return item[1]

        t_rdd = self.rdd
        m_filter = None

        if filters:
            # extract metric list filter if exists
            m_filter = filters.pop('metrics', None) 
            
            for f in filters:
                t_rdd = t_rdd.filter(lambda x: x['rawdata'][f] in filters[f])

            t_rdd = t_rdd.map(lambda x: {i:x['rawdata'][i] for i in x['rawdata'] if i not in filters})
        else:
            t_rdd = t_rdd.map(lambda x: x['rawdata'])

        t_rdd = t_rdd.flatMap(lambda x: x.keys()) \
                    .map(lambda x: (x, 1)) \
                    .reduceByKey(lambda x, y: x + y)

        if m_filter:
            t_rdd = t_rdd.filter(lambda x: x[0] in m_filter)
           
        stats = t_rdd.collect()
        stats = sorted(stats, key=get_count)
        if limit > 0:
            stats = stats[0:limit]
        return stats

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

        m_filter = None
        
        if filters:
            m_filter = filters.pop('metrics', None)
            
            if filters.has_key('start_ts'):
                start_ts = filters.pop('start_ts')
                # filter by timestamps
                t_rdd = t_rdd.filter(lambda x: (int(x['timestamp']) >= start_ts))
            if filters.has_key('end_ts'):
                #filter by timestamps
                end_ts = filters.pop('end_ts')
                t_rdd = t_rdd.filter(lambda x: (int(x['timestamp']) <= end_ts))
            for f in filters:
                t_rdd = t_rdd.filter(lambda x: x['rawdata'][f] in filters[f])
                
        t_rdd = t_rdd.map(lambda x: (x['timestamp'], x['rawdata'].items())) \
                      .flatMapValues(func) \
                      .map(lambda x: (x[1][0], (x[0], x[1][1])))

        if m_filter:
            t_rdd = t_rdd.filter(lambda x: x[0] in m_filter)
                
        t_rdd = t_rdd.distinct() \
                     .groupByKey() \
                     .mapValues(list) \
                     .map(lambda x : (x[0], sorted(x[1], key=get_ts)))

        return t_rdd.collect()
