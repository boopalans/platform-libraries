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

Name:       xr_data_handler
Purpose:    XR Telemetry Data Handler Implementation
"""
import re
import json
from platformlibs.json_data_handler import JsonDataHandler
from platformlibs.common_helpers import flatten_dict

class XrDataHandler(JsonDataHandler):
    '''
    Sample Telemetry data handler library
    '''
    def __init__(self,
                 spark_context,
                 datasource,
                 path):
        '''
        Constructor
        :param sc:
        :param cluster_name:
        :param manager_hostname:
        '''
        JsonDataHandler.__init__(self,
                                 spark_context,
                                 datasource,
                                 path)

    @staticmethod
    def preprocess(data_dict):
        '''
        pre-processing telemetry data
        :param data_dict:
        '''
        raw_data_str = data_dict.pop('rawdata').decode('utf-8')
        raw_data_str = re.sub(r'"{"', '{"', raw_data_str)
        raw_data_str = re.sub(r'}"}', '}}', raw_data_str)
        raw_data_dict = json.loads(raw_data_str)
        if 'path' in raw_data_dict.keys():
            data_dict['path'] = raw_data_dict['path']

        if 'identifier' in raw_data_dict.keys():
            data_dict['identifier'] = raw_data_dict['identifier']

        if 'type' in raw_data_dict.keys():
            data_dict['type'] = raw_data_dict['type']

        data_dict['rawdata'] = flatten_dict(raw_data_dict['content'])
        return data_dict

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
             .flatMapValues(func)

        if filters and filters.has_key('metric_type'):
            metric_type = filters['metric_type']
            if metric_type is 'ipsla':
                t_rdd = t_rdd.filter(lambda x: ('ipsla' in x[1]))
            if metric_type is 'mpls':
                t_rdd = t_rdd.filter(lambda x: ('mpls' in x[1]))
            if metric_type is 'infra':
                t_rdd = t_rdd.filter(lambda x: ('ipsla' not in x)) \
                             .filter(lambda x: ('mpls' not in x))

        t_rdd = t_rdd.map(lambda x: (x, 1)) \
                     .reduceByKey(lambda x, y: x + y) \
                     .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                     .groupByKey() \
                     .mapValues(list) \
                     .map(lambda x: (x[0], sorted(x[1], key=get_count)))

        if limit > 0:
            t_rdd = t_rdd.map(lambda x: (x[0], x[1][0:limit]))

        return t_rdd.collect()
